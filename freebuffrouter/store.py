"""Single-file sqlite3 store replacing both Redis (hot state) and
ClickHouse (cold request-log history).

Design (per the chosen approach):
  * One DB file, WAL journal mode for concurrent readers + one writer.
  * Hot-state tables (fb_accounts, gateway_keys) are read into an in-process
    cache by the managers on startup; this mirrors Redis' sub-ms reads.
    Writes go through here (best-effort, synchronous but cheap).
  * Cold request_logs table stores full request/response JSON, fed by an
    async background queue so the request path never blocks on disk I/O.

Thread-safety: a single module-level connection guarded by a lock. sqlite3
with WAL + `check_same_thread=False` tolerates concurrent access from the
uvicorn event-loop thread pool and the background log-flush task.
"""
from __future__ import annotations

import json
import secrets
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Request-log DTOs (mirror internal/db/logstore.go)
# ---------------------------------------------------------------------------


@dataclass
class RequestLog:
    timestamp: float
    request_id: str = ""
    client_key: str = ""
    model: str = ""
    upstream: str = ""
    account_id: str = ""
    status_code: int = 0
    latency_ms: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    error_msg: str = ""
    input_text: str = ""
    output_text: str = ""
    request_body: str = ""
    response_body: str = ""


LOG_BUFFER_SIZE = 10000
LOG_FLUSH_INTERVAL = 2.0
LOG_BATCH_SIZE = 200


class Store:
    """Owns the sqlite connection + async log pipeline."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._log_q: "queue.Queue[Optional[RequestLog]]" = queue.Queue(maxsize=LOG_BUFFER_SIZE)
        self._flush_thread: Optional[threading.Thread] = None
        self._closed = False
        self._ensure_schema()

    # -- schema ------------------------------------------------------------
    def _ensure_schema(self) -> None:
        with self._lock:
            c = self._conn
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.execute("PRAGMA busy_timeout=5000")
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS fb_accounts (
                    token TEXT PRIMARY KEY,
                    user_id TEXT DEFAULT '',
                    email TEXT DEFAULT '',
                    disabled INTEGER DEFAULT 0,
                    disabled_at INTEGER DEFAULT 0,
                    cooldown_until INTEGER DEFAULT 0,
                    quota_recent REAL DEFAULT 0,
                    quota_limit REAL DEFAULT 0,
                    quota_synced_at INTEGER DEFAULT 0,
                    quota_reset_at INTEGER DEFAULT 0,
                    quota_period TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS gateway_keys (
                    key TEXT PRIMARY KEY,
                    name TEXT DEFAULT '',
                    role TEXT DEFAULT 'inference',
                    allowed_models TEXT DEFAULT '',
                    rpm INTEGER DEFAULT 0,
                    burst INTEGER DEFAULT 0,
                    token_quota INTEGER DEFAULT 0,
                    tokens_used INTEGER DEFAULT 0,
                    requests INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT 0,
                    disabled INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS dashboard_credentials (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    password_hash TEXT NOT NULL,
                    password_salt TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS proxies (
                    id TEXT PRIMARY KEY,
                    protocol TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    username TEXT DEFAULT '',
                    password TEXT DEFAULT '',
                    enabled INTEGER DEFAULT 1,
                    label TEXT DEFAULT '',
                    upstreams TEXT DEFAULT '["all"]',
                    created_at REAL DEFAULT 0,
                    last_used_at REAL DEFAULT 0,
                    fail_count INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS request_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    request_id TEXT,
                    client_key TEXT,
                    model TEXT,
                    upstream TEXT,
                    account_id TEXT,
                    status_code INTEGER,
                    latency_ms INTEGER,
                    tokens_in INTEGER,
                    tokens_out INTEGER,
                    error_msg TEXT,
                    input_text TEXT,
                    output_text TEXT,
                    request_body TEXT,
                    response_body TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_reqlog_ts ON request_logs(ts);
                CREATE INDEX IF NOT EXISTS idx_reqlog_model ON request_logs(model);
                """
            )
            c.commit()

    def load_dashboard_credential(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM dashboard_credentials WHERE id=1"
            ).fetchone()
        return dict(row) if row else None

    def save_dashboard_credential(self, password_hash: str, password_salt: str) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO dashboard_credentials
                    (id,password_hash,password_salt,created_at,updated_at)
                VALUES (1,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    password_hash=excluded.password_hash,
                    password_salt=excluded.password_salt,
                    updated_at=excluded.updated_at
                """,
                (password_hash, password_salt, now, now),
            )
            self._conn.commit()

    def start(self) -> None:
        """Launch the async log-flush background thread."""
        if self._flush_thread is None:
            self._flush_thread = threading.Thread(
                target=self._flush_loop, name="fr-log-flush", daemon=True
            )
            self._flush_thread.start()

    # -- fb accounts (hot state) ------------------------------------------
    def load_fb_accounts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM fb_accounts").fetchall()
        return [dict(r) for r in rows]

    def save_fb_account(self, d: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO fb_accounts
                    (token,user_id,email,disabled,disabled_at,cooldown_until,
                     quota_recent,quota_limit,quota_synced_at,quota_reset_at,quota_period)
                VALUES (:token,:user_id,:email,:disabled,:disabled_at,:cooldown_until,
                        :quota_recent,:quota_limit,:quota_synced_at,:quota_reset_at,:quota_period)
                ON CONFLICT(token) DO UPDATE SET
                    user_id=excluded.user_id,
                    email=excluded.email,
                    disabled=excluded.disabled,
                    disabled_at=excluded.disabled_at,
                    cooldown_until=excluded.cooldown_until,
                    quota_recent=excluded.quota_recent,
                    quota_limit=excluded.quota_limit,
                    quota_synced_at=excluded.quota_synced_at,
                    quota_reset_at=excluded.quota_reset_at,
                    quota_period=excluded.quota_period
                """,
                d,
            )
            self._conn.commit()

    def delete_fb_account(self, token: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM fb_accounts WHERE token=?", (token,))
            self._conn.commit()

    # -- gateway keys (hot state) -----------------------------------------
    def load_gateway_keys(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM gateway_keys").fetchall()
        return [dict(r) for r in rows]

    def save_gateway_key(self, d: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO gateway_keys
                    (key,name,role,allowed_models,rpm,burst,token_quota,tokens_used,requests,created_at,disabled)
                VALUES (:key,:name,:role,:allowed_models,:rpm,:burst,:token_quota,:tokens_used,:requests,:created_at,:disabled)
                ON CONFLICT(key) DO UPDATE SET
                    name=excluded.name,
                    role=excluded.role,
                    allowed_models=excluded.allowed_models,
                    rpm=excluded.rpm,
                    burst=excluded.burst,
                    token_quota=excluded.token_quota,
                    tokens_used=excluded.tokens_used,
                    requests=excluded.requests,
                    disabled=excluded.disabled
                """,
                d,
            )
            self._conn.commit()

    def delete_gateway_key(self, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM gateway_keys WHERE key=?", (key,))
            self._conn.commit()

    def increment_gateway_key(self, key: str, tokens: int, requests: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE gateway_keys SET tokens_used=tokens_used+?, requests=requests+? WHERE key=?",
                (tokens, requests, key),
            )
            self._conn.commit()

    # -- proxy pool (hot state) -------------------------------------------
    def load_proxies(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM proxies ORDER BY created_at, id").fetchall()
        return [dict(r) for r in rows]

    def save_proxy(self, d: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO proxies
                    (id,protocol,host,port,username,password,enabled,label,upstreams,
                     created_at,last_used_at,fail_count)
                VALUES (:id,:protocol,:host,:port,:username,:password,:enabled,:label,
                        :upstreams,:created_at,:last_used_at,:fail_count)
                ON CONFLICT(id) DO UPDATE SET
                    protocol=excluded.protocol, host=excluded.host, port=excluded.port,
                    username=excluded.username, password=excluded.password,
                    enabled=excluded.enabled, label=excluded.label,
                    upstreams=excluded.upstreams, last_used_at=excluded.last_used_at,
                    fail_count=excluded.fail_count
                """,
                d,
            )
            self._conn.commit()

    def delete_proxy(self, proxy_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM proxies WHERE id=?", (proxy_id,))
            self._conn.commit()

    # -- request logs (cold, async) ---------------------------------------
    def log_request(self, r: RequestLog) -> None:
        """Non-blocking enqueue — drops if the buffer is full."""
        try:
            self._log_q.put_nowait(r)
        except queue.Full:
            pass

    def _flush_loop(self) -> None:
        batch: list[RequestLog] = []
        last = time.monotonic()
        while not self._closed:
            timeout = LOG_FLUSH_INTERVAL
            try:
                item = self._log_q.get(timeout=timeout)
            except queue.Empty:
                item = None
            if item is not None:
                batch.append(item)
            now = time.monotonic()
            if batch and (len(batch) >= LOG_BATCH_SIZE or now - last >= LOG_FLUSH_INTERVAL):
                self._flush_batch(batch)
                batch = []
                last = now
        if batch:
            self._flush_batch(batch)

    def _flush_batch(self, batch: list[RequestLog]) -> None:
        rows = [
            (
                r.timestamp, r.request_id, r.client_key, r.model, r.upstream,
                r.account_id, r.status_code, r.latency_ms, r.tokens_in, r.tokens_out,
                r.error_msg, r.input_text, r.output_text, r.request_body, r.response_body,
            )
            for r in batch
        ]
        try:
            with self._lock:
                self._conn.executemany(
                    """
                    INSERT INTO request_logs
                        (ts,request_id,client_key,model,upstream,account_id,status_code,
                         latency_ms,tokens_in,tokens_out,error_msg,input_text,output_text,
                         request_body,response_body)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )
                self._conn.commit()
        except Exception:
            pass

    # -- history queries ---------------------------------------------------
    def request_stats(self, since_ts: float) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN status_code>=400 THEN 1 ELSE 0 END) AS errors,
                       AVG(latency_ms) AS avg_lat,
                       SUM(tokens_in) AS tin,
                       SUM(tokens_out) AS tout
                FROM request_logs WHERE ts>=?
                """,
                (since_ts,),
            ).fetchone()
        total = row["total"] or 0
        errors = row["errors"] or 0
        tin = row["tin"] or 0
        tout = row["tout"] or 0
        return {
            "total_requests": total,
            "total_errors": errors,
            "error_rate_pct": round((errors / total * 100) if total else 0.0, 2),
            "avg_latency_ms": round(row["avg_lat"] or 0.0, 2),
            "total_tokens_in": tin,
            "total_tokens_out": tout,
            "total_tokens": tin + tout,
        }

    def model_stats(self, since_ts: float, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT model,
                       COUNT(*) AS total,
                       SUM(CASE WHEN status_code>=400 THEN 1 ELSE 0 END) AS errors,
                       AVG(latency_ms) AS avg_lat,
                       SUM(tokens_in) AS tin,
                       SUM(tokens_out) AS tout
                FROM request_logs WHERE ts>=?
                GROUP BY model ORDER BY total DESC LIMIT ?
                """,
                (since_ts, limit),
            ).fetchall()
        out = []
        for r in rows:
            tin = r["tin"] or 0
            tout = r["tout"] or 0
            out.append({
                "model": r["model"],
                "total_requests": r["total"],
                "total_errors": r["errors"] or 0,
                "avg_latency_ms": round(r["avg_lat"] or 0.0, 2),
                "total_tokens_in": tin,
                "total_tokens_out": tout,
                "total_tokens": tin + tout,
            })
        return out

    def recent_requests(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM request_logs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": str(r["id"]),
                "timestamp": _iso(r["ts"]),
                "client_key": r["client_key"],
                "model": r["model"],
                "upstream": r["upstream"],
                "account_id": r["account_id"],
                "status_code": r["status_code"],
                "latency_ms": r["latency_ms"],
                "tokens_in": r["tokens_in"],
                "tokens_out": r["tokens_out"],
                "cache_hit_pct": 0.0,
                "input_text": r["input_text"] or "",
                "output_text": r["output_text"] or "",
                "error_msg": r["error_msg"] or "",
            })
        return out

    def request_detail(self, rid: int) -> Optional[dict[str, Any]]:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM request_logs WHERE id=?", (rid,)
            ).fetchone()
        if r is None:
            return None
        return {
            "id": str(r["id"]),
            "timestamp": _iso(r["ts"]),
            "client_key": r["client_key"],
            "model": r["model"],
            "upstream": r["upstream"],
            "account_id": r["account_id"],
            "status_code": r["status_code"],
            "latency_ms": r["latency_ms"],
            "tokens_in": r["tokens_in"],
            "tokens_out": r["tokens_out"],
            "error_msg": r["error_msg"] or "",
            "input_text": r["input_text"] or "",
            "output_text": r["output_text"] or "",
            "request_body": _raw(r["request_body"]),
            "response_body": _raw(r["response_body"]),
        }

    def close(self) -> None:
        self._closed = True
        if self._flush_thread is not None:
            self._flush_thread.join(timeout=10)
        with self._lock:
            try:
                self._conn.commit()
            finally:
                self._conn.close()


def _iso(ts: float) -> str:
    import datetime
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat()


def _raw(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return s
