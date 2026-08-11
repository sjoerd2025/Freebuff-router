"""Global HTTP/SOCKS5 proxy pool matching the Go implementation.

Proxies are scoped to upstream families, not individual Freebuff accounts.
The selected proxy is reused for all session/run/chat calls in one request.
Five consecutive failures auto-disable an entry.
"""
from __future__ import annotations

import asyncio
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import httpx

from .httpclient import upstream_client
from .store import Store

FAIL_THRESHOLD = 5
VALID_PROTOCOLS = {"http", "socks5"}
VALID_UPSTREAMS = {"all"}


@dataclass
class ProxyEntry:
    id: str
    protocol: str
    host: str
    port: int
    username: str = ""
    password: str = ""
    enabled: bool = True
    label: str = ""
    upstreams: list[str] | None = None
    created_at: float = 0.0
    last_used_at: float = 0.0
    fail_count: int = 0

    def applies_to(self, upstream: str) -> bool:
        scopes = self.upstreams or ["all"]
        return "all" in scopes or upstream in scopes

    def url(self) -> str:
        auth = ""
        if self.username:
            auth = f"{quote(self.username, safe='')}:{quote(self.password, safe='')}@"
        return f"{self.protocol}://{auth}{self.host}:{self.port}"

    def public(self) -> dict[str, Any]:
        d = self.to_dict()
        if d["password"]:
            d["password"] = "***"
        return d

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "protocol": self.protocol,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "enabled": self.enabled,
            "label": self.label,
            "upstreams": self.upstreams or ["all"],
            "created_at": self.created_at,
            "last_used_at": self.last_used_at,
            "fail_count": self.fail_count,
        }


class ProxyPool:
    def __init__(self, store: Store):
        self._store = store
        self._entries: dict[str, ProxyEntry] = {}
        self._rr = 0
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._lock = asyncio.Lock()

    def load(self) -> int:
        for row in self._store.load_proxies():
            try:
                scopes = json.loads(row.get("upstreams") or '["all"]')
            except Exception:
                scopes = ["all"]
            entry = ProxyEntry(
                id=row["id"], protocol=row["protocol"], host=row["host"],
                port=int(row["port"]), username=row.get("username") or "",
                password=row.get("password") or "", enabled=bool(row.get("enabled")),
                label=row.get("label") or "", upstreams=scopes,
                created_at=float(row.get("created_at") or 0),
                last_used_at=float(row.get("last_used_at") or 0),
                fail_count=int(row.get("fail_count") or 0),
            )
            self._entries[entry.id] = entry
        return len(self._entries)

    def list(self) -> list[dict[str, Any]]:
        return [p.public() for p in self._entries.values()]

    def get(self, proxy_id: str) -> ProxyEntry | None:
        return self._entries.get(proxy_id)

    def add(self, data: dict[str, Any]) -> ProxyEntry:
        protocol = str(data.get("protocol", "")).lower()
        host = str(data.get("host", "")).strip()
        port = int(data.get("port", 0))
        scopes = data.get("upstreams") or ["all"]
        if protocol not in VALID_PROTOCOLS:
            raise ValueError("protocol must be http or socks5")
        if not host or port < 1 or port > 65535:
            raise ValueError("valid host and port are required")
        if not isinstance(scopes, list) or not scopes or any(s not in VALID_UPSTREAMS for s in scopes):
            raise ValueError("upstreams must contain all")
        if "all" in scopes:
            scopes = ["all"]
        entry = ProxyEntry(
            id=data.get("id") or secrets.token_hex(8), protocol=protocol, host=host,
            port=port, username=str(data.get("username") or ""),
            password=str(data.get("password") or ""), enabled=bool(data.get("enabled", True)),
            label=str(data.get("label") or ""), upstreams=scopes, created_at=time.time(),
        )
        self._entries[entry.id] = entry
        self._save(entry)
        return entry

    @staticmethod
    def parse_url(value: str) -> dict[str, Any]:
        """Parse http://user:pass@host:port into an add() payload."""
        raw = value.strip()
        parsed = urlsplit(raw)
        protocol = parsed.scheme.lower()
        if protocol not in VALID_PROTOCOLS:
            raise ValueError("scheme must be http or socks5")
        if not parsed.hostname or parsed.port is None:
            raise ValueError("proxy URL must include host and port")
        return {
            "protocol": protocol,
            "host": parsed.hostname,
            "port": parsed.port,
            "username": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "upstreams": ["all"],
            "label": f"{parsed.hostname}:{parsed.port}",
        }

    def bulk_add(self, raw: str) -> tuple[int, int, list[str]]:
        added = 0
        failed = 0
        errors: list[str] = []
        for line_number, line in enumerate(raw.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                self.add(self.parse_url(line))
                added += 1
            except (ValueError, TypeError) as exc:
                failed += 1
                errors.append(f"line {line_number}: {exc}")
        return added, failed, errors

    def update(self, proxy_id: str, data: dict[str, Any]) -> ProxyEntry:
        entry = self._entries.get(proxy_id)
        if entry is None:
            raise KeyError("proxy not found")
        merged = entry.to_dict()
        merged.update({k: v for k, v in data.items() if v is not None})
        if merged.get("password") == "***":
            merged["password"] = entry.password
        merged["id"] = entry.id
        self._clients.pop(proxy_id, None)
        self._entries.pop(proxy_id)
        return self.add(merged)

    def delete(self, proxy_id: str) -> None:
        self._entries.pop(proxy_id, None)
        client = self._clients.pop(proxy_id, None)
        if client:
            asyncio.create_task(client.aclose())
        self._store.delete_proxy(proxy_id)

    def toggle(self, proxy_id: str, enabled: bool | None = None) -> ProxyEntry:
        entry = self._entries.get(proxy_id)
        if entry is None:
            raise KeyError("proxy not found")
        entry.enabled = not entry.enabled if enabled is None else bool(enabled)
        self._save(entry)
        return entry

    async def client_for(self, upstream: str) -> tuple[httpx.AsyncClient, str]:
        candidates = [p for p in self._entries.values() if p.enabled and p.applies_to(upstream)]
        if not candidates:
            return upstream_client(), ""
        async with self._lock:
            entry = candidates[self._rr % len(candidates)]
            self._rr += 1
            entry.last_used_at = time.time()
            self._save(entry)
            client = self._clients.get(entry.id)
            if client is None:
                client = httpx.AsyncClient(
                    proxy=entry.url(),
                    timeout=httpx.Timeout(300.0, connect=10.0),
                    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
                    follow_redirects=False,
                    http2=True,
                )
                self._clients[entry.id] = client
            return client, entry.id

    def mark_result(self, proxy_id: str, error: bool) -> None:
        if not proxy_id:
            return
        entry = self._entries.get(proxy_id)
        if entry is None:
            return
        if error:
            entry.fail_count += 1
            if entry.fail_count >= FAIL_THRESHOLD:
                entry.enabled = False
        else:
            entry.fail_count = 0
        self._save(entry)

    async def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        if clients:
            await asyncio.gather(*(c.aclose() for c in clients), return_exceptions=True)

    async def test(self, proxy_id: str) -> dict[str, Any]:
        entry = self._entries.get(proxy_id)
        if entry is None:
            raise KeyError("proxy not found")
        client, selected_id = await self.client_for("freebuff")
        started = time.monotonic()
        try:
            response = await client.get("http://httpbin.org/ip", timeout=10.0)
            ok = 200 <= response.status_code < 400
            self.mark_result(selected_id, not ok)
            return {
                "ok": ok,
                "status_code": response.status_code,
                "latency_ms": int((time.monotonic() - started) * 1000),
                "proxy_id": selected_id,
            }
        except httpx.HTTPError as exc:
            self.mark_result(selected_id, True)
            return {
                "ok": False,
                "error": str(exc),
                "latency_ms": int((time.monotonic() - started) * 1000),
                "proxy_id": selected_id,
            }

    def _save(self, entry: ProxyEntry) -> None:
        d = entry.to_dict()
        d["upstreams"] = json.dumps(d["upstreams"])
        d["enabled"] = 1 if entry.enabled else 0
        self._store.save_proxy(d)
