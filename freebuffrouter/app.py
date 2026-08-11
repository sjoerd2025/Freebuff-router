"""FastAPI application — the gateway HTTP surface (Freebuff-only).

Ports the route table + middleware from main.go, restricted to the Freebuff
provider. Redis + ClickHouse are replaced by a single sqlite Store.
"""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from . import config, httpclient
from .auth import (
    AuthManager,
    DASHBOARD_SESSION_KEY,
    ROLE_ADMIN,
    generate_gateway_key,
    mask_key,
)
from .health import UpstreamHealth
from .models import model_list, is_freebuff_model
from .ratelimit import RateLimiter, SessionStore
from .store import Store, RequestLog
from .freebuff.account import FreebuffAccountManager
from .freebuff.proxy import proxy_freebuff
from .freebuff import device as fb_device
from .proxy_pool import ProxyPool

log = logging.getLogger("freebuff-router")

PUBLIC_PATHS = {"/health", "/", "/dashboard", "/login", "/logout"}
SESSION_COOKIE = "freebuffrouter_session"

_DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")

_LOGIN_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>freebuff-router — login</title>
<style>
  body { margin:0; background:#f2efe9; color:#0a0a0a; font-family:"Courier New",
    ui-monospace,SFMono-Regular,Menlo,monospace; display:flex; align-items:center;
    justify-content:center; height:100vh; }
  .box { background:#ffffff; border:4px solid #0a0a0a; border-radius:0;
    padding:34px; width:340px; box-shadow:8px 8px 0 #0a0a0a; }
  h1 { font-size:20px; margin:0 0 4px; text-transform:uppercase; letter-spacing:1px; }
  h1 span { color:#0a0a0a; background:#ffe600; padding:0 4px; }
  p { color:#0a0a0a; font-size:13px; margin:0 0 22px; font-weight:700; }
  input { width:100%; background:#fff; border:3px solid #0a0a0a; color:#0a0a0a;
    padding:11px 13px; border-radius:0; font-size:14px; box-sizing:border-box;
    font-family:inherit; }
  input:focus { outline:none; background:#fffbe6; box-shadow:3px 3px 0 #0a0a0a; }
  button { width:100%; margin-top:14px; background:#0a0a0a; border:3px solid #0a0a0a;
    color:#fff; padding:11px; border-radius:0; font-size:14px; cursor:pointer;
    font-family:inherit; font-weight:700; text-transform:uppercase;
    box-shadow:3px 3px 0 #0a0a0a; }
  button:hover { background:#ffe600; color:#0a0a0a; }
  .err { color:#d40000; font-size:13px; margin-top:12px; min-height:16px; font-weight:700; }
  .hint { margin-top:18px; color:#555; font-size:12px; text-align:center;
    border-top:2px solid #0a0a0a; padding-top:14px; }
</style></head><body>
<div class="box">
  <h1>freebuff<span>-router</span></h1>
  <p>Enter the dashboard password to continue.</p>
  <input id="password" type="password" placeholder="Dashboard password" autofocus
    onkeydown="if(event.key==='Enter')doLogin()">
  <button onclick="doLogin()">Sign in</button>
  <div class="err" id="err"></div>
  <div class="hint">default password: 123456</div>
</div>
<script>
async function doLogin() {
  const password = document.getElementById('password').value;
  if (!password) return;
  const r = await fetch('/login', {method:'POST',
    headers:{'Content-Type':'application/json'}, body: JSON.stringify({password})});
  if (r.ok) { window.location = '/dashboard'; }
  else { document.getElementById('err').textContent = 'Invalid password'; }
}
</script>
</body></html>"""


class State:
    store: Store
    auth: AuthManager
    sessions: SessionStore
    limiter: RateLimiter
    fb: FreebuffAccountManager
    fb_health: UpstreamHealth
    proxy_pool: ProxyPool
    quota_task: Optional[asyncio.Task] = None


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = Store(config.SQLITE_PATH)
    store.start()
    state.store = store
    state.auth = AuthManager(store)
    state.sessions = SessionStore()
    state.limiter = RateLimiter(config.RATE_LIMIT_RPM, config.RATE_LIMIT_BURST, config.RATE_LIMIT_WINDOW_SEC)
    state.fb = FreebuffAccountManager(store)
    loaded = state.fb.load()
    state.fb_health = UpstreamHealth("freebuff")
    state.proxy_pool = ProxyPool(store)
    state.proxy_pool.load()

    if state.auth.bootstrap_info:
        log.warning(state.auth.bootstrap_info)
    log.info("freebuff-router(py) started version=%s port=%s fb_accounts=%d auth_keys=%d db=sqlite:%s",
             config.VERSION, config.PORT, loaded, state.auth.count(), config.SQLITE_PATH)
    base = f"http://{config.HOST}:{config.PORT}"
    log.info("dashboard URL: %s/dashboard", base)
    log.info("login URL:     %s/login", base)
    log.info("API base URL:  %s/v1   (POST /v1/chat/completions, GET /v1/models)",
             base)

    if not config.WORKERS_DISABLED:
        state.quota_task = asyncio.create_task(_quota_sync_worker())

    try:
        yield
    finally:
        if state.quota_task:
            state.quota_task.cancel()
        await state.proxy_pool.close()
        await httpclient.aclose()
        store.close()
        log.info("freebuff-router(py) stopped")


async def _quota_sync_worker():
    await asyncio.sleep(config.FB_QUOTA_SYNC_STARTUP_DELAY)
    while True:
        try:
            await state.fb.sync_all_quota()
        except Exception:
            pass
        await asyncio.sleep(config.FB_QUOTA_SYNC_TICK)


app = FastAPI(title="freebuff-router", version=config.VERSION, lifespan=lifespan)


# ---------------------------------------------------------------------------
# Middleware — security headers + request id + auth + rate limit
# ---------------------------------------------------------------------------
@app.middleware("http")
async def gateway_middleware(request: Request, call_next):
    rid = request.headers.get("X-Request-ID") or secrets.token_hex(8)
    request.state.request_id = rid
    request.state.client_key = ""
    request.state.client_key_masked = ""

    path = request.url.path

    # -- auth --
    if not (path in PUBLIC_PATHS):
        if not config.GATEWAY_AUTH_DISABLE:
            if state.auth.is_empty():
                return JSONResponse({"error": "no gateway API keys configured"}, status_code=503)
            key = ""
            authz = request.headers.get("Authorization", "")
            if authz.startswith("Bearer "):
                key = authz[len("Bearer "):]
            else:
                ck = request.cookies.get(SESSION_COOKIE)
                if ck:
                    key = state.sessions.lookup(ck)
            if not key:
                if "text/html" in request.headers.get("Accept", "") and not path.startswith("/api/"):
                    return RedirectResponse("/login", status_code=302)
                return JSONResponse({"error": "missing or invalid Authorization header (expected Bearer <key>)"},
                                    status_code=401)
            if key != DASHBOARD_SESSION_KEY and not state.auth.valid(key):
                if "text/html" in request.headers.get("Accept", ""):
                    resp = RedirectResponse("/login", status_code=302)
                    resp.delete_cookie(SESSION_COOKIE, path="/")
                    return resp
                return JSONResponse({"error": "invalid API key"}, status_code=401)
            request.state.client_key = key
            request.state.client_key_masked = mask_key(key)

        # -- rate limit --
        if path not in ("/health", "/", "/dashboard"):
            client_key = request.state.client_key
            client_id = client_key or (request.client.host if request.client else "unknown")
            rpm = state.limiter.limit
            burst = state.limiter.burst
            if client_key:
                lim = state.auth.lookup_limits(client_key)
                if lim is not None:
                    krpm, kburst = lim
                    if krpm == 0:
                        rpm = 0  # unlimited
                    else:
                        rpm = krpm if krpm > 0 else rpm
                        burst = kburst if kburst > 0 else burst
            if rpm != 0 and not state.limiter.allow(client_id, rpm, burst):
                return JSONResponse(
                    {"error": "rate limit exceeded",
                     "hint": f"limit: {rpm} req/{int(state.limiter.window)}s, burst: {burst}"},
                    status_code=429)

    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


def _require_admin(request: Request) -> Optional[JSONResponse]:
    if config.GATEWAY_AUTH_DISABLE:
        return None
    key = getattr(request.state, "client_key", "")
    if not key:
        return JSONResponse({"error": "authentication required"}, status_code=401)
    info = state.auth.get(key)
    if key == DASHBOARD_SESSION_KEY:
        return None
    if not info:
        return JSONResponse({"error": "invalid API key"}, status_code=401)
    if info.role != ROLE_ADMIN:
        return JSONResponse({"error": "admin role required for this endpoint"}, status_code=403)
    return None


# ---------------------------------------------------------------------------
# Core endpoints
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {
        "service": "freebuff-router",
        "version": config.VERSION,
        "mode": "freebuff (python port)",
        "fb_accounts": state.fb.len(),
    }


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(_DASHBOARD_HTML)


@app.get("/health")
async def health():
    recent, limit, exhausted = state.fb.quota_summary()
    return {
        "status": "ok",
        "version": config.VERSION,
        "freebuff": {
            "accounts": state.fb.len(),
            "quota_recent": recent,
            "quota_limit": limit,
            "exhausted": exhausted,
            "account_status": state.fb.account_status(),
            "health": state.fb_health.stats(),
        },
        "auth_keys": state.auth.count(),
    }


@app.head("/health")
async def health_head():
    return Response(status_code=200)


@app.api_route("/v1/models", methods=["GET"])
@app.api_route("/models", methods=["GET"])
async def list_models():
    return {"object": "list", "data": model_list()}


@app.api_route("/v1/chat/completions", methods=["POST"])
@app.api_route("/chat/completions", methods=["POST"])
async def chat_completions(request: Request):
    raw = await request.body()
    if len(raw) > config.MAX_REQUEST_BODY:
        return JSONResponse({"error": "request body too large", "limit_bytes": config.MAX_REQUEST_BODY},
                            status_code=413)
    try:
        body_map = json.loads(raw)
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)
    if not isinstance(body_map, dict):
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    model = body_map.get("model") or ""
    if not model:
        model = "fb/deepseek-v4-flash"
        body_map["model"] = model

    # Per-key model whitelist
    key = getattr(request.state, "client_key", "")
    if key:
        info = state.auth.get(key)
        if info and not info.is_model_allowed(model):
            return JSONResponse({"error": "model not allowed for this API key", "model": model}, status_code=403)

    if not is_freebuff_model(model):
        return JSONResponse(
            {"error": f"model not supported by this gateway (freebuff-only): {model}"},
            status_code=404)

    client_stream = bool(body_map.get("stream")) or \
        request.headers.get("Accept") == "text/event-stream"

    ctx: dict = {
        "tokens_in": 0, "tokens_out": 0, "status_code": 200,
        "upstream_account": "", "output_text": "", "response_body": "", "error_msg": "",
    }
    start = time.time()

    async def is_disconnected():
        try:
            return await request.is_disconnected()
        except Exception:
            return False

    result = await proxy_freebuff(
        body_map, state.fb, client_stream, state.fb_health, ctx, is_disconnected,
        state.proxy_pool,
    )

    # Per-key token quota tracking + async log (fired for terminal responses).
    def _finalize():
        if key:
            total = int(ctx["tokens_in"]) + int(ctx["tokens_out"])
            if total > 0:
                state.auth.increment_tokens(key, total)
            else:
                state.auth.increment_requests(key)
        state.store.log_request(RequestLog(
            timestamp=start,
            request_id=getattr(request.state, "request_id", ""),
            client_key=getattr(request.state, "client_key_masked", ""),
            model=model,
            upstream="freebuff",
            account_id=ctx.get("upstream_account", ""),
            status_code=int(ctx.get("status_code", 200)),
            latency_ms=int((time.time() - start) * 1000),
            tokens_in=int(ctx["tokens_in"]),
            tokens_out=int(ctx["tokens_out"]),
            error_msg=ctx.get("error_msg", ""),
            input_text=_extract_input_text(body_map),
            output_text=ctx.get("output_text", ""),
            request_body=json.dumps(body_map),
            response_body=ctx.get("response_body", ""),
        ))

    if result is None:
        # client disconnected mid-stream
        _finalize()
        return Response(status_code=499)

    # For streaming responses, finalize after the body is consumed by wrapping
    # the iterator; for JSON responses finalize immediately.
    from starlette.responses import StreamingResponse
    if isinstance(result, StreamingResponse):
        orig_iter = result.body_iterator

        async def wrapper():
            try:
                async for chunk in orig_iter:
                    yield chunk
            finally:
                _finalize()

        result.body_iterator = wrapper()
        return result

    _finalize()
    return result


def _extract_input_text(body_map: dict) -> str:
    msgs = body_map.get("messages")
    if not isinstance(msgs, list):
        return ""
    for m in reversed(msgs):
        if not isinstance(m, dict) or m.get("role") != "user":
            continue
        content = m.get("content")
        if isinstance(content, str):
            return content[:500]
        if isinstance(content, list):
            parts = [p.get("text", "") for p in content
                     if isinstance(p, dict) and p.get("type") == "text"]
            if parts:
                return " ".join(parts)[:500]
    return ""


# ---------------------------------------------------------------------------
# Freebuff management endpoints
# ---------------------------------------------------------------------------
@app.post("/fb/import")
async def fb_import(request: Request):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    token = (body.get("token") or "").strip()
    if not token:
        return JSONResponse({"error": "missing token field"}, status_code=400)
    try:
        added, total = await state.fb.add_account(token)
    except Exception as e:
        return JSONResponse({"error": "failed to add freebuff token", "detail": str(e)}, status_code=502)
    return {"added": added, "total": total}


@app.post("/fb/import/bulk")
async def fb_import_bulk(request: Request):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    tokens = body.get("tokens") or []
    raw = body.get("raw") or ""

    entries: list[tuple[str, str, str]] = []  # (token, email, user_id)
    if not tokens and raw.strip():
        for line in raw.split("\n"):
            line = line.strip()
            if not line:
                continue
            for part in line.split(","):
                part = part.strip()
                if not part:
                    continue
                pipe = part.split("|")
                token = pipe[0].strip()
                email = pipe[1].strip() if len(pipe) > 1 else ""
                uid = pipe[2].strip() if len(pipe) > 2 else ""
                if token:
                    entries.append((token, email, uid))
    else:
        for t in tokens:
            t = (t or "").strip()
            if t:
                entries.append((t, "", ""))

    if not entries:
        return JSONResponse({"error": "no tokens provided"}, status_code=400)

    added = failed = 0
    errors: list[str] = []
    for token, email, uid in entries:
        try:
            ok, _ = await state.fb.add_account_with_info(token, uid, email)
            if ok:
                added += 1
        except Exception as e:
            failed += 1
            errors.append((token[:12] + "..." if len(token) > 12 else token) + ": " + str(e))
    return {"added": added, "failed": failed, "total": state.fb.len(), "errors": errors}


@app.get("/fb/accounts")
async def fb_accounts(request: Request):
    if (err := _require_admin(request)):
        return err
    accounts = state.fb.list_accounts()
    return {"accounts": accounts, "total": len(accounts)}


@app.post("/fb/quota/sync")
async def fb_quota_sync(request: Request):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    token = (body.get("token") or "").strip()
    if token:
        acc = state.fb.get_account(token)
        if acc is None:
            return JSONResponse({"error": "account not found"}, status_code=404)
        try:
            await state.fb.sync_quota(acc)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=502)
        return {"status": "ok", "synced": 1}
    synced = await state.fb.sync_all_quota()
    return {"status": "ok", "synced": synced}


@app.delete("/fb/accounts/{token}")
async def fb_delete_account(request: Request, token: str):
    if (err := _require_admin(request)):
        return err
    if not token:
        return JSONResponse({"error": "missing token parameter"}, status_code=400)
    state.fb.remove_account(token)
    return {"deleted": True}


@app.get("/api/proxies")
async def list_proxies(request: Request):
    if (err := _require_admin(request)):
        return err
    return {"proxies": state.proxy_pool.list()}


@app.post("/api/proxies")
async def add_proxy(request: Request):
    if (err := _require_admin(request)):
        return err
    try:
        entry = state.proxy_pool.add(await _json(request))
        return entry.public()
    except (ValueError, TypeError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/proxies/bulk")
async def bulk_add_proxies(request: Request):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    raw = body.get("raw") or body.get("proxies") or ""
    if isinstance(raw, list):
        raw = "\n".join(str(item) for item in raw)
    if not isinstance(raw, str) or not raw.strip():
        return JSONResponse({"error": "no proxy URLs provided"}, status_code=400)
    added, failed, errors = state.proxy_pool.bulk_add(raw)
    return {
        "added": added,
        "failed": failed,
        "total": len(state.proxy_pool.list()),
        "errors": errors,
    }


@app.put("/api/proxies/{proxy_id}")
async def update_proxy(request: Request, proxy_id: str):
    if (err := _require_admin(request)):
        return err
    try:
        return state.proxy_pool.update(proxy_id, await _json(request)).public()
    except KeyError as e:
        return JSONResponse({"error": str(e)}, status_code=404)
    except (ValueError, TypeError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.delete("/api/proxies/{proxy_id}")
async def delete_proxy(request: Request, proxy_id: str):
    if (err := _require_admin(request)):
        return err
    if state.proxy_pool.get(proxy_id) is None:
        return JSONResponse({"error": "proxy not found"}, status_code=404)
    state.proxy_pool.delete(proxy_id)
    return {"deleted": True}


@app.post("/api/proxies/bulk-delete")
async def bulk_delete_proxies(request: Request):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    proxy_ids = body.get("ids")
    if not isinstance(proxy_ids, list):
        return JSONResponse({"error": "ids must be an array"}, status_code=400)
    ids = [str(proxy_id).strip() for proxy_id in proxy_ids if str(proxy_id).strip()]
    if not ids:
        return JSONResponse({"error": "no proxy ids provided"}, status_code=400)
    deleted = 0
    missing = []
    for proxy_id in ids:
        if state.proxy_pool.get(proxy_id) is None:
            missing.append(proxy_id)
            continue
        state.proxy_pool.delete(proxy_id)
        deleted += 1
    return {"deleted": deleted, "missing": missing, "total": len(state.proxy_pool.list())}


@app.post("/api/proxies/{proxy_id}/toggle")
async def toggle_proxy(request: Request, proxy_id: str):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    try:
        return state.proxy_pool.toggle(proxy_id, body.get("enabled")).public()
    except KeyError as e:
        return JSONResponse({"error": str(e)}, status_code=404)


@app.post("/api/proxies/{proxy_id}/test")
async def test_proxy(request: Request, proxy_id: str):
    if (err := _require_admin(request)):
        return err
    try:
        return await state.proxy_pool.test(proxy_id)
    except KeyError as e:
        return JSONResponse({"error": str(e)}, status_code=404)


@app.post("/fb/oauth/device/start")
async def fb_device_start(request: Request):
    if (err := _require_admin(request)):
        return err
    try:
        res = await fb_device.start_device_auth()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return {
        "state": res.state,
        "auth_url": res.auth_url,
        "fingerprint_id": res.fingerprint_id,
        "fingerprint_hash": res.fingerprint_hash,
        "expires_at": res.expires_at,
    }


@app.get("/fb/oauth/device/poll")
async def fb_device_poll(request: Request):
    if (err := _require_admin(request)):
        return err
    q = request.query_params
    fp_id = q.get("fingerprint_id", "")
    fp_hash = q.get("fingerprint_hash", "")
    try:
        expires_at = int(q.get("expires_at", "0"))
    except ValueError:
        expires_at = 0
    if not fp_id or not fp_hash:
        return JSONResponse({"error": "fingerprint_id and fingerprint_hash are required"}, status_code=400)
    try:
        res = await fb_device.poll_device_auth(fp_id, fp_hash, expires_at)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=502)
    if res.status == "ready":
        await state.fb.add_account_with_info(res.auth_token, res.user_id, res.email)
        return {"status": "ready", "auth_token": res.auth_token, "user_id": res.user_id, "email": res.email}
    if res.status == "error":
        return {"status": "error", "error": res.error}
    return {"status": "pending"}


# ---------------------------------------------------------------------------
# Gateway API-key management (admin)
# ---------------------------------------------------------------------------
@app.get("/api/keys")
async def list_keys(request: Request):
    if (err := _require_admin(request)):
        return err
    return {"keys": [k.public() for k in state.auth.get_all()]}


@app.post("/api/keys")
async def create_key(request: Request):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    key = generate_gateway_key()
    info = state.auth.add(
        key,
        name=body.get("name", ""),
        role=body.get("role", ROLE_ADMIN),
        allowed_models=body.get("allowed_models") or [],
        rpm=int(body.get("rpm", 0)),
        burst=int(body.get("burst", 0)),
        token_quota=int(body.get("token_quota", 0)),
    )
    return info.public(include_full=True)


@app.delete("/api/keys/{key}")
async def delete_key(request: Request, key: str):
    if (err := _require_admin(request)):
        return err
    resolved = state.auth.resolve_key(key)
    if not resolved:
        return JSONResponse({"error": "key not found"}, status_code=404)
    if state.auth.get(resolved).role == ROLE_ADMIN and state.auth.count_admins() <= 1:
        return JSONResponse({"error": "cannot delete the last admin key"}, status_code=400)
    state.auth.remove(resolved)
    return {"deleted": True}


@app.put("/api/keys/{key}")
async def update_key(request: Request, key: str):
    if (err := _require_admin(request)):
        return err
    resolved = state.auth.resolve_key(key)
    if not resolved:
        return JSONResponse({"error": "key not found"}, status_code=404)
    body = await _json(request)
    ok = state.auth.update(
        resolved,
        name=body.get("name"),
        role=body.get("role"),
        allowed_models=body.get("allowed_models"),
        rpm=body.get("rpm"),
        burst=body.get("burst"),
        token_quota=body.get("token_quota"),
        disabled=body.get("disabled"),
    )
    return {"updated": ok}


# ---------------------------------------------------------------------------
# History (admin)
# ---------------------------------------------------------------------------
@app.get("/history")
async def history(request: Request):
    if (err := _require_admin(request)):
        return err
    hours = int(request.query_params.get("hours", "24"))
    since = time.time() - hours * 3600
    stats = state.store.request_stats(since)
    by_model = state.store.model_stats(since, 20)
    return {"stats": stats, "by_model": by_model}


@app.get("/history/recent")
async def history_recent(request: Request):
    if (err := _require_admin(request)):
        return err
    limit = int(request.query_params.get("limit", "50"))
    return {"requests": state.store.recent_requests(limit)}


@app.get("/history/detail/{rid}")
async def history_detail(request: Request, rid: str):
    if (err := _require_admin(request)):
        return err
    try:
        detail = state.store.request_detail(int(rid))
    except ValueError:
        return JSONResponse({"error": "invalid id"}, status_code=400)
    if detail is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return detail


# ---------------------------------------------------------------------------
# Login / logout (cookie session)
# ---------------------------------------------------------------------------
@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(_LOGIN_HTML)


@app.post("/login")
async def login(request: Request):
    password = ""
    ctype = request.headers.get("Content-Type", "")
    if "application/json" in ctype:
        body = await _json(request)
        password = body.get("password") or ""
    else:
        form = await request.form()
        password = form.get("password") or ""
    if not state.auth.valid_dashboard_password(password):
        return JSONResponse({"error": "invalid dashboard password"}, status_code=401)
    token = state.sessions.create(DASHBOARD_SESSION_KEY)
    resp = JSONResponse({"ok": True})
    resp.set_cookie(SESSION_COOKIE, token, httponly=True, secure=config.COOKIE_SECURE,
                    samesite="lax", path="/")
    return resp


@app.post("/api/dashboard/password")
async def change_dashboard_password(request: Request):
    if (err := _require_admin(request)):
        return err
    body = await _json(request)
    password = body.get("password") or ""
    if len(password) < 4:
        return JSONResponse({"error": "password must be at least 4 characters"}, status_code=400)
    state.auth.set_dashboard_password(password)
    return {"ok": True}


@app.post("/logout")
async def logout(request: Request):
    ck = request.cookies.get(SESSION_COOKIE)
    if ck:
        state.sessions.delete(ck)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp


async def _json(request: Request) -> dict:
    try:
        data = await request.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
