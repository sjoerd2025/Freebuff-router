"""ProxyFreebuff — chat completions handling for the Freebuff upstream.

Ported from ProxyFreebuff / fbStreamToNonStream / fbParseCooldown in
internal/upstream/freebuff.go. Returns a Starlette response (streaming or
JSON) and records tokens / output preview via the passed-in `ctx` dict so
the caller can log to sqlite.
"""
from __future__ import annotations

import asyncio
import copy
import json
import re
import time
from typing import Any, Optional

import httpx
from starlette.responses import JSONResponse, StreamingResponse

from .. import config
from ..httpclient import upstream_client
from ..health import UpstreamHealth
from ..util import truncate_log
from ..models import fb_model_config, strip_fb_prefix
from ..proxy_pool import ProxyPool
from .account import FreebuffAccountManager, FreebuffAccount
from .session import get_or_create_session, get_or_create_run, delete_session
from .transform import fb_transform

DENYLIST_HEADERS = {"set-cookie", "server", "via", "x-powered-by", "transfer-encoding",
                    "content-encoding", "content-length"}


def _error(ctx: dict, status: int, msg: str) -> JSONResponse:
    ctx["error_msg"] = msg
    ctx["response_body"] = json.dumps({"error": msg})
    ctx["status_code"] = status
    return JSONResponse({"error": msg}, status_code=status)


async def proxy_freebuff(
    body_map: dict[str, Any],
    am: FreebuffAccountManager,
    client_stream: bool,
    fb_health: Optional[UpstreamHealth],
    ctx: dict,
    is_disconnected,
    proxy_pool: Optional[ProxyPool] = None,
):
    if config.FREEBUFF_DISABLED or am is None or am.len() == 0:
        return _error(ctx, 503, "freebuff upstream not available")

    if fb_health is not None and not fb_health.can_request():
        fb_health.record_request(0, "circuit open")
        return _error(ctx, 503, "freebuff upstream circuit breaker open")

    gateway_model = body_map.get("model") or ""
    mc = fb_model_config(gateway_model)
    upstream_model = mc.upstream
    agent_id = mc.agent

    total = am.len()
    req_start = time.monotonic()
    if proxy_pool is not None:
        client, proxy_id = await proxy_pool.client_for("freebuff")
    else:
        client, proxy_id = upstream_client(), ""
    last_err = ""

    for attempt in range(total):
        if await is_disconnected():
            ctx["status_code"] = 499
            return None

        try:
            acc = am.next()
        except RuntimeError:
            break

        async with acc.lock:
            # 1. session
            try:
                sess = await get_or_create_session(client, acc.token, acc.user_id, upstream_model)
            except Exception as e:
                last_err = f"session: {e}"
                continue

            # 2. run
            try:
                run_id = await get_or_create_run(client, acc.token, agent_id)
            except Exception as e:
                last_err = f"run: {e}"
                continue

            # 3. transform (deep copy to avoid mutation across attempts)
            transformed = fb_transform(copy.deepcopy(body_map), upstream_model, sess.instance_id, run_id)

            headers = {
                "Authorization": "Bearer " + acc.token,
                "Content-Type": "application/json",
                "User-Agent": config.FREEBUFF_USER_AGENT,
                "x-freebuff-instance-id": sess.instance_id,
            }
            if acc.user_id:
                headers["x-freebuff-acting-user-id"] = acc.user_id

            timeout = config.FREEBUFF_UPSTREAM_TIMEOUT if client_stream else config.FREEBUFF_NONSTREAM_TIMEOUT

            resp = await _open_stream(client, transformed, headers, timeout)
            if resp is None:
                if proxy_pool:
                    proxy_pool.mark_result(proxy_id, True)
                last_err = "network: request failed"
                continue
            if proxy_pool:
                proxy_pool.mark_result(proxy_id, resp.status_code in (502, 503, 504))

            # 6. handle 428 (stale session) — recreate + retry once
            if resp.status_code == 428:
                text = await _read_capped(resp, 4096)
                await resp.aclose()
                if "waiting_room_required" in text:
                    await delete_session(client, acc.token)
                    try:
                        sess = await get_or_create_session(client, acc.token, acc.user_id, upstream_model)
                    except Exception as e:
                        last_err = f"session recreate: {e}"
                        continue
                    transformed = fb_transform(copy.deepcopy(body_map), upstream_model, sess.instance_id, run_id)
                    headers["x-freebuff-instance-id"] = sess.instance_id
                    resp = await _open_stream(client, transformed, headers, timeout)
                    if resp is None:
                        if proxy_pool:
                            proxy_pool.mark_result(proxy_id, True)
                        last_err = "network retry: request failed"
                        continue
                else:
                    last_err = f"428: {truncate_log(text, 200)}"
                    continue

            # 7. handle 429 (rate limited) — cooldown account
            if resp.status_code == 429:
                text = await _read_capped(resp, 4096)
                await resp.aclose()
                cooldown = _parse_cooldown(text)
                acc.cooldown_until = time.time() + cooldown
                am.save_account(acc)
                last_err = f"429: {truncate_log(text, 200)}"
                continue

            # 8. 5xx — try next account
            if resp.status_code >= 500:
                await resp.aclose()
                last_err = f"upstream {resp.status_code}"
                continue

            # 9. success
            if fb_health is not None:
                fb_health.record_request(time.monotonic() - req_start, None)
            ctx["upstream_account"] = acc.email
            ctx["status_code"] = resp.status_code

            if client_stream:
                return _streaming_response(resp, acc, am, ctx)
            else:
                agg = await _stream_to_nonstream(resp, upstream_model)
                await resp.aclose()
                usage = agg.get("usage") or {}
                ctx["tokens_in"] = int(usage.get("prompt_tokens") or 0)
                ctx["tokens_out"] = int(usage.get("completion_tokens") or 0)
                ctx["response_body"] = json.dumps(agg)
                try:
                    ctx["output_text"] = agg["choices"][0]["message"]["content"]
                except Exception:
                    pass
                am.increment_session_count(acc)
                return JSONResponse(agg, status_code=200)

    # all accounts failed
    if fb_health is not None:
        fb_health.record_request(time.monotonic() - req_start, "all accounts failed")
    return _error(ctx, 503, "all freebuff accounts failed: " + last_err)


async def _open_stream(client: httpx.AsyncClient, body: dict, headers: dict, timeout: float):
    """Send the upstream chat request as a streaming response (kept open)."""
    req = client.build_request(
        "POST",
        config.FREEBUFF_API_BASE + config.FREEBUFF_CHAT_PATH,
        json=body,
        headers=headers,
        timeout=timeout,
    )
    try:
        resp = await client.send(req, stream=True)
        return resp
    except httpx.HTTPError:
        return None


async def _read_capped(resp: httpx.Response, cap: int) -> str:
    chunks = []
    total = 0
    async for chunk in resp.aiter_bytes():
        chunks.append(chunk)
        total += len(chunk)
        if total >= cap:
            break
    return b"".join(chunks).decode("utf-8", "replace")


def _streaming_response(resp: httpx.Response, acc: FreebuffAccount,
                        am: FreebuffAccountManager, ctx: dict) -> StreamingResponse:
    out_headers = {}
    for k, v in resp.headers.items():
        if k.lower() in DENYLIST_HEADERS:
            continue
        out_headers[k] = v

    async def gen():
        buf = ""
        stream_usage: Optional[dict] = None
        full = []
        try:
            async for line in resp.aiter_lines():
                # aiter_lines strips newlines; re-add for spec-compliant SSE
                out_line = line + "\n"
                full.append(out_line)
                yield out_line.encode("utf-8")
                trimmed = line.strip()
                if trimmed.startswith("data: "):
                    data = trimmed[len("data: "):]
                    if data and data != "[DONE]":
                        try:
                            chunk = json.loads(data)
                        except Exception:
                            chunk = None
                        if isinstance(chunk, dict):
                            inner = chunk.get("data")
                            if isinstance(inner, dict) and "choices" in inner:
                                chunk = inner
                            u = chunk.get("usage")
                            if isinstance(u, dict):
                                stream_usage = u
        finally:
            await resp.aclose()
            if stream_usage:
                ctx["tokens_in"] = int(stream_usage.get("prompt_tokens") or 0)
                ctx["tokens_out"] = int(stream_usage.get("completion_tokens") or 0)
                ctx["response_body"] = "".join(full)
            am.increment_session_count(acc)

    return StreamingResponse(gen(), status_code=resp.status_code,
                             media_type="text/event-stream", headers=out_headers)


async def _stream_to_nonstream(resp: httpx.Response, model: str) -> dict[str, Any]:
    content = reasoning = finish_reason = resp_id = resp_model = ""
    usage: Optional[dict] = None

    async for line in resp.aiter_lines():
        line = line.strip()
        if not line.startswith("data: "):
            continue
        data = line[len("data: "):]
        if data == "[DONE]" or data == "":
            continue
        try:
            chunk = json.loads(data)
        except Exception:
            continue
        inner = chunk.get("data")
        if isinstance(inner, dict) and "choices" in inner:
            chunk = inner
        if chunk.get("id"):
            resp_id = chunk["id"]
        if chunk.get("model"):
            resp_model = chunk["model"]
        if isinstance(chunk.get("usage"), dict):
            usage = chunk["usage"]
        choices = chunk.get("choices")
        if not isinstance(choices, list) or not choices:
            continue
        choice = choices[0]
        if not isinstance(choice, dict):
            continue
        delta = choice.get("delta")
        if isinstance(delta, dict):
            if isinstance(delta.get("content"), str):
                content += delta["content"]
            if isinstance(delta.get("reasoning_content"), str):
                reasoning += delta["reasoning_content"]
        if choice.get("finish_reason"):
            finish_reason = choice["finish_reason"]

    if not finish_reason:
        finish_reason = "stop"
    if not resp_id:
        resp_id = "fb_" + str(time.time_ns())
    if not resp_model:
        resp_model = model
    if usage is None:
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    msg: dict[str, Any] = {"role": "assistant"}
    if content:
        msg["content"] = content
    elif reasoning:
        msg["content"] = reasoning
        msg["reasoning_used_as_content"] = True
    else:
        msg["content"] = ""
    if reasoning and content:
        msg["reasoning_content"] = reasoning

    return {
        "id": resp_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": resp_model,
        "choices": [{
            "index": 0,
            "message": msg,
            "finish_reason": finish_reason,
            "logprobs": None,
        }],
        "usage": usage,
    }


_TRY_AGAIN_RE = re.compile(r"try again in\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?", re.IGNORECASE)


def _parse_cooldown(body: str) -> float:
    """Return cooldown seconds from a 429 body."""
    try:
        data = json.loads(body)
        ms = int(((data.get("error") or {}).get("retryAfterMs")) or 0)
        if ms > 0:
            return ms / 1000.0
    except Exception:
        pass
    m = _TRY_AGAIN_RE.search(body.lower())
    if m and any(m.groups()):
        h = int(m.group(1) or 0)
        mn = int(m.group(2) or 0)
        s = int(m.group(3) or 0)
        if h or mn or s:
            return h * 3600 + mn * 60 + s
    return 60.0
