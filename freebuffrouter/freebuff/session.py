"""Freebuff session + run cache (in-memory, ephemeral).

Ported from the fbGetOrCreateSession / fbGetOrCreateRun family in
internal/upstream/freebuff.go.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import httpx

from .. import config
from ..util import random_hex


@dataclass
class FreebuffSession:
    instance_id: str
    model: str
    expires_at: float  # unix ts
    remaining_ms: int


@dataclass
class _RunEntry:
    run_id: str
    created_at: float


_session_cache: dict[str, FreebuffSession] = {}   # key: token:model
_run_cache: dict[str, _RunEntry] = {}             # key: token:agent
_cache_lock = asyncio.Lock()


def _headers(token: str, extra: Optional[dict] = None) -> dict:
    h = {
        "Authorization": "Bearer " + token,
        "User-Agent": config.FREEBUFF_USER_AGENT,
    }
    if extra:
        h.update(extra)
    return h


async def get_or_create_session(client: httpx.AsyncClient, token: str, user_id: str, model: str) -> FreebuffSession:
    cache_key = token + ":" + model
    async with _cache_lock:
        cached = _session_cache.get(cache_key)
    if cached and (cached.expires_at - time.time()) > config.FREEBUFF_SESSION_MIN_REMAINING:
        return cached

    sess = await _get_session(client, token)
    if sess and sess.model == model and (sess.expires_at - time.time()) > config.FREEBUFF_SESSION_MIN_REMAINING:
        async with _cache_lock:
            _session_cache[cache_key] = sess
        return sess

    if sess and sess.model != model:
        await delete_session(client, token)

    asyncio.create_task(_fire_ads_and_streak(client, token))

    sess = await _create_session(client, token, model)
    async with _cache_lock:
        _session_cache[cache_key] = sess
    return sess


async def _get_session(client: httpx.AsyncClient, token: str) -> Optional[FreebuffSession]:
    try:
        resp = await client.get(
            config.FREEBUFF_API_BASE + config.FREEBUFF_SESSION_PATH,
            headers=_headers(token),
            timeout=config.FREEBUFF_SESSION_TIMEOUT,
        )
    except httpx.HTTPError:
        return None
    if resp.status_code != 200:
        return None
    data = resp.json()
    if data.get("status") != "active":
        return None
    return _to_session(data)


async def _create_session(client: httpx.AsyncClient, token: str, model: str) -> FreebuffSession:
    resp = await client.post(
        config.FREEBUFF_API_BASE + config.FREEBUFF_SESSION_PATH,
        json={"model": model},
        headers=_headers(token, {"Content-Type": "application/json", "x-freebuff-model": model}),
        timeout=config.FREEBUFF_SESSION_TIMEOUT,
    )
    body = resp.text
    if resp.status_code != 200:
        raise RuntimeError(f"POST session: {resp.status_code} {body}")
    data = resp.json()

    if data.get("status") == "queued" and data.get("instanceId"):
        for _ in range(8):
            await asyncio.sleep(1.5)
            sess = await _get_session_with_instance(client, token, data["instanceId"])
            if sess:
                return sess
        raise RuntimeError("session stayed queued")

    if data.get("status") != "active" or not data.get("instanceId"):
        raise RuntimeError(f"session not active: {data.get('status')}")
    return _to_session(data)


async def _get_session_with_instance(client: httpx.AsyncClient, token: str, instance_id: str) -> Optional[FreebuffSession]:
    try:
        resp = await client.get(
            config.FREEBUFF_API_BASE + config.FREEBUFF_SESSION_PATH,
            headers=_headers(token, {"x-freebuff-instance-id": instance_id}),
            timeout=config.FREEBUFF_SESSION_TIMEOUT,
        )
    except httpx.HTTPError:
        return None
    if resp.status_code != 200:
        return None
    data = resp.json()
    if data.get("status") != "active":
        return None
    return _to_session(data)


async def delete_session(client: httpx.AsyncClient, token: str) -> None:
    try:
        resp = await client.request(
            "DELETE",
            config.FREEBUFF_API_BASE + config.FREEBUFF_SESSION_PATH,
            headers=_headers(token),
            timeout=config.FREEBUFF_SESSION_TIMEOUT,
        )
        await resp.aclose()
    except httpx.HTTPError:
        pass
    async with _cache_lock:
        for k in [k for k in _session_cache if k.startswith(token + ":")]:
            del _session_cache[k]


async def _fire_ads_and_streak(client: httpx.AsyncClient, token: str) -> None:
    ads_body = {
        "provider": "gravity",
        "sessionId": str(uuid.uuid4()),
        "surface": "waiting_room",
        "device": {"os": "linux", "timezone": "UTC", "locale": "en-US"},
        "userAgent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    }
    try:
        r = await client.post(
            config.FREEBUFF_API_BASE + config.FREEBUFF_ADS_PATH,
            json=ads_body,
            headers=_headers(token, {"Content-Type": "application/json"}),
            timeout=5.0,
        )
        await r.aclose()
    except httpx.HTTPError:
        pass
    try:
        r = await client.get(
            config.FREEBUFF_API_BASE + config.FREEBUFF_STREAK_PATH,
            headers=_headers(token),
            timeout=5.0,
        )
        await r.aclose()
    except httpx.HTTPError:
        pass


async def get_or_create_run(client: httpx.AsyncClient, token: str, agent_id: str) -> str:
    cache_key = token + ":" + agent_id
    async with _cache_lock:
        cached = _run_cache.get(cache_key)
    if cached and (time.time() - cached.created_at) < config.FREEBUFF_RUN_CACHE_TTL:
        return cached.run_id

    resp = await client.post(
        config.FREEBUFF_API_BASE + config.FREEBUFF_RUN_PATH,
        json={"action": "START", "agentId": agent_id, "ancestorRunIds": []},
        headers=_headers(token, {"Content-Type": "application/json"}),
        timeout=config.FREEBUFF_SESSION_TIMEOUT,
    )
    body = resp.text
    if resp.status_code != 200:
        raise RuntimeError(f"start run: {resp.status_code} {body}")
    data = resp.json()
    run_id = data.get("runId") or ""
    if not run_id:
        raise RuntimeError("no runId in response")
    async with _cache_lock:
        _run_cache[cache_key] = _RunEntry(run_id=run_id, created_at=time.time())
    return run_id


def _to_session(data: dict) -> FreebuffSession:
    from .account import _parse_rfc3339
    expires_at = _parse_rfc3339(data.get("expiresAt") or "")
    return FreebuffSession(
        instance_id=data.get("instanceId") or "",
        model=data.get("model") or "",
        expires_at=expires_at,
        remaining_ms=int(data.get("remainingMs") or 0),
    )
