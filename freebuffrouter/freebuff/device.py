"""Freebuff device / login-URL OAuth flow.

Ported from fbStartDeviceAuth / fbPollDeviceAuth in
internal/upstream/freebuff.go.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse, parse_qs

import httpx

from .. import config
from ..httpclient import short_client
from ..util import truncate_log, random_hex
from .account import probe_me


@dataclass
class DeviceStart:
    state: str
    auth_url: str
    fingerprint_id: str
    fingerprint_hash: str
    expires_at: int


@dataclass
class DevicePoll:
    status: str  # pending | ready | error
    auth_token: str = ""
    user_id: str = ""
    email: str = ""
    error: str = ""


async def start_device_auth() -> DeviceStart:
    fp_id = "gw-" + random_hex(12)
    client = short_client()
    resp = await client.post(
        config.FREEBUFF_API_BASE + "/api/auth/cli/code",
        json={"fingerprintId": fp_id},
        headers={
            "Content-Type": "application/json",
            "User-Agent": config.FREEBUFF_USER_AGENT,
        },
        timeout=config.FREEBUFF_SESSION_TIMEOUT,
    )
    body = resp.text
    if resp.status_code != 200:
        raise RuntimeError(f"fb device start [{resp.status_code}]: {truncate_log(body, 200)}")
    data = resp.json()
    login_url = data.get("loginUrl") or ""
    if not login_url:
        raise RuntimeError("fb device start: missing loginUrl")
    fingerprint_id = data.get("fingerprintId") or fp_id
    state = fingerprint_id
    code = _parse_auth_code(login_url)
    if code:
        state = code
    return DeviceStart(
        state=state,
        auth_url=login_url,
        fingerprint_id=fingerprint_id,
        fingerprint_hash=data.get("fingerprintHash") or "",
        expires_at=int(data.get("expiresAt") or 0),
    )


async def poll_device_auth(fingerprint_id: str, fingerprint_hash: str, expires_at: int) -> DevicePoll:
    client = short_client()
    params = {
        "fingerprintId": fingerprint_id,
        "fingerprintHash": fingerprint_hash,
        "expiresAt": str(expires_at),
    }
    resp = await client.get(
        config.FREEBUFF_API_BASE + "/api/auth/cli/status",
        params=params,
        headers={"User-Agent": config.FREEBUFF_USER_AGENT},
        timeout=config.FREEBUFF_SESSION_TIMEOUT,
    )
    if resp.status_code != 200:
        return DevicePoll(status="pending")
    try:
        data = resp.json()
    except Exception:
        return DevicePoll(status="pending")

    user = data.get("user") or {}
    token = user.get("authToken") or data.get("authToken") or data.get("token") or ""
    message = data.get("message") or ""

    if message == "Authentication successful!" and token:
        user_id = user.get("id") or data.get("userId") or data.get("uid") or ""
        email = user.get("email") or data.get("email") or ""
        if not user_id or not email:
            try:
                uid2, email2 = await probe_me(token)
                user_id = user_id or uid2
                email = email or email2
            except Exception:
                pass
        return DevicePoll(status="ready", auth_token=token, user_id=user_id, email=email)

    return DevicePoll(status="pending")


def _parse_auth_code(login_url: str) -> str:
    try:
        q = parse_qs(urlparse(login_url).query)
        v = q.get("auth_code")
        return v[0] if v else ""
    except Exception:
        return ""
