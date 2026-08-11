"""Freebuff account pool + manager.

Ported from FreebuffAccount / FreebuffAccountManager in
internal/upstream/freebuff.go. Redis persistence replaced by sqlite Store.
"""
from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

from .. import config
from ..httpclient import short_client
from ..store import Store
from ..models import FB_QUOTA_REFERENCE_MODEL
from ..util import truncate_log, mask_token


@dataclass
class FreebuffAccount:
    token: str
    user_id: str = ""
    email: str = ""
    disabled: bool = False
    disabled_at: float = 0.0
    cooldown_until: float = 0.0     # unix ts, 0 = none
    quota_recent: float = 0.0
    quota_limit: float = 0.0
    quota_synced_at: float = 0.0
    quota_reset_at: float = 0.0     # unix ts
    quota_period: str = ""
    hourly_session_count: int = 0
    hourly_window_start: float = 0.0
    # Per-account serialization — Freebuff upstream breaks on concurrent >1.
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, compare=False, repr=False)

    def to_row(self) -> dict:
        return {
            "token": self.token,
            "user_id": self.user_id,
            "email": self.email,
            "disabled": 1 if self.disabled else 0,
            "disabled_at": int(self.disabled_at),
            "cooldown_until": int(self.cooldown_until),
            "quota_recent": self.quota_recent,
            "quota_limit": self.quota_limit,
            "quota_synced_at": int(self.quota_synced_at),
            "quota_reset_at": int(self.quota_reset_at),
            "quota_period": self.quota_period,
        }

    def status(self) -> str:
        if self.disabled:
            return "disabled"
        if self.cooldown_until and time.time() < self.cooldown_until:
            return "cooldown"
        return "active"

    def public(self) -> dict:
        return {
            "token": mask_token(self.token),
            "token_full": self.token,
            "user_id": self.user_id,
            "email": self.email,
            "status": self.status(),
            "disabled": self.disabled,
            "cooldown_until": self.cooldown_until,
            "quota_recent": self.quota_recent,
            "quota_limit": self.quota_limit,
            "quota_reset_at": self.quota_reset_at,
            "quota_period": self.quota_period,
            "quota_synced_at": self.quota_synced_at,
            "hourly_session_count": self.hourly_session_count,
        }


def _row_to_account(r: dict) -> FreebuffAccount:
    return FreebuffAccount(
        token=r["token"],
        user_id=r.get("user_id") or "",
        email=r.get("email") or "",
        disabled=bool(r.get("disabled")),
        disabled_at=float(r.get("disabled_at") or 0),
        cooldown_until=float(r.get("cooldown_until") or 0),
        quota_recent=float(r.get("quota_recent") or 0),
        quota_limit=float(r.get("quota_limit") or 0),
        quota_synced_at=float(r.get("quota_synced_at") or 0),
        quota_reset_at=float(r.get("quota_reset_at") or 0),
        quota_period=r.get("quota_period") or "",
    )


class FreebuffAccountManager:
    def __init__(self, store: Store):
        self._store = store
        self._accounts: dict[str, FreebuffAccount] = {}
        self._counter = itertools.count()

    # -- load/save ---------------------------------------------------------
    def load(self) -> int:
        for r in self._store.load_fb_accounts():
            acc = _row_to_account(r)
            self._accounts[acc.token] = acc
        return len(self._accounts)

    def save_account(self, acc: FreebuffAccount) -> None:
        self._store.save_fb_account(acc.to_row())

    # -- queries -----------------------------------------------------------
    def len(self) -> int:
        return len(self._accounts)

    def get_account(self, token: str) -> Optional[FreebuffAccount]:
        return self._accounts.get(token)

    def list_accounts(self) -> list[dict]:
        return [a.public() for a in self._accounts.values()]

    def quota_summary(self) -> tuple[float, float, int]:
        total_recent = total_limit = 0.0
        exhausted = 0
        for a in self._accounts.values():
            total_recent += a.quota_recent
            total_limit += a.quota_limit
            if a.quota_limit > 0 and a.quota_recent >= a.quota_limit:
                exhausted += 1
        return total_recent, total_limit, exhausted

    def account_status(self) -> dict[str, int]:
        active = cooldown = disabled = 0
        for a in self._accounts.values():
            status = a.status()
            if status == "active":
                active += 1
            elif status == "cooldown":
                cooldown += 1
            else:
                disabled += 1
        return {"active": active, "cooldown": cooldown, "disabled": disabled}

    def next(self) -> FreebuffAccount:
        """Quota-aware selection: skip disabled/cooldown/exhausted, prefer
        the lowest quota_recent, then round-robin among ties-by-order."""
        if not self._accounts:
            raise RuntimeError("no freebuff accounts available")

        now = time.time()
        eligible: list[FreebuffAccount] = []
        earliest_reset = 0.0
        for a in self._accounts.values():
            cooldown = a.cooldown_until and now < a.cooldown_until
            exhausted = a.quota_limit > 0 and a.quota_recent >= a.quota_limit
            if a.quota_reset_at and (earliest_reset == 0.0 or a.quota_reset_at < earliest_reset):
                earliest_reset = a.quota_reset_at
            if not a.disabled and not cooldown and not exhausted:
                eligible.append(a)

        if not eligible:
            if earliest_reset:
                import datetime
                iso = datetime.datetime.fromtimestamp(earliest_reset, tz=datetime.timezone.utc).isoformat()
                raise RuntimeError(f"all freebuff accounts quota exhausted, resets at {iso}")
            raise RuntimeError("all freebuff accounts on cooldown or disabled")

        eligible.sort(key=lambda a: a.quota_recent)
        idx = next(self._counter)
        return eligible[idx % len(eligible)]

    def increment_session_count(self, acc: FreebuffAccount) -> None:
        now = time.time()
        hour_start = now - (now % 3600)
        if acc.hourly_window_start == 0.0 or acc.hourly_window_start < hour_start:
            acc.hourly_window_start = hour_start
            acc.hourly_session_count = 0
        acc.hourly_session_count += 1

    # -- add/remove --------------------------------------------------------
    async def add_account(self, token: str) -> tuple[bool, int]:
        return await self.add_account_with_info(token, "", "")

    async def add_account_with_info(self, token: str, user_id: str, email: str) -> tuple[bool, int]:
        existing = self._accounts.get(token)
        if existing:
            updated = False
            if not existing.email and email:
                existing.email = email
                updated = True
            if not existing.user_id and user_id:
                existing.user_id = user_id
                updated = True
            if updated:
                self.save_account(existing)
            return False, self.len()

        if not user_id or not email:
            uid2, email2 = await probe_me(token)
            user_id = user_id or uid2
            email = email or email2

        acc = FreebuffAccount(token=token, user_id=user_id, email=email)
        self._accounts[token] = acc
        self.save_account(acc)
        try:
            await self.sync_quota(acc)
        except Exception:
            pass
        return True, self.len()

    def remove_account(self, token: str) -> None:
        self._accounts.pop(token, None)
        self._store.delete_fb_account(token)

    # -- quota sync --------------------------------------------------------
    async def sync_quota(self, acc: FreebuffAccount) -> None:
        client = short_client()
        try:
            resp = await client.get(
                config.FREEBUFF_API_BASE + config.FREEBUFF_SESSION_PATH,
                headers={
                    "Authorization": "Bearer " + acc.token,
                    "User-Agent": config.FREEBUFF_USER_AGENT,
                },
                timeout=10.0,
            )
        except httpx.HTTPError as e:
            raise RuntimeError(f"quota sync: {e}")
        body = resp.text
        if resp.status_code != 200:
            raise RuntimeError(f"quota sync [{resp.status_code}]: {truncate_log(body, 200)}")
        try:
            data = resp.json()
        except Exception as e:
            raise RuntimeError(f"quota sync parse: {e}")

        rl_map = data.get("rateLimitsByModel") or {}
        rl = rl_map.get(FB_QUOTA_REFERENCE_MODEL)
        if not rl:
            acc.quota_synced_at = time.time()
            self.save_account(acc)
            return

        acc.quota_recent = float(rl.get("recentCount") or 0)
        acc.quota_limit = float(rl.get("limit") or 0)
        acc.quota_period = rl.get("period") or ""
        acc.quota_synced_at = time.time()
        reset_at = rl.get("resetAt") or ""
        if reset_at:
            ts = _parse_rfc3339(reset_at)
            if ts:
                acc.quota_reset_at = ts
                if acc.quota_limit > 0 and acc.quota_recent >= acc.quota_limit:
                    acc.cooldown_until = ts
        self.save_account(acc)

    async def sync_all_quota(self) -> int:
        synced = 0
        for acc in list(self._accounts.values()):
            try:
                await self.sync_quota(acc)
                synced += 1
            except Exception:
                pass
        return synced


async def probe_me(token: str) -> tuple[str, str]:
    """GET /api/v1/me → (user_id, email)."""
    client = short_client()
    resp = await client.get(
        config.FREEBUFF_API_BASE + config.FREEBUFF_ME_PATH,
        headers={
            "Authorization": "Bearer " + token,
            "User-Agent": config.FREEBUFF_USER_AGENT,
        },
        timeout=10.0,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"probe /me: {resp.status_code}")
    data = resp.json()
    return data.get("id") or "", data.get("email") or ""


def _parse_rfc3339(s: str) -> float:
    import datetime
    try:
        s2 = s.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(s2).timestamp()
    except Exception:
        return 0.0
