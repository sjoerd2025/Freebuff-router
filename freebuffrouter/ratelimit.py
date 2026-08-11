"""Per-client token-bucket rate limiter + session store.

Ported from internal/ratelimit/ratelimit.go and
internal/auth/session_store.go.
"""
from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass


@dataclass
class _Bucket:
    tokens: float
    last: float


class RateLimiter:
    def __init__(self, limit: int, burst: int, window_sec: float):
        self.limit = limit
        self.burst = burst
        self.window = window_sec
        self._clients: dict[str, _Bucket] = {}
        self._mu = threading.Lock()

    def allow(self, client_id: str, rpm: int, burst: int) -> bool:
        if rpm <= 0:
            return True
        if burst <= 0:
            burst = rpm
        now = time.monotonic()
        with self._mu:
            b = self._clients.get(client_id)
            if b is None:
                self._clients[client_id] = _Bucket(tokens=float(burst) - 1, last=now)
                return True
            elapsed = now - b.last
            refill = elapsed * rpm / self.window
            b.tokens = min(b.tokens + refill, float(burst))
            b.last = now
            if b.tokens >= 1:
                b.tokens -= 1
                return True
            return False

    def cleanup(self) -> None:
        cutoff = time.monotonic() - 600
        with self._mu:
            for cid in [c for c, b in self._clients.items() if b.last < cutoff]:
                del self._clients[cid]


class SessionStore:
    """Session token -> API key mapping (256-bit random tokens)."""

    def __init__(self):
        self._m: dict[str, str] = {}
        self._mu = threading.Lock()

    def create(self, api_key: str) -> str:
        token = secrets.token_urlsafe(32)
        with self._mu:
            self._m[token] = api_key
        return token

    def lookup(self, token: str) -> str:
        with self._mu:
            return self._m.get(token, "")

    def delete(self, token: str) -> None:
        with self._mu:
            self._m.pop(token, None)
