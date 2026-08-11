"""Shared async HTTP clients (httpx), mirroring the Go connection pools.

We keep two clients:
  * `upstream` — long timeout for chat streaming.
  * `short` — short timeout for probes / session / device flow.
"""
from __future__ import annotations

import httpx

# Long-lived pooled clients created lazily on first use so they bind to the
# running event loop.
_upstream: httpx.AsyncClient | None = None
_short: httpx.AsyncClient | None = None


def upstream_client() -> httpx.AsyncClient:
    global _upstream
    if _upstream is None:
        _upstream = httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            follow_redirects=False,
            http2=True,
        )
    return _upstream


def short_client() -> httpx.AsyncClient:
    global _short
    if _short is None:
        _short = httpx.AsyncClient(
            timeout=httpx.Timeout(12.0, connect=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
            follow_redirects=False,
        )
    return _short


async def aclose() -> None:
    global _upstream, _short
    for c in (_upstream, _short):
        if c is not None:
            await c.aclose()
    _upstream = None
    _short = None
