"""freebuff-router — Python port.

OpenAI-compatible API gateway for the Freebuff (codebuff.com) upstream.

This is a focused Python re-implementation of the Go gateway that:
  * serves only the Freebuff provider (fb/* models),
  * uses FastAPI + uvicorn with native SSE streaming,
  * replaces Redis (hot state) + ClickHouse (log history) with a single
    sqlite3 file (WAL mode) plus an in-process cache layer.
"""

__version__ = "1.0.0"
