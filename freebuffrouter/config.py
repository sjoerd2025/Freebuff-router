"""Runtime configuration resolved from environment variables / .env file.

Values are loaded from the process environment first, then from a `.env`
file in the working directory (via python-dotenv). See `.env.example` for
the documented list of variables.
"""
from __future__ import annotations

import os

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
PORT = _env_int("PORT", 20130)
HOST = os.getenv("HOST", "127.0.0.1")
VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Persistence — single sqlite file
# ---------------------------------------------------------------------------
SQLITE_PATH = os.getenv("SQLITE_PATH", os.getenv("LOG_SQLITE_PATH", "freebuffrouter.db"))

# ---------------------------------------------------------------------------
# Auth / rate limiting
# ---------------------------------------------------------------------------
GATEWAY_KEY_FILE = os.getenv("GATEWAY_KEY_FILE", "")
GATEWAY_API_KEYS = os.getenv("GATEWAY_API_KEYS", "")
GATEWAY_AUTH_DISABLE = os.getenv("GATEWAY_AUTH_DISABLE") == "1"
GATEWAY_NO_AUTOBOOTSTRAP = os.getenv("GATEWAY_NO_AUTOBOOTSTRAP") == "1"

RATE_LIMIT_RPM = _env_int("RATE_LIMIT_RPM", 60)
RATE_LIMIT_BURST = _env_int("RATE_LIMIT_BURST", 10)
RATE_LIMIT_WINDOW_SEC = _env_float("RATE_LIMIT_WINDOW_SEC", 60.0)

COOKIE_SECURE = os.getenv("COOKIE_SECURE", "1") != "0"

# ---------------------------------------------------------------------------
# Workers / probes gates
# ---------------------------------------------------------------------------
WORKERS_DISABLED = os.getenv("WORKERS_DISABLED") == "1"
HEALTH_PROBES_DISABLED = os.getenv("HEALTH_PROBES_DISABLED") == "1"

# ---------------------------------------------------------------------------
# Freebuff upstream
# ---------------------------------------------------------------------------
FREEBUFF_DISABLED = os.getenv("FREEBUFF_DISABLED") == "1"

FREEBUFF_API_BASE = os.getenv("FREEBUFF_API_BASE", "https://www.codebuff.com")
FREEBUFF_CHAT_PATH = "/api/v1/chat/completions"
FREEBUFF_SESSION_PATH = "/api/v1/freebuff/session"
FREEBUFF_RUN_PATH = "/api/v1/agent-runs"
FREEBUFF_ME_PATH = "/api/v1/me"
FREEBUFF_STREAK_PATH = "/api/v1/freebuff/streak"
FREEBUFF_ADS_PATH = "/api/v1/ads"

FREEBUFF_USER_AGENT = os.getenv("FREEBUFF_USER_AGENT", "Freebuff-CLI/0.0.142")
FREEBUFF_BUFFY_PREFIX = "You are Buffy, the strategic coding assistant."

# Timeouts (seconds)
FREEBUFF_UPSTREAM_TIMEOUT = _env_float("FREEBUFF_UPSTREAM_TIMEOUT", 25.0)
FREEBUFF_SESSION_TIMEOUT = _env_float("FREEBUFF_SESSION_TIMEOUT", 12.0)
FREEBUFF_NONSTREAM_TIMEOUT = _env_float("FREEBUFF_NONSTREAM_TIMEOUT", 50.0)

# Session cache: reuse if > 60s remaining
FREEBUFF_SESSION_MIN_REMAINING = _env_float("FREEBUFF_SESSION_MIN_REMAINING", 60.0)
# Run cache: reuse runId for 10 minutes
FREEBUFF_RUN_CACHE_TTL = _env_float("FREEBUFF_RUN_CACHE_TTL", 600.0)

# Model context limit (DeepSeek V4 Flash = 1M tokens)
FB_MAX_CONTEXT = 1_048_576
FB_DEFAULT_MAX_TOKENS = 393_216  # 384K

# ---------------------------------------------------------------------------
# Circuit breaker (health tracker)
# ---------------------------------------------------------------------------
CB_OPEN_THRESHOLD = _env_int("CB_OPEN_THRESHOLD", 5)
CB_OPEN_DURATION = _env_float("CB_OPEN_DURATION", 60.0)
CB_HALF_OPEN_PROBES = _env_int("CB_HALF_OPEN_PROBES", 1)

# ---------------------------------------------------------------------------
# Quota sync worker cadence
# ---------------------------------------------------------------------------
FB_QUOTA_SYNC_TICK = _env_float("FB_QUOTA_SYNC_TICK", 300.0)          # 5 minutes
FB_QUOTA_SYNC_STARTUP_DELAY = _env_float("FB_QUOTA_SYNC_STARTUP_DELAY", 10.0)

# ---------------------------------------------------------------------------
# Request body cap (bytes)
# ---------------------------------------------------------------------------
MAX_REQUEST_BODY = _env_int("MAX_REQUEST_BODY", 10 * 1024 * 1024)  # 10MB


def env_or(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if v else default
