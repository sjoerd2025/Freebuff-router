"""Small shared helpers."""
from __future__ import annotations

import secrets


def truncate_log(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len] + "..."


def mask_token(token: str) -> str:
    if len(token) <= 12:
        return token
    return token[:8] + "..." + token[-4:]


def random_hex(n: int) -> str:
    return secrets.token_hex(n)
