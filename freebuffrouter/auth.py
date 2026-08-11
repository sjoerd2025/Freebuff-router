"""Gateway API-key management + model whitelist + per-key rate metadata.

Ported from internal/auth/auth.go. sqlite replaces Redis for persistence.
"""
from __future__ import annotations

import os
import hashlib
import hmac
import secrets
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from . import config
from .store import Store

ROLE_INFERENCE = "inference"
ROLE_ADMIN = "admin"
DASHBOARD_SESSION_KEY = "__dashboard_admin__"
DEFAULT_DASHBOARD_PASSWORD = "123456"
_PASSWORD_ITERATIONS = 310_000


def mask_key(key: str) -> str:
    if len(key) > 12:
        return key[:8] + "..." + key[-4:]
    return key


def match_model(pattern: str, model: str) -> bool:
    if pattern == model:
        return True
    if pattern.endswith("/*"):
        prefix = pattern[:-2]
        return model.startswith(prefix + "/")
    if pattern.endswith("*"):
        prefix = pattern[:-1]
        return model.startswith(prefix)
    return False


def _hash_password(password: str, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, _PASSWORD_ITERATIONS
    ).hex()


@dataclass
class GatewayKey:
    key: str
    name: str = ""
    role: str = ROLE_INFERENCE
    allowed_models: list[str] = field(default_factory=list)
    rpm: int = 0
    burst: int = 0
    token_quota: int = 0
    tokens_used: int = 0
    requests: int = 0
    created_at: int = 0
    disabled: bool = False

    @property
    def key_masked(self) -> str:
        return mask_key(self.key)

    def is_model_allowed(self, model: str) -> bool:
        if not self.allowed_models:
            return True
        return any(match_model(p, model) for p in self.allowed_models)

    def to_row(self) -> dict:
        return {
            "key": self.key,
            "name": self.name,
            "role": self.role,
            "allowed_models": ",".join(self.allowed_models),
            "rpm": self.rpm,
            "burst": self.burst,
            "token_quota": self.token_quota,
            "tokens_used": self.tokens_used,
            "requests": self.requests,
            "created_at": self.created_at,
            "disabled": 1 if self.disabled else 0,
        }

    def public(self, include_full: bool = False) -> dict:
        d = {
            "key_masked": self.key_masked,
            "name": self.name,
            "role": self.role,
            "allowed_models": list(self.allowed_models),
            "rpm": self.rpm,
            "burst": self.burst,
            "token_quota": self.token_quota,
            "tokens_used": self.tokens_used,
            "requests": self.requests,
            "created_at": self.created_at,
            "disabled": self.disabled,
        }
        if include_full:
            d["key"] = self.key
        return d


def _row_to_key(r: dict) -> GatewayKey:
    allowed = [m.strip() for m in (r.get("allowed_models") or "").split(",") if m.strip()]
    role = r.get("role") or ROLE_INFERENCE
    return GatewayKey(
        key=r["key"],
        name=r.get("name") or "",
        role=role,
        allowed_models=allowed,
        rpm=int(r.get("rpm") or 0),
        burst=int(r.get("burst") or 0),
        token_quota=int(r.get("token_quota") or 0),
        tokens_used=int(r.get("tokens_used") or 0),
        requests=int(r.get("requests") or 0),
        created_at=int(r.get("created_at") or 0),
        disabled=bool(r.get("disabled")),
    )


class AuthManager:
    def __init__(self, store: Store):
        self._store = store
        self._dashboard_password_hash = ""
        self._dashboard_password_salt = ""
        self._load_dashboard_password()
        self._keys: dict[str, GatewayKey] = {}
        self._mu = threading.RLock()
        self._bootstrap_info: Optional[str] = None
        self._load_or_bootstrap()

    def _load_dashboard_password(self) -> None:
        credential = self._store.load_dashboard_credential()
        if credential is None:
            salt = secrets.token_bytes(16)
            digest = _hash_password(DEFAULT_DASHBOARD_PASSWORD, salt)
            self._store.save_dashboard_credential(digest, salt.hex())
            self._dashboard_password_hash = digest
            self._dashboard_password_salt = salt.hex()
        else:
            self._dashboard_password_hash = credential["password_hash"]
            self._dashboard_password_salt = credential["password_salt"]

    def valid_dashboard_password(self, password: str) -> bool:
        try:
            salt = bytes.fromhex(self._dashboard_password_salt)
        except ValueError:
            return False
        candidate = _hash_password(password, salt)
        return hmac.compare_digest(candidate, self._dashboard_password_hash)

    def set_dashboard_password(self, password: str) -> None:
        if not password:
            raise ValueError("password cannot be empty")
        salt = secrets.token_bytes(16)
        digest = _hash_password(password, salt)
        self._store.save_dashboard_credential(digest, salt.hex())
        self._dashboard_password_hash = digest
        self._dashboard_password_salt = salt.hex()

    def _load_or_bootstrap(self) -> None:
        rows = self._store.load_gateway_keys()
        for r in rows:
            k = _row_to_key(r)
            self._keys[k.key] = k

        if not self._keys:
            bootstrap = self._collect_bootstrap_keys()
            for raw in bootstrap:
                k = GatewayKey(key=raw, name="bootstrap", role=ROLE_ADMIN, created_at=int(time.time()))
                self._keys[raw] = k
                self._store.save_gateway_key(k.to_row())

        if not self._keys:
            if config.GATEWAY_AUTH_DISABLE or config.GATEWAY_NO_AUTOBOOTSTRAP:
                return
            # auto-bootstrap admin key
            gen = "gw-" + secrets.token_hex(32)
            k = GatewayKey(key=gen, name="bootstrap", role=ROLE_ADMIN, created_at=int(time.time()))
            self._keys[gen] = k
            self._store.save_gateway_key(k.to_row())
            path = config.GATEWAY_KEY_FILE or "bootstrap-key.txt"
            try:
                p = Path(path).resolve()
                p.write_text(gen + "\n", encoding="utf-8")
                try:
                    os.chmod(p, 0o600)
                except Exception:
                    pass
                self._bootstrap_info = f"AUTO-BOOTSTRAP admin key written to {p} (masked {mask_key(gen)})"
            except Exception:
                self._bootstrap_info = f"AUTO-BOOTSTRAP admin key: {gen}"

    def _collect_bootstrap_keys(self) -> list[str]:
        out: list[str] = []
        if config.GATEWAY_API_KEYS:
            for k in config.GATEWAY_API_KEYS.replace("\n", ",").split(","):
                k = k.strip()
                if k:
                    out.append(k)
        src = config.GATEWAY_KEY_FILE or "./gateway-key.txt"
        try:
            data = Path(src).read_text(encoding="utf-8")
            for k in data.replace("\r", "\n").replace(",", "\n").split("\n"):
                k = k.strip()
                if k:
                    out.append(k)
        except Exception:
            pass
        return out

    @property
    def bootstrap_info(self) -> Optional[str]:
        return self._bootstrap_info

    # -- queries -----------------------------------------------------------
    def valid(self, key: str) -> bool:
        with self._mu:
            if not self._keys:
                return False
            info = self._keys.get(key)
            return bool(info) and not info.disabled

    def get(self, key: str) -> Optional[GatewayKey]:
        with self._mu:
            return self._keys.get(key)

    def lookup_limits(self, key: str) -> Optional[tuple[int, int]]:
        with self._mu:
            info = self._keys.get(key)
            if not info:
                return None
            return info.rpm, info.burst

    def get_all(self) -> list[GatewayKey]:
        with self._mu:
            return list(self._keys.values())

    def count(self) -> int:
        with self._mu:
            return len(self._keys)

    def count_admins(self) -> int:
        with self._mu:
            return sum(1 for k in self._keys.values() if k.role == ROLE_ADMIN and not k.disabled)

    def is_empty(self) -> bool:
        with self._mu:
            return len(self._keys) == 0

    def resolve_key(self, key_or_masked: str) -> Optional[str]:
        with self._mu:
            if key_or_masked in self._keys:
                return key_or_masked
            for k, info in self._keys.items():
                if info.key_masked == key_or_masked:
                    return k
        return None

    # -- mutations ---------------------------------------------------------
    def add(self, key: str, name: str, role: str, allowed_models: list[str],
            rpm: int, burst: int, token_quota: int) -> GatewayKey:
        info = GatewayKey(
            key=key, name=name, role=role or ROLE_INFERENCE,
            allowed_models=list(allowed_models or []), rpm=rpm, burst=burst,
            token_quota=token_quota, created_at=int(time.time()),
        )
        with self._mu:
            self._keys[key] = info
        self._store.save_gateway_key(info.to_row())
        return info

    def remove(self, key: str) -> None:
        with self._mu:
            self._keys.pop(key, None)
        self._store.delete_gateway_key(key)

    def update(self, key: str, *, name=None, role=None, allowed_models=None,
               rpm=None, burst=None, token_quota=None, disabled=None) -> bool:
        with self._mu:
            info = self._keys.get(key)
            if not info:
                return False
            if name:
                info.name = name
            if role in (ROLE_ADMIN, ROLE_INFERENCE):
                info.role = role
            if allowed_models is not None:
                info.allowed_models = list(allowed_models)
            if rpm is not None and rpm >= 0:
                info.rpm = rpm
            if burst is not None and burst >= 0:
                info.burst = burst
            if token_quota is not None and token_quota >= 0:
                info.token_quota = token_quota
            if disabled is not None:
                info.disabled = bool(disabled)
            row = info.to_row()
        self._store.save_gateway_key(row)
        return True

    def increment_tokens(self, key: str, amount: int) -> None:
        with self._mu:
            info = self._keys.get(key)
            if not info:
                return
            info.tokens_used += amount
            info.requests += 1
            if info.token_quota > 0 and info.tokens_used >= info.token_quota:
                info.disabled = True
            row = info.to_row() if info.disabled else None
        self._store.increment_gateway_key(key, amount, 1)
        if row is not None:
            self._store.save_gateway_key(row)

    def increment_requests(self, key: str) -> None:
        with self._mu:
            info = self._keys.get(key)
            if not info:
                return
            info.requests += 1
        self._store.increment_gateway_key(key, 0, 1)


def generate_gateway_key() -> str:
    return "gw-" + secrets.token_urlsafe(24)
