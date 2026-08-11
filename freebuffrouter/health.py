"""Circuit breaker + passive health tracker.

Ported from internal/upstream/health.go for the Freebuff circuit breaker.
"""
from __future__ import annotations

import threading
import time
from enum import IntEnum
from typing import Optional

from . import config


class CircuitState(IntEnum):
    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2

    def __str__(self) -> str:
        return {0: "closed", 1: "open", 2: "half-open"}[int(self)]


class UpstreamHealth:
    def __init__(self, name: str):
        self.name = name
        self._mu = threading.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_errs = 0
        self._opened_at = 0.0
        self._half_open_probes = 0
        # passive stats
        self._total_requests = 0
        self._total_errors = 0
        self._total_latency_ms = 0
        self._last_request_at = 0.0
        self._last_error_at = 0.0
        self._last_error_msg = ""

    def state(self) -> CircuitState:
        with self._mu:
            return self._state

    def record_request(self, latency_sec: float, err: Optional[str]) -> None:
        with self._mu:
            self._total_requests += 1
            self._total_latency_ms += int(latency_sec * 1000)
            self._last_request_at = time.time()
            if err:
                self._total_errors += 1
                self._last_error_at = time.time()
                self._last_error_msg = err
                self._consecutive_errs += 1
                if self._consecutive_errs >= config.CB_OPEN_THRESHOLD and self._state == CircuitState.CLOSED:
                    self._state = CircuitState.OPEN
                    self._opened_at = time.time()
            else:
                if self._state == CircuitState.HALF_OPEN:
                    self._state = CircuitState.CLOSED
                    self._consecutive_errs = 0
                    self._half_open_probes = 0
                elif self._state == CircuitState.CLOSED:
                    self._consecutive_errs = 0

    def can_request(self) -> bool:
        with self._mu:
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.OPEN:
                if time.time() - self._opened_at >= config.CB_OPEN_DURATION:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_probes = 0
                    return True
                return False
            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_probes < config.CB_HALF_OPEN_PROBES:
                    self._half_open_probes += 1
                    return True
                return False
            return True

    def stats(self) -> dict:
        with self._mu:
            total = self._total_requests
            err = self._total_errors
            lat = self._total_latency_ms
            return {
                "name": self.name,
                "circuit_state": str(self._state),
                "consecutive_errs": self._consecutive_errs,
                "total_requests": total,
                "total_errors": err,
                "error_rate_pct": round((err / total * 100) if total else 0.0, 2),
                "avg_latency_ms": round((lat / total) if total else 0.0, 2),
                "last_error_msg": self._last_error_msg,
            }
