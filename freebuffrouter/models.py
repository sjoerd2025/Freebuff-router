"""Freebuff model catalog + gateway-id -> upstream mapping.

Ported from FreebuffModels in internal/upstream/freebuff.go.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FreebuffModelConfig:
    gateway_id: str   # e.g. "fb/deepseek-v4-flash"
    upstream: str     # e.g. "deepseek/deepseek-v4-flash"
    agent: str        # e.g. "base2-free-deepseek-flash"
    reasoning: bool
    full_mode: bool   # True = only available in full access mode (US/EU exit)


FREEBUFF_MODELS: list[FreebuffModelConfig] = [
    FreebuffModelConfig("fb/deepseek-v4-flash", "deepseek/deepseek-v4-flash", "base2-free-deepseek-flash", True, False),
    FreebuffModelConfig("fb/mimo-v2.5", "mimo/mimo-v2.5", "base2-free-mimo", False, False),
    FreebuffModelConfig("fb/deepseek-v4-pro", "deepseek/deepseek-v4-pro", "base2-free-deepseek", True, True),
    FreebuffModelConfig("fb/minimax-m3", "minimax/minimax-m3", "base2-free-minimax-m3", False, True),
    FreebuffModelConfig("fb/gpt-5.6-luna", "openai/gpt-5.6-luna", "base2-free-luna", False, True),
    FreebuffModelConfig("fb/glm-5.2", "z-ai/glm-5.2", "base2-free-glm", True, True),
]

# Reference model used for quota lookups (available in all tiers).
FB_QUOTA_REFERENCE_MODEL = "deepseek/deepseek-v4-flash"


def is_freebuff_model(model: str) -> bool:
    return model.startswith("fb/")


def strip_fb_prefix(model: str) -> str:
    return model[len("fb/"):] if model.startswith("fb/") else model


def fb_model_config(gateway_id: str) -> FreebuffModelConfig:
    for m in FREEBUFF_MODELS:
        if m.gateway_id == gateway_id:
            return m
    # Default to deepseek-v4-flash
    return FREEBUFF_MODELS[0]


def model_list() -> list[dict]:
    """The /v1/models payload (Freebuff-only)."""
    out = []
    for m in FREEBUFF_MODELS:
        out.append({
            "id": m.gateway_id,
            "object": "model",
            "owned_by": "freebuff",
            "reasoning": m.reasoning,
        })
    return out
