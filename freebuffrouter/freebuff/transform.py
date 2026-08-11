"""Freebuff request body transformation.

Ported from fbTransform / fbEnsureBuffyPrefix / fbInjectEndTurnTool in
internal/upstream/freebuff.go.
"""
from __future__ import annotations

import uuid
from typing import Any

from .. import config
from ..util import random_hex


def fb_transform(body: dict[str, Any], upstream_model: str, instance_id: str, run_id: str) -> dict[str, Any]:
    """Mutate + return the transformed body dict (caller works on a copy)."""
    _ensure_buffy_prefix(body)
    _inject_end_turn_tool(body)

    body["model"] = upstream_model
    body["stream"] = True
    body["provider"] = {"data_collection": "deny"}
    body["stop"] = ['"cb_easp"']

    # Default max_tokens to 384K if client didn't set it, then clamp so
    # prompt + completion <= 1M.
    if "max_tokens" not in body and "max_completion_tokens" not in body:
        body["max_tokens"] = config.FB_DEFAULT_MAX_TOKENS

    mt = _as_int(body.get("max_tokens"))
    if mt > 0:
        estimated_prompt = 0
        msgs = body.get("messages")
        if isinstance(msgs, list):
            for m in msgs:
                if isinstance(m, dict) and isinstance(m.get("content"), str):
                    estimated_prompt += len(m["content"]) // 4
        max_allowed = config.FB_MAX_CONTEXT - estimated_prompt
        if max_allowed < 1000:
            max_allowed = 1000
        if mt > max_allowed:
            body["max_tokens"] = max_allowed

    body["codebuff_metadata"] = {
        "run_id": run_id,
        "client_id": "wf-" + random_hex(8),
        "cost_mode": "free",
        "freebuff_instance_id": instance_id,
        "trace_session_id": str(uuid.uuid4()),
    }

    body.pop("reasoning", None)
    body.pop("extra_body", None)
    return body


def _ensure_buffy_prefix(body: dict[str, Any]) -> None:
    msgs = body.get("messages")
    if not isinstance(msgs, list) or not msgs:
        body["messages"] = [{"role": "system", "content": config.FREEBUFF_BUFFY_PREFIX}]
        return

    first = msgs[0]
    if not isinstance(first, dict):
        return

    if first.get("role") != "system":
        body["messages"] = [{"role": "system", "content": config.FREEBUFF_BUFFY_PREFIX}] + msgs
        return

    content = first.get("content")
    if not isinstance(content, str):
        # multimodal array — prepend separate system message
        body["messages"] = [{"role": "system", "content": config.FREEBUFF_BUFFY_PREFIX}] + msgs
        return

    if not content.startswith(config.FREEBUFF_BUFFY_PREFIX):
        first["content"] = config.FREEBUFF_BUFFY_PREFIX + "\n\n" + content


def _inject_end_turn_tool(body: dict[str, Any]) -> None:
    tools = body.get("tools")
    if not isinstance(tools, list) or not tools:
        return
    for t in tools:
        if isinstance(t, dict):
            fn = t.get("function")
            if isinstance(fn, dict) and fn.get("name") == "end_turn":
                return
    tools.append({
        "type": "function",
        "function": {
            "name": "end_turn",
            "description": "Signal the end of the current task.",
            "parameters": {"type": "object", "properties": {}},
        },
    })


def _as_int(v: Any) -> int:
    if isinstance(v, bool):
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    try:
        return int(v)
    except Exception:
        return 0
