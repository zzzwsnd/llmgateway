import json
from typing import Any

from app.model.session import SessionEvent, SessionEventType


def encode_sse(event: dict[str, Any]) -> str:
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


def encode_session_sse(event: SessionEvent) -> str:
    if event.type is SessionEventType.HEARTBEAT:
        return ": heartbeat\n\n"
    lines: list[str] = []
    if event.event_id is not None:
        lines.append(f"id: {event.event_id}")
    lines.append(f"event: {event.type.value}")
    lines.append(f"data: {json.dumps(event.data, ensure_ascii=False, separators=(',', ':'))}")
    return "\n".join(lines) + "\n\n"
