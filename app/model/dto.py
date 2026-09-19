from dataclasses import dataclass
from typing import Any, Literal

from app.model.enums import ModelEnum
from app.model.request import Message, StructuredOutputFormat
from app.model.response import JsonValue, Usage


@dataclass(frozen=True)
class ProviderCompletion:
    content: str
    usage: Usage


@dataclass(frozen=True)
class ProviderRequest:
    messages: list[Message]
    timeout_seconds: float
    response_schema: dict[str, Any] | None = None
    response_schema_name: str | None = None
    structured_output_format: StructuredOutputFormat | None = None
    temperature: float | None = None
    max_output_tokens: int | None = None
    stream_resume_token: str | None = None


@dataclass(frozen=True)
class ProviderStreamEvent:
    type: Literal["text_delta", "completed", "failed"]
    delta: str | None = None
    model: ModelEnum | None = None
    error_code: str | None = None
    upstream_first_delta_latency_ms: int | None = None


@dataclass(frozen=True)
class JsonParseResult:
    content: str
    value: JsonValue
    repaired: bool
