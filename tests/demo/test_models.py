from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.core.utils import encode_sse
from app.model.entity import (
    AttemptStatus,
    AttemptType,
    CallAttempt,
    CallTrace,
    JsonValidationStatus,
)
from app.model.enums import ModelEnum
from app.model.request import LLMRequest, Message


def test_call_trace_accepts_active_business_status() -> None:
    trace = CallTrace(
        request_id="request-1",
        timestamp="2026-09-20T00:00:00Z",
        requested_model="general-primary",
        input_tokens=0,
        output_tokens=0,
        cost_usd=0,
        latency_ms=0,
        attempts=0,
        status="running",
    )

    assert trace.status == "running"


def test_call_attempt_rejects_noncanonical_prompt_sha256() -> None:
    with pytest.raises(ValidationError, match="prompt_sha256"):
        CallAttempt(
            attempt_id="attempt-1",
            call_id="call-1",
            attempt_no=1,
            attempt_type=AttemptType.JSON_RETRY,
            model="general-primary",
            prompt_name="json-repair",
            prompt_version="json-repair-v1",
            prompt_sha256="A" * 64,
            status=AttemptStatus.RUNNING,
            json_validation_status=JsonValidationStatus.NOT_REQUESTED,
            started_at=datetime.now(timezone.utc),
        )


def test_request_rejects_stream_with_response_schema() -> None:
    with pytest.raises(ValidationError, match="stream 与 response_schema 不能同时使用"):
        LLMRequest(
            model="general-primary",
            messages=[Message(role="user", content="hello")],
            stream=True,
            response_schema={"type": "object"},
        )


def test_message_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Message(role="user", content="hello", unknown=True)


def test_encode_sse_preserves_unicode() -> None:
    assert encode_sse({"type": "content.delta", "delta": "你好"}) == (
        'data: {"type": "content.delta", "delta": "你好"}\n\n'
    )


def test_request_parses_registered_model_enum() -> None:
    request = LLMRequest(
        model="general-primary",
        messages=[Message(role="user", content="hello")],
    )

    assert request.model is ModelEnum.GENERAL_PRIMARY


def test_request_rejects_unregistered_model_name() -> None:
    with pytest.raises(ValidationError):
        LLMRequest(
            model="invented-model",
            messages=[Message(role="user", content="hello")],
        )
