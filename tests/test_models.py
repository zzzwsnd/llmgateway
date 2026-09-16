import pytest
from pydantic import ValidationError

from app.core.utils import encode_sse
from app.model.request import LLMRequest, Message


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
