from collections.abc import AsyncIterator
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.core.errors import GatewayError
from app.main import create_app
from app.model.entity import CallTrace
from app.model.request import LLMRequest
from app.model.response import LLMResponse, Usage


class FakeLLMService:
    def __init__(
        self,
        response: LLMResponse | None = None,
        error: Exception | None = None,
        stream_chunks: list[str] | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.stream_chunks = list(stream_chunks or [])

    async def complete(self, request: LLMRequest) -> LLMResponse:
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response

    def stream(self, request: LLMRequest) -> AsyncIterator[str]:
        if self.error is not None:
            raise self.error

        async def iterate() -> AsyncIterator[str]:
            for chunk in self.stream_chunks:
                yield chunk

        return iterate()


class FakeTraceService:
    def __init__(self, traces: list[CallTrace] | None = None) -> None:
        self.traces = list(traces or [])

    def list_traces(self) -> list[CallTrace]:
        return list(self.traces)


class FakeNativeService:
    def __init__(
        self,
        response: dict[str, object] | None = None,
        error: Exception | None = None,
        stream_chunks: list[str] | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.stream_chunks = list(stream_chunks or [])
        self.complete_requests: list[object] = []
        self.stream_requests: list[object] = []

    async def complete(self, request: object) -> dict[str, object]:
        self.complete_requests.append(request)
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response

    def stream(self, request: object) -> AsyncIterator[str]:
        self.stream_requests.append(request)
        if self.error is not None:
            raise self.error

        async def iterate() -> AsyncIterator[str]:
            for chunk in self.stream_chunks:
                yield chunk

        return iterate()


class FakeModelService:
    def __init__(
        self,
        response: dict[str, object] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.response = response or {"object": "list", "data": []}
        self.error = error

    def list_models(self) -> dict[str, object]:
        if self.error is not None:
            raise self.error
        return self.response


def create_test_app(
    *,
    chat_service: FakeNativeService | None = None,
    responses_service: FakeNativeService | None = None,
    model_service: FakeModelService | None = None,
):
    return create_app(
        FakeLLMService(),
        FakeTraceService(),
        chat_completions_service=chat_service or FakeNativeService(response={}),
        responses_service=responses_service or FakeNativeService(response={}),
        model_service=model_service or FakeModelService(),
    )


def valid_request() -> dict[str, object]:
    return {
        "model": "general-primary",
        "messages": [{"role": "user", "content": "hello"}],
    }


def test_gateway_module_exports_app() -> None:
    from gateway import app

    assert app.title == "Agent LLM Gateway"
    assert app.version == "0.0.1"


def test_non_stream_endpoint_returns_service_response() -> None:
    expected = LLMResponse(
        request_id="request-1",
        model="general-primary",
        content="answer",
        usage=Usage(input_tokens=1, output_tokens=2),
        latency_ms=3,
        attempts=1,
    )
    app = create_app(FakeLLMService(response=expected), FakeTraceService())

    with TestClient(app) as client:
        response = client.post("/v1/llm", json=valid_request())

    assert response.status_code == 200
    assert response.json() == expected.model_dump(mode="json")


def test_stream_endpoint_returns_event_stream() -> None:
    chunks = [
        'data: {"type": "content.delta", "delta": "hello"}\n\n',
        'data: {"type": "response.completed", "model": "general-primary"}\n\n',
    ]
    app = create_app(
        FakeLLMService(stream_chunks=chunks),
        FakeTraceService(),
    )

    with TestClient(app) as client:
        response = client.post("/v1/llm/stream", json=valid_request())

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.text == "".join(chunks)


def test_trace_endpoint_returns_service_traces() -> None:
    trace = CallTrace(
        request_id="request-1",
        timestamp=datetime(2026, 9, 16, tzinfo=timezone.utc),
        requested_model="general-primary",
        actual_model="general-primary",
        input_tokens=1,
        output_tokens=2,
        cost_usd=0.000009,
        latency_ms=3,
        attempts=1,
        status="success",
    )
    app = create_app(FakeLLMService(), FakeTraceService([trace]))

    with TestClient(app) as client:
        response = client.get("/v1/traces")

    assert response.status_code == 200
    assert response.json() == [trace.model_dump(mode="json")]


def test_gateway_error_is_converted_globally() -> None:
    app = create_app(
        FakeLLMService(
            error=GatewayError(
                "unknown_model",
                "模型不在 Gateway 允许列表中",
                400,
            )
        ),
        FakeTraceService(),
    )

    with TestClient(app) as client:
        response = client.post("/v1/llm", json=valid_request())

    assert response.status_code == 400
    assert response.json() == {
        "detail": {
            "code": "unknown_model",
            "message": "模型不在 Gateway 允许列表中",
        }
    }


def test_unexpected_error_is_converted_without_leaking_details() -> None:
    app = create_app(
        FakeLLMService(error=RuntimeError("sensitive internal detail")),
        FakeTraceService(),
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/v1/llm", json=valid_request())

    assert response.status_code == 500
    assert response.json() == {
        "detail": {
            "code": "internal_server_error",
            "message": "服务内部错误",
        }
    }


def test_chat_completions_non_stream_endpoint_returns_native_response() -> None:
    expected = {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 1,
        "model": "general-primary",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "answer"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
    }
    service = FakeNativeService(response=expected)

    with TestClient(create_test_app(chat_service=service)) as client:
        response = client.post("/v1/chat/completions", json=valid_request())

    assert response.status_code == 200
    assert response.json() == expected
    assert len(service.complete_requests) == 1
    assert service.stream_requests == []


def test_chat_completions_stream_endpoint_wraps_native_sse() -> None:
    chunks = [
        'data: {"object":"chat.completion.chunk"}\n\n',
        "data: [DONE]\n\n",
    ]
    service = FakeNativeService(stream_chunks=chunks)
    request = {**valid_request(), "stream": True}

    with TestClient(create_test_app(chat_service=service)) as client:
        response = client.post("/v1/chat/completions", json=request)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.text == "".join(chunks)
    assert len(service.stream_requests) == 1
    assert service.complete_requests == []


def test_responses_non_stream_endpoint_returns_native_response() -> None:
    expected = {
        "id": "resp_test",
        "object": "response",
        "created_at": 1,
        "status": "completed",
        "model": "general-responses",
        "output": [],
        "parallel_tool_calls": False,
        "tool_choice": "none",
        "tools": [],
        "error": None,
        "usage": None,
    }
    service = FakeNativeService(response=expected)

    with TestClient(create_test_app(responses_service=service)) as client:
        response = client.post(
            "/v1/responses",
            json={"model": "general-responses", "input": "hello"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert len(service.complete_requests) == 1
    assert service.stream_requests == []


def test_responses_stream_endpoint_wraps_native_sse() -> None:
    chunks = [
        'data: {"type":"response.created","sequence_number":0}\n\n',
        'data: {"type":"response.completed","sequence_number":1}\n\n',
    ]
    service = FakeNativeService(stream_chunks=chunks)

    with TestClient(create_test_app(responses_service=service)) as client:
        response = client.post(
            "/v1/responses",
            json={"model": "general-responses", "input": "hello", "stream": True},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.text == "".join(chunks)
    assert len(service.stream_requests) == 1
    assert service.complete_requests == []


def test_models_endpoint_exposes_only_public_metadata() -> None:
    expected = {
        "object": "list",
        "data": [
            {
                "id": "general-primary",
                "object": "model",
                "owned_by": "deepseek",
                "protocol": "chat_completions",
                "capabilities": {
                    "streaming": True,
                    "structured_output": True,
                    "tools": False,
                    "multimodal": False,
                },
            }
        ],
    }

    with TestClient(
        create_test_app(model_service=FakeModelService(expected))
    ) as client:
        response = client.get("/v1/models")

    assert response.status_code == 200
    assert response.json() == expected
    serialized = response.text
    for private_name in (
        "base_url",
        "api_key_env",
        "provider_model",
        "pricing",
        "fallback",
    ):
        assert private_name not in serialized


def test_default_models_endpoint_excludes_secrets_and_private_configuration(
    monkeypatch,
) -> None:
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-secret-sentinel")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-secret-sentinel")

    with TestClient(create_app()) as client:
        response = client.get("/v1/models")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"object", "data"}
    assert [model["id"] for model in payload["data"]] == [
        "general-primary",
        "general-backup",
        "general-responses",
    ]
    for model in payload["data"]:
        assert set(model) == {
            "id",
            "object",
            "owned_by",
            "protocol",
            "capabilities",
        }
        assert set(model["capabilities"]) == {
            "streaming",
            "structured_output",
            "tools",
            "multimodal",
        }
    for private_value in (
        "deepseek-secret-sentinel",
        "openai-secret-sentinel",
        "DEEPSEEK_API_KEY",
        "OPENAI_API_KEY",
        "https://api.deepseek.com",
        "https://api.openai.com/v1",
        "deepseek-v4-flash",
        "deepseek-chat",
        "gpt-4.1-mini",
        '"pricing"',
        '"input_per_million"',
        '"output_per_million"',
        '"fallback"',
        '"base_url"',
        '"api_key_env"',
        '"provider_model"',
    ):
        assert private_value not in response.text


def test_native_gateway_error_uses_openai_error_body() -> None:
    service = FakeNativeService(
        error=GatewayError("protocol_mismatch", "Selected model is incompatible", 400)
    )

    with TestClient(create_test_app(chat_service=service)) as client:
        response = client.post("/v1/chat/completions", json=valid_request())

    assert response.status_code == 400
    assert response.json() == {
        "error": {
            "message": "Selected model is incompatible",
            "type": "invalid_request_error",
            "param": None,
            "code": "protocol_mismatch",
        }
    }


def test_native_model_enum_validation_uses_openai_error_body() -> None:
    with TestClient(create_test_app()) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "private-provider-model",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )

    assert response.status_code == 422
    assert response.json()["error"]["type"] == "invalid_request_error"
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["error"]["param"] == "model"
    assert "private-provider-model" not in response.json()["error"]["message"]


def test_native_unexpected_error_is_sanitized_in_openai_error_body() -> None:
    service = FakeNativeService(error=RuntimeError("sensitive internal detail"))

    with TestClient(
        create_test_app(responses_service=service), raise_server_exceptions=False
    ) as client:
        response = client.post(
            "/v1/responses",
            json={"model": "general-responses", "input": "hello"},
        )

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "message": "Internal server error",
            "type": "server_error",
            "param": None,
            "code": "internal_server_error",
        }
    }


def test_chat_structured_stream_is_rejected_before_service_call() -> None:
    service = FakeNativeService(response={})
    payload = {
        **valid_request(),
        "stream": True,
        "response_format": {"type": "json_object"},
    }

    with TestClient(create_test_app(chat_service=service)) as client:
        response = client.post("/v1/chat/completions", json=payload)

    assert response.status_code == 422
    assert response.json()["error"]["type"] == "invalid_request_error"
    assert response.json()["error"]["code"] == "validation_error"
    assert service.complete_requests == []
    assert service.stream_requests == []


def test_responses_structured_stream_is_rejected_before_service_call() -> None:
    service = FakeNativeService(response={})
    payload = {
        "model": "general-responses",
        "input": "hello",
        "stream": True,
        "text": {"format": {"type": "json_object"}},
    }

    with TestClient(create_test_app(responses_service=service)) as client:
        response = client.post("/v1/responses", json=payload)

    assert response.status_code == 422
    assert response.json()["error"]["type"] == "invalid_request_error"
    assert response.json()["error"]["code"] == "validation_error"
    assert service.complete_requests == []
    assert service.stream_requests == []
