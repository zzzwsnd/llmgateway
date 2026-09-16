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
        error: GatewayError | None = None,
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
