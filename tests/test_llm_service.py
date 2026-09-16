import json
from collections.abc import AsyncIterator
from typing import Any

import pytest

from app.core.errors import GatewayError, RetryableProviderError
from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.provider_dao import ProviderDao
from app.dao.trace_dao import TraceDao
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.model.dto import ProviderCompletion
from app.model.entity import ModelConfig
from app.model.request import LLMRequest, Message
from app.model.response import Usage
from app.service.llm_service import LLMService
from app.service.prompt_service import PromptService
from app.service.trace_service import TraceService


class FakeProviderMapper:
    def __init__(
        self,
        completions: list[ProviderCompletion | Exception] | None = None,
        streams: list[list[str | Exception]] | None = None,
    ) -> None:
        self.completions = list(completions or [])
        self.streams = list(streams or [])
        self.complete_models: list[str] = []
        self.stream_models: list[str] = []

    async def complete(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
        response_schema: dict[str, Any] | None,
    ) -> ProviderCompletion:
        self.complete_models.append(config.provider_model)
        result = self.completions.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def stream(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
    ) -> AsyncIterator[str]:
        self.stream_models.append(config.provider_model)
        for item in self.streams.pop(0):
            if isinstance(item, Exception):
                raise item
            yield item


def build_service(mapper: FakeProviderMapper) -> tuple[LLMService, TraceService]:
    model_dao = ModelDao(MemoryModelMapper())
    trace_service = TraceService(model_dao, TraceDao(MemoryTraceMapper()))
    service = LLMService(
        model_dao=model_dao,
        provider_dao=ProviderDao(mapper),
        prompt_service=PromptService(PromptDao(MemoryPromptMapper())),
        trace_service=trace_service,
    )
    return service, trace_service


def request(**changes: Any) -> LLMRequest:
    values: dict[str, Any] = {
        "model": "general-primary",
        "messages": [Message(role="user", content="hello")],
    }
    values.update(changes)
    return LLMRequest(**values)


def completion(content: str = "answer") -> ProviderCompletion:
    return ProviderCompletion(content, Usage(input_tokens=10, output_tokens=20))


def decode_events(chunks: list[str]) -> list[dict[str, Any]]:
    return [json.loads(chunk.removeprefix("data: ").strip()) for chunk in chunks]


async def test_complete_returns_provider_result_and_records_trace() -> None:
    mapper = FakeProviderMapper(completions=[completion()])
    service, traces = build_service(mapper)

    response = await service.complete(request())

    assert response.model == "general-primary"
    assert response.content == "answer"
    assert response.usage == Usage(input_tokens=10, output_tokens=20)
    assert response.attempts == 1
    assert traces.list_traces()[0].actual_model == "general-primary"
    assert traces.list_traces()[0].status == "success"


async def test_complete_retries_one_temporary_failure_on_same_model() -> None:
    mapper = FakeProviderMapper(
        completions=[RetryableProviderError("temporary"), completion()]
    )
    service, _ = build_service(mapper)

    response = await service.complete(request())

    assert response.attempts == 2
    assert mapper.complete_models == ["deepseek-v4-flash", "deepseek-v4-flash"]


async def test_complete_falls_back_after_second_temporary_failure() -> None:
    mapper = FakeProviderMapper(
        completions=[
            RetryableProviderError("one"),
            RetryableProviderError("two"),
            completion("backup answer"),
        ]
    )
    service, traces = build_service(mapper)

    response = await service.complete(request())

    assert response.model == "general-backup"
    assert response.content == "backup answer"
    assert response.attempts == 3
    assert mapper.complete_models == [
        "deepseek-v4-flash",
        "deepseek-v4-flash",
        "deepseek-chat",
    ]
    assert traces.list_traces()[0].actual_model == "general-backup"


async def test_complete_rejects_unknown_model_with_400() -> None:
    service, _ = build_service(FakeProviderMapper())

    with pytest.raises(GatewayError) as error:
        await service.complete(request(model="missing"))

    assert error.value.code == "unknown_model"
    assert error.value.status_code == 400


async def test_complete_rejects_stream_flag() -> None:
    service, _ = build_service(FakeProviderMapper())

    with pytest.raises(GatewayError) as error:
        await service.complete(request(stream=True))

    assert error.value.code == "use_stream_endpoint"
    assert error.value.status_code == 400


@pytest.mark.parametrize(
    ("content", "schema", "error_code"),
    [
        ("not json", {"type": "object"}, "invalid_json"),
        ('{"name": 1}', {"type": "object", "properties": {"name": {"type": "string"}}}, "schema_validation_failed"),
    ],
)
async def test_complete_rejects_invalid_structured_output(
    content: str,
    schema: dict[str, Any],
    error_code: str,
) -> None:
    service, _ = build_service(FakeProviderMapper(completions=[completion(content)]))

    with pytest.raises(GatewayError) as error:
        await service.complete(request(response_schema=schema))

    assert error.value.code == error_code


async def test_complete_records_model_unavailable_after_all_models_fail() -> None:
    mapper = FakeProviderMapper(
        completions=[RetryableProviderError(str(index)) for index in range(4)]
    )
    service, traces = build_service(mapper)

    with pytest.raises(GatewayError) as error:
        await service.complete(request())

    assert error.value.code == "model_unavailable"
    trace = traces.list_traces()[0]
    assert trace.status == "failed"
    assert trace.attempts == 4
    assert trace.error_code == "model_unavailable"


async def test_stream_emits_deltas_completion_and_success_trace() -> None:
    mapper = FakeProviderMapper(streams=[["hello", " world"]])
    service, traces = build_service(mapper)

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "hello"},
        {"type": "content.delta", "delta": " world"},
        {"type": "response.completed", "model": "general-primary"},
    ]
    assert traces.list_traces()[0].status == "success"


async def test_stream_falls_back_before_first_delta() -> None:
    mapper = FakeProviderMapper(
        streams=[[RetryableProviderError("primary")], ["backup"]]
    )
    service, traces = build_service(mapper)

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "backup"},
        {"type": "response.completed", "model": "general-backup"},
    ]
    assert mapper.stream_models == ["deepseek-v4-flash", "deepseek-chat"]
    assert traces.list_traces()[0].attempts == 2


async def test_stream_fails_after_first_delta_without_fallback() -> None:
    mapper = FakeProviderMapper(streams=[["partial", RetryableProviderError("lost")]])
    service, traces = build_service(mapper)

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "partial"},
        {"type": "response.failed", "error": "upstream_stream_failed"},
    ]
    assert mapper.stream_models == ["deepseek-v4-flash"]
    assert traces.list_traces()[0].status == "failed"


def test_stream_rejects_response_schema_before_iteration() -> None:
    service, _ = build_service(FakeProviderMapper())

    with pytest.raises(GatewayError) as error:
        service.stream(request(response_schema={"type": "object"}))

    assert error.value.code == "unsupported_combination"
    assert error.value.status_code == 400


def test_stream_rejects_unknown_model_before_iteration() -> None:
    service, _ = build_service(FakeProviderMapper())

    with pytest.raises(GatewayError) as error:
        service.stream(request(model="missing"))

    assert error.value.code == "unknown_model"
