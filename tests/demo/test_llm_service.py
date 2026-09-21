import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.core.errors import GatewayError, RetryableProviderError
from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.trace_dao import TraceDao
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.model.config import GatewayConfig, ModelRouteConfig, ProviderConfig
from app.model.dto import (
    ProviderCompletion,
    ProviderRequest,
    ProviderStreamChunk,
    ProviderStreamEvent,
)
from app.model.entity import AttemptStatus, AttemptType, JsonValidationStatus
from app.model.enums import LLMProtocolEnum, ModelEnum, ModelProviderEnum
from app.model.request import LLMRequest, Message
from app.model.response import Usage
from app.model.session import SessionInterface
from app.service.llm_service import LLMService
from app.service.prompt_service import PromptService
from app.service.protocol_factory import ProtocolFactory
from app.service.provider.base import BaseModelProvider
from app.service.provider_factory import ProviderFactory
from app.service.trace_service import TraceService


class FakeProtocolDao:
    def __init__(
        self,
        completions: list[ProviderCompletion | Exception] | None = None,
        streams: list[list[str | ProviderStreamChunk | Exception]] | None = None,
    ) -> None:
        self.completions = list(completions or [])
        self.streams = list(streams or [])
        self.complete_models: list[str] = []
        self.stream_models: list[str] = []
        self.providers: list[ProviderConfig] = []
        self.requests: list[ProviderRequest] = []

    async def complete(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> ProviderCompletion:
        self.complete_models.append(model.provider_model)
        self.providers.append(provider)
        self.requests.append(request)
        result = self.completions.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def stream(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> AsyncIterator[ProviderStreamChunk]:
        self.stream_models.append(model.provider_model)
        self.providers.append(provider)
        self.requests.append(request)
        for item in self.streams.pop(0):
            if isinstance(item, Exception):
                raise item
            yield (
                ProviderStreamChunk(delta=item)
                if isinstance(item, str)
                else item
            )


class ClosableProtocolDao(FakeProtocolDao):
    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def stream(
        self, provider, model, request
    ) -> AsyncIterator[ProviderStreamChunk]:
        try:
            yield ProviderStreamChunk(delta="partial")
            await asyncio.Event().wait()
        finally:
            self.closed = True


@pytest.mark.asyncio
async def test_closing_gateway_stream_closes_underlying_provider_stream() -> None:
    provider_dao = ClosableProtocolDao()
    service, _, _, _ = build_service(chat_dao=provider_dao)
    events = service.stream_events(request())

    assert (await anext(events)).delta == "partial"
    await events.aclose()

    assert provider_dao.closed is True


def gateway_config(
    *,
    max_retries_per_model: int = 1,
    initial_delay_seconds: float = 0,
    backoff_multiplier: float = 2,
) -> GatewayConfig:
    return GatewayConfig.model_validate(
        {
            "app": {"title": "Test Gateway", "version": "test"},
            "retry": {
                "max_retries_per_model": max_retries_per_model,
                "initial_delay_seconds": initial_delay_seconds,
                "backoff_multiplier": backoff_multiplier,
            },
            "json_parsing": {
                "retry_prompt": {
                    "active_version": "json-repair-v1",
                    "versions": {
                        "json-repair-v1": {
                            "renderer": "str-format-v1",
                            "template": (
                                "缺失参数：{missing_parameters}\n"
                                "错误参数：{invalid_parameters}\n"
                                "JSON 格式错误：{json_error}\n"
                                "JSON Schema：{schema}"
                            ),
                        }
                    },
                }
            },
            "postgres": {
                "host_env": "TEST_POSTGRES_HOST",
                "port_env": "TEST_POSTGRES_PORT",
                "dbname_env": "TEST_POSTGRES_DBNAME",
                "user_env": "TEST_POSTGRES_USER",
                "password_env": "TEST_POSTGRES_PASSWORD",
            },
            "redis": {
                "host_env": "TEST_REDIS_HOST",
                "port_env": "TEST_REDIS_PORT",
                "password_env": "TEST_REDIS_PASSWORD",
                "database_env": "TEST_REDIS_DATABASE",
            },
            "providers": {
                "deepseek": {
                    "base_url": "https://deepseek.test",
                    "api_key_env": "TEST_DEEPSEEK_API_KEY",
                    "supported_protocols": ["chat_completions"],
                },
                "openai": {
                    "base_url": "https://openai.test/v1",
                    "api_key_env": "TEST_OPENAI_API_KEY",
                    "supported_protocols": ["chat_completions", "responses"],
                },
            },
            "models": {
                "general-primary": {
                    "provider": "deepseek",
                    "provider_model": "deepseek-primary",
                    "protocol": "chat_completions",
                    "fallback": "general-backup",
                    "structured_output_mode": "json_object",
                    "capabilities": {
                        "streaming": True,
                        "structured_output": True,
                        "tools": False,
                        "multimodal": False,
                    },
                    "pricing": {"input_per_million": 1, "output_per_million": 4},
                },
                "general-backup": {
                    "provider": "deepseek",
                    "provider_model": "deepseek-backup",
                    "protocol": "chat_completions",
                    "structured_output_mode": "json_object",
                    "capabilities": {
                        "streaming": True,
                        "structured_output": True,
                        "tools": False,
                        "multimodal": False,
                    },
                    "pricing": {"input_per_million": 0.8, "output_per_million": 3.2},
                },
                "general-responses": {
                    "provider": "openai",
                    "provider_model": "gpt-responses",
                    "protocol": "responses",
                    "capabilities": {
                        "streaming": True,
                        "structured_output": True,
                        "tools": False,
                        "multimodal": False,
                    },
                    "pricing": {"input_per_million": 0.4, "output_per_million": 1.6},
                },
            },
        }
    )


def build_service(
    chat_dao: FakeProtocolDao | None = None,
    responses_dao: FakeProtocolDao | None = None,
    config: GatewayConfig | None = None,
) -> tuple[LLMService, TraceService, FakeProtocolDao, FakeProtocolDao]:
    config = config or gateway_config()
    chat_dao = chat_dao or FakeProtocolDao()
    responses_dao = responses_dao or FakeProtocolDao()
    protocol_factory = ProtocolFactory(
        {
            LLMProtocolEnum.CHAT_COMPLETIONS: chat_dao,
            LLMProtocolEnum.RESPONSES: responses_dao,
        }
    )
    provider_factory = ProviderFactory(
        {
            ModelProviderEnum.DEEPSEEK: BaseModelProvider(
                config.providers[ModelProviderEnum.DEEPSEEK], protocol_factory
            ),
            ModelProviderEnum.OPENAI: BaseModelProvider(
                config.providers[ModelProviderEnum.OPENAI], protocol_factory
            ),
        }
    )
    model_dao = ModelDao(MemoryModelMapper(config))
    trace_service = TraceService(model_dao, TraceDao(MemoryTraceMapper()))
    service = LLMService(
        gateway_config=config,
        provider_factory=provider_factory,
        prompt_service=PromptService(PromptDao(MemoryPromptMapper())),
        trace_service=trace_service,
    )
    return service, trace_service, chat_dao, responses_dao


def request(**changes: Any) -> LLMRequest:
    values: dict[str, Any] = {
        "model": ModelEnum.GENERAL_PRIMARY,
        "messages": [Message(role="user", content="hello")],
    }
    values.update(changes)
    return LLMRequest(**values)


def completion(content: str = "answer") -> ProviderCompletion:
    return ProviderCompletion(content, Usage(input_tokens=10, output_tokens=20))


def decode_events(chunks: list[str]) -> list[dict[str, Any]]:
    return [json.loads(chunk.removeprefix("data: ").strip()) for chunk in chunks]


async def test_complete_routes_deepseek_model_through_its_provider_map_entry() -> None:
    service, _, chat_dao, responses_dao = build_service(
        chat_dao=FakeProtocolDao(completions=[completion()])
    )

    response = await service.complete(request())

    assert response.model == ModelEnum.GENERAL_PRIMARY
    assert chat_dao.complete_models == ["deepseek-primary"]
    assert chat_dao.providers == [gateway_config().providers[ModelProviderEnum.DEEPSEEK]]
    assert responses_dao.complete_models == []


async def test_complete_persists_client_interface_on_logical_call() -> None:
    service, traces, _, _ = build_service(
        chat_dao=FakeProtocolDao(completions=[completion()])
    )

    response = await service.complete(
        request(),
        interface=SessionInterface.CHAT_COMPLETIONS,
    )

    call = await traces._trace_dao.get_call(response.request_id)
    assert call is not None
    assert call.interface is SessionInterface.CHAT_COMPLETIONS


async def test_complete_routes_openai_model_through_its_provider_map_entry() -> None:
    service, _, chat_dao, responses_dao = build_service(
        responses_dao=FakeProtocolDao(completions=[completion()])
    )

    response = await service.complete(request(model=ModelEnum.GENERAL_RESPONSES))

    assert response.model == ModelEnum.GENERAL_RESPONSES
    assert responses_dao.complete_models == ["gpt-responses"]
    assert responses_dao.providers == [gateway_config().providers[ModelProviderEnum.OPENAI]]
    assert chat_dao.complete_models == []


@pytest.mark.parametrize(
    ("model", "required_protocol"),
    [
        (ModelEnum.GENERAL_PRIMARY, LLMProtocolEnum.RESPONSES),
        (ModelEnum.GENERAL_RESPONSES, LLMProtocolEnum.CHAT_COMPLETIONS),
    ],
)
async def test_complete_rejects_required_protocol_mismatch_before_provider_invocation(
    model: ModelEnum,
    required_protocol: LLMProtocolEnum,
) -> None:
    service, _, chat_dao, responses_dao = build_service()

    with pytest.raises(GatewayError) as error:
        await service.complete(request(model=model), required_protocol=required_protocol)

    assert error.value.code == "protocol_mismatch"
    assert error.value.status_code == 400
    assert chat_dao.complete_models == []
    assert responses_dao.complete_models == []


async def test_complete_retries_then_uses_yaml_fallback_for_the_same_protocol() -> None:
    service, traces, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            completions=[
                RetryableProviderError("one"),
                RetryableProviderError("two"),
                completion("backup answer"),
            ]
        )
    )

    response = await service.complete(request())

    assert response.model == ModelEnum.GENERAL_BACKUP
    assert response.content == "backup answer"
    assert response.attempts == 3
    assert chat_dao.complete_models == [
        "deepseek-primary",
        "deepseek-primary",
        "deepseek-backup",
    ]
    assert (await traces.list_traces())[0].actual_model == ModelEnum.GENERAL_BACKUP
    attempts = await traces.list_attempts(response.request_id)
    assert [item.attempt_type for item in attempts] == [
        AttemptType.INITIAL,
        AttemptType.RETRY,
        AttemptType.FALLBACK,
    ]
    assert [item.status for item in attempts] == [
        AttemptStatus.FAILED,
        AttemptStatus.FAILED,
        AttemptStatus.SUCCESS,
    ]


async def test_complete_uses_configured_exponential_backoff_before_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("app.service.llm_service.asyncio.sleep", sleep)
    config = gateway_config(
        max_retries_per_model=3,
        initial_delay_seconds=1,
        backoff_multiplier=2,
    )
    service, _, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            completions=[
                RetryableProviderError(str(index)) for index in range(4)
            ]
            + [completion("backup answer")]
        ),
        config=config,
    )

    response = await service.complete(request())

    assert response.model == ModelEnum.GENERAL_BACKUP
    assert response.attempts == 5
    assert chat_dao.complete_models == ["deepseek-primary"] * 4 + [
        "deepseek-backup"
    ]
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2, 4]


async def test_complete_returns_provider_result_and_records_trace() -> None:
    service, traces, _, _ = build_service(
        chat_dao=FakeProtocolDao(completions=[completion()])
    )

    response = await service.complete(request())

    assert response.content == "answer"
    assert response.usage == Usage(input_tokens=10, output_tokens=20)
    assert response.attempts == 1
    trace = (await traces.list_traces())[0]
    assert trace.actual_model == ModelEnum.GENERAL_PRIMARY
    assert trace.status == "success"
    assert trace.cost_usd == 0.00009


async def test_complete_does_not_retry_provider_when_success_attempt_audit_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeProtocolDao(
        completions=[completion("primary answer"), completion("backup answer")]
    )
    service, traces, _, _ = build_service(
        chat_dao=provider,
        config=gateway_config(max_retries_per_model=0),
    )
    original_finish_attempt = traces.finish_attempt
    finish_attempt_calls = 0

    async def fail_first_finish_attempt(*args: Any, **kwargs: Any):
        nonlocal finish_attempt_calls
        finish_attempt_calls += 1
        if finish_attempt_calls == 1:
            raise RuntimeError("audit database unavailable")
        return await original_finish_attempt(*args, **kwargs)

    monkeypatch.setattr(traces, "finish_attempt", fail_first_finish_attempt)

    with pytest.raises(GatewayError) as captured:
        await service.complete(request())

    assert captured.value.code == "audit_persistence_failed"
    assert provider.complete_models == ["deepseek-primary"]
    trace = (await traces.list_traces())[0]
    attempts = await traces.list_attempts(trace.request_id)
    assert [item.status for item in attempts] == [AttemptStatus.RUNNING]


async def test_complete_does_not_repeat_provider_when_call_audit_finalize_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeProtocolDao(completions=[completion("answer")])
    service, traces, _, _ = build_service(chat_dao=provider)

    async def fail_finish_call(*args: Any, **kwargs: Any):
        raise RuntimeError("audit database unavailable")

    monkeypatch.setattr(traces, "finish_call", fail_finish_call)

    with pytest.raises(GatewayError) as captured:
        await service.complete(request())

    assert captured.value.code == "audit_persistence_failed"
    assert provider.complete_models == ["deepseek-primary"]
    trace = (await traces.list_traces())[0]
    assert trace.status == "running"
    attempts = await traces.list_attempts(trace.request_id)
    assert [item.status for item in attempts] == [AttemptStatus.SUCCESS]


async def test_complete_rejects_stream_flag() -> None:
    service, _, _, _ = build_service()

    with pytest.raises(GatewayError) as error:
        await service.complete(request(stream=True))

    assert error.value.code == "use_stream_endpoint"


@pytest.mark.parametrize(
    ("content", "schema", "error_code"),
    [
        ("not json", {"type": "object"}, "invalid_json"),
        (
            '{"name": 1}',
            {"type": "object", "properties": {"name": {"type": "string"}}},
            "schema_validation_failed",
        ),
    ],
)
async def test_complete_records_usage_when_structured_output_validation_fails(
    content: str,
    schema: dict[str, Any],
    error_code: str,
) -> None:
    service, traces, _, _ = build_service(
        chat_dao=FakeProtocolDao(completions=[completion(content)]),
        config=gateway_config(max_retries_per_model=0),
    )

    with pytest.raises(GatewayError) as error:
        await service.complete(request(response_schema=schema))

    assert error.value.code == error_code
    trace = (await traces.list_traces())[0]
    assert trace.status == "failed"
    assert trace.actual_model is None
    assert trace.input_tokens == 10
    assert trace.output_tokens == 20
    assert trace.error_code == error_code
    attempts = await traces.list_attempts(trace.request_id)
    assert attempts[0].status is AttemptStatus.INVALID_OUTPUT
    assert attempts[0].json_validation_status is JsonValidationStatus.INVALID


async def test_complete_retries_invalid_parameters_with_configured_prompt() -> None:
    provider = FakeProtocolDao(
        completions=[
            completion('{"age":"old"}'),
            completion('{"name":"Ada","age":37}'),
        ]
    )
    service, traces, _, _ = build_service(chat_dao=provider)
    schema = {
        "type": "object",
        "required": ["name", "age"],
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
    }

    response = await service.complete(request(response_schema=schema))

    assert response.parsed == {"name": "Ada", "age": 37}
    assert response.attempts == 2
    assert [message.role for message in provider.requests[1].messages] == [
        "user",
        "assistant",
        "user",
    ]
    retry_prompt = provider.requests[1].messages[-1].content
    assert "缺失参数：$.name" in retry_prompt
    assert "错误参数：$.age: 'old' is not of type 'integer'" in retry_prompt
    trace_list = await traces.list_traces()
    assert len(trace_list) == 1
    assert trace_list[0].status == "success"
    assert trace_list[0].input_tokens == 20
    assert trace_list[0].output_tokens == 40
    attempts = await traces.list_attempts(response.request_id)
    assert [item.attempt_type for item in attempts] == [
        AttemptType.INITIAL,
        AttemptType.JSON_RETRY,
    ]
    assert attempts[0].status is AttemptStatus.INVALID_OUTPUT
    assert attempts[0].json_validation_errors == [
        {"path": "$.name", "code": "missing"},
        {"path": "$.age", "code": "invalid"},
    ]
    assert attempts[1].prompt_name == "json-repair"
    assert attempts[1].prompt_version == "json-repair-v1"
    assert attempts[1].prompt_sha256 == hashlib.sha256(
        retry_prompt.encode("utf-8")
    ).hexdigest()


async def test_complete_accepts_scalar_json_when_schema_allows_it() -> None:
    provider = FakeProtocolDao(completions=[completion('"ready"')])
    service, _, _, _ = build_service(chat_dao=provider)

    response = await service.complete(
        request(response_schema={"type": "string"})
    )

    assert response.content == '"ready"'
    assert response.parsed == "ready"


async def test_complete_rejects_invalid_schema_before_provider_invocation() -> None:
    service, _, chat_dao, responses_dao = build_service()

    with pytest.raises(GatewayError) as captured:
        await service.complete(request(response_schema={"type": "not-a-json-type"}))

    assert captured.value.code == "invalid_response_schema"
    assert captured.value.status_code == 400
    assert chat_dao.requests == []
    assert responses_dao.requests == []


async def test_complete_retries_unrepairable_json_then_returns_invalid_json() -> None:
    provider = FakeProtocolDao(
        completions=[completion('{"name": }'), completion("still not json")]
    )
    service, traces, _, _ = build_service(chat_dao=provider)

    with pytest.raises(GatewayError) as captured:
        await service.complete(request(response_schema={"type": "object"}))

    assert captured.value.code == "invalid_json"
    assert len(provider.requests) == 2
    assert "JSON 格式错误：JSON 语法错误，无法自动修复" in (
        provider.requests[1].messages[-1].content
    )
    trace = (await traces.list_traces())[0]
    assert trace.input_tokens == 20
    assert trace.output_tokens == 40


async def test_complete_returns_bracket_repaired_json_without_retry() -> None:
    provider = FakeProtocolDao(completions=[completion('{"name":"Ada"')])
    service, traces, _, _ = build_service(chat_dao=provider)

    response = await service.complete(
        request(
            response_schema={
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            }
        )
    )

    assert response.content == '{"name":"Ada"}'
    assert response.parsed == {"name": "Ada"}
    assert response.attempts == 1
    assert len(provider.requests) == 1
    attempts = await traces.list_attempts(response.request_id)
    assert len(attempts) == 1
    assert attempts[0].json_validation_status is JsonValidationStatus.REPAIRED


async def test_complete_records_provider_business_error() -> None:
    provider_error = GatewayError("gateway_misconfigured", "Missing API key", 503)
    service, traces, _, _ = build_service(
        chat_dao=FakeProtocolDao(completions=[provider_error])
    )

    with pytest.raises(GatewayError) as error:
        await service.complete(request())

    assert error.value is provider_error
    trace = (await traces.list_traces())[0]
    assert trace.status == "failed"
    assert trace.actual_model is None
    assert trace.input_tokens == 0
    assert trace.output_tokens == 0
    assert trace.error_code == "gateway_misconfigured"


async def test_complete_records_model_unavailable_after_all_models_fail() -> None:
    service, traces, _, _ = build_service(
        chat_dao=FakeProtocolDao(
            completions=[RetryableProviderError(str(index)) for index in range(4)]
        )
    )

    with pytest.raises(GatewayError) as error:
        await service.complete(request())

    assert error.value.code == "model_unavailable"
    trace = (await traces.list_traces())[0]
    assert trace.status == "failed"
    assert trace.attempts == 4
    assert trace.error_code == "model_unavailable"


async def test_stream_events_exposes_typed_deltas_and_completion() -> None:
    service, traces, _, _ = build_service(
        chat_dao=FakeProtocolDao(streams=[["hello", " world"]])
    )

    events = [event async for event in service.stream_events(request())]

    assert [event.type for event in events] == [
        "text_delta",
        "text_delta",
        "completed",
    ]
    assert [event.delta for event in events] == ["hello", " world", None]
    assert events[0].upstream_first_delta_latency_ms is not None
    assert events[1].upstream_first_delta_latency_ms is None
    assert events[2].model is ModelEnum.GENERAL_PRIMARY
    assert (await traces.list_traces())[0].status == "success"


async def test_stream_records_terminal_provider_usage_on_attempt() -> None:
    provider = FakeProtocolDao(
        streams=[
            [
                "hello",
                ProviderStreamChunk(
                    usage=Usage(input_tokens=7, output_tokens=11)
                ),
            ]
        ]
    )
    service, traces, _, _ = build_service(chat_dao=provider)

    _ = [event async for event in service.stream_events(request())]

    trace = (await traces.list_traces())[0]
    attempts = await traces.list_attempts(trace.request_id)
    assert trace.input_tokens == 7
    assert trace.output_tokens == 11
    assert attempts[0].input_tokens == 7
    assert attempts[0].output_tokens == 11
    assert attempts[0].cost_usd > 0


async def test_stream_does_not_reclassify_provider_success_when_audit_finalize_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeProtocolDao(streams=[["answer"], ["backup"]])
    service, traces, _, _ = build_service(
        chat_dao=provider,
        config=gateway_config(max_retries_per_model=0),
    )
    original_finish_attempt = traces.finish_attempt
    finish_attempt_calls = 0

    async def fail_first_finish_attempt(*args: Any, **kwargs: Any):
        nonlocal finish_attempt_calls
        finish_attempt_calls += 1
        if finish_attempt_calls == 1:
            raise RuntimeError("audit database unavailable")
        return await original_finish_attempt(*args, **kwargs)

    monkeypatch.setattr(traces, "finish_attempt", fail_first_finish_attempt)

    with pytest.raises(GatewayError) as captured:
        _ = [event async for event in service.stream_events(request())]

    assert captured.value.code == "audit_persistence_failed"
    assert provider.stream_models == ["deepseek-primary"]
    trace = (await traces.list_traces())[0]
    attempts = await traces.list_attempts(trace.request_id)
    assert [item.status for item in attempts] == [AttemptStatus.RUNNING]


async def test_first_stream_delta_reports_latency_from_initial_upstream_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = iter([100.0, 100.0, 100.025])
    monkeypatch.setattr(
        "app.service.llm_service.time.perf_counter", lambda: next(ticks)
    )
    service, _, _, _ = build_service(
        chat_dao=FakeProtocolDao(streams=[["hello", " world"]])
    )
    events = service.stream_events(request())

    first = await anext(events)
    await events.aclose()

    assert first == ProviderStreamEvent(
        type="text_delta",
        delta="hello",
        upstream_first_delta_latency_ms=25,
    )


async def test_first_delta_latency_includes_pre_output_fallback_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = iter([100.0, 100.0, 101.0])
    monkeypatch.setattr(
        "app.service.llm_service.time.perf_counter", lambda: next(ticks)
    )
    service, _, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            streams=[
                [RetryableProviderError("primary")],
                [RetryableProviderError("primary retry")],
                ["backup"],
            ]
        )
    )
    events = service.stream_events(request())

    first = await anext(events)
    await events.aclose()

    assert first.upstream_first_delta_latency_ms == 1_000
    assert chat_dao.stream_models == [
        "deepseek-primary",
        "deepseek-primary",
        "deepseek-backup",
    ]


async def test_empty_upstream_delta_does_not_complete_first_delta_measurement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = iter([100.0, 100.0, 100.025])
    monkeypatch.setattr(
        "app.service.llm_service.time.perf_counter", lambda: next(ticks)
    )
    service, _, _, _ = build_service(
        chat_dao=FakeProtocolDao(streams=[["", "hello"]])
    )
    events = service.stream_events(request())

    first = await anext(events)
    await events.aclose()

    assert first.delta == "hello"
    assert first.upstream_first_delta_latency_ms == 25


async def test_legacy_stream_preserves_exact_sse_event_contract() -> None:
    service, _, _, _ = build_service(
        chat_dao=FakeProtocolDao(streams=[["hello", " world"]])
    )

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "hello"},
        {"type": "content.delta", "delta": " world"},
        {"type": "response.completed", "model": "general-primary"},
    ]


async def test_stream_falls_back_before_first_delta() -> None:
    service, traces, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            streams=[
                [RetryableProviderError("primary")],
                [RetryableProviderError("primary retry")],
                ["backup"],
            ]
        )
    )

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "backup"},
        {"type": "response.completed", "model": "general-backup"},
    ]
    assert chat_dao.stream_models == [
        "deepseek-primary",
        "deepseek-primary",
        "deepseek-backup",
    ]
    trace = (await traces.list_traces())[0]
    assert trace.attempts == 3
    attempts = await traces.list_attempts(trace.request_id)
    assert [item.attempt_type for item in attempts] == [
        AttemptType.INITIAL,
        AttemptType.RETRY,
        AttemptType.FALLBACK,
    ]


async def test_stream_uses_configured_exponential_backoff_before_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("app.service.llm_service.asyncio.sleep", sleep)
    config = gateway_config(
        max_retries_per_model=3,
        initial_delay_seconds=1,
        backoff_multiplier=2,
    )
    service, _, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            streams=[
                [RetryableProviderError(str(index))] for index in range(4)
            ]
            + [["backup"]]
        ),
        config=config,
    )

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "backup"},
        {"type": "response.completed", "model": "general-backup"},
    ]
    assert chat_dao.stream_models == ["deepseek-primary"] * 4 + [
        "deepseek-backup"
    ]
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2, 4]


async def test_stream_retry_reuses_resume_token_only_on_the_same_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("app.service.llm_service.asyncio.sleep", sleep)
    service, _, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            streams=[
                [
                    "partial",
                    RetryableProviderError("lost", resume_token="session-1:cursor-7"),
                ],
                [" continued"],
            ]
        )
    )

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "partial"},
        {"type": "content.delta", "delta": " continued"},
        {"type": "response.completed", "model": "general-primary"},
    ]
    assert chat_dao.stream_models == ["deepseek-primary", "deepseek-primary"]
    assert [item.stream_resume_token for item in chat_dao.requests] == [
        None,
        "session-1:cursor-7",
    ]
    sleep.assert_awaited_once_with(0)


async def test_stream_does_not_reuse_stale_resume_token_after_new_content() -> None:
    config = gateway_config(max_retries_per_model=2)
    service, _, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            streams=[
                [
                    "partial",
                    RetryableProviderError("lost", resume_token="session-1:cursor-7"),
                ],
                [" continued", RetryableProviderError("lost again")],
                ["replayed content"],
            ]
        ),
        config=config,
    )

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "partial"},
        {"type": "content.delta", "delta": " continued"},
        {"type": "response.failed", "error": "upstream_stream_failed"},
    ]
    assert chat_dao.stream_models == ["deepseek-primary", "deepseek-primary"]
    assert [item.stream_resume_token for item in chat_dao.requests] == [
        None,
        "session-1:cursor-7",
    ]


async def test_stream_fallback_starts_with_a_clean_provider_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("app.service.llm_service.asyncio.sleep", sleep)
    service, _, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            streams=[
                [RetryableProviderError("one", resume_token="primary-session")],
                [RetryableProviderError("two", resume_token="primary-session")],
                ["backup"],
            ]
        )
    )

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events[-1] == {
        "type": "response.completed",
        "model": "general-backup",
    }
    assert chat_dao.stream_models == [
        "deepseek-primary",
        "deepseek-primary",
        "deepseek-backup",
    ]
    assert [item.stream_resume_token for item in chat_dao.requests] == [
        None,
        "primary-session",
        None,
    ]


async def test_stream_fails_after_first_delta_without_fallback() -> None:
    service, traces, chat_dao, _ = build_service(
        chat_dao=FakeProtocolDao(
            streams=[["partial", RetryableProviderError("lost")]]
        )
    )

    events = decode_events([chunk async for chunk in service.stream(request())])

    assert events == [
        {"type": "content.delta", "delta": "partial"},
        {"type": "response.failed", "error": "upstream_stream_failed"},
    ]
    assert chat_dao.stream_models == ["deepseek-primary"]
    assert (await traces.list_traces())[0].status == "failed"


def test_stream_events_rejects_protocol_mismatch_before_iteration() -> None:
    service, _, chat_dao, responses_dao = build_service()

    with pytest.raises(GatewayError) as error:
        service.stream_events(
            request(), required_protocol=LLMProtocolEnum.RESPONSES
        )

    assert error.value.code == "protocol_mismatch"
    assert chat_dao.stream_models == []
    assert responses_dao.stream_models == []


def test_stream_rejects_response_schema_before_iteration() -> None:
    service, _, _, _ = build_service()

    with pytest.raises(GatewayError) as error:
        service.stream(request(response_schema={"type": "object"}))

    assert error.value.code == "unsupported_combination"
    assert error.value.status_code == 400
