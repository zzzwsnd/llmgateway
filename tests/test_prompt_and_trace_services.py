import pytest

from app.core.errors import GatewayError
from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.trace_dao import TraceDao
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.model.request import LLMRequest, Message, PromptSelection
from app.model.response import Usage
from app.service.prompt_service import PromptService
from app.service.trace_service import TraceService


def build_trace_service() -> TraceService:
    return TraceService(
        model_dao=ModelDao(MemoryModelMapper()),
        trace_dao=TraceDao(MemoryTraceMapper()),
    )


def test_prompt_service_renders_selected_template() -> None:
    service = PromptService(PromptDao(MemoryPromptMapper()))

    message = service.render(
        PromptSelection(
            name="knowledge_decision",
            version="v1",
            variables={"product_name": "Portal"},
        )
    )

    assert message.role == "system"
    assert message.content.startswith("你是Portal的知识库决策器")


def test_prompt_service_rejects_unknown_template() -> None:
    service = PromptService(PromptDao(MemoryPromptMapper()))

    with pytest.raises(GatewayError) as error:
        service.render(PromptSelection(name="missing", version="v1"))

    assert error.value.code == "unknown_prompt_template"
    assert error.value.status_code == 400


def test_prompt_service_rejects_missing_variable() -> None:
    service = PromptService(PromptDao(MemoryPromptMapper()))

    with pytest.raises(GatewayError) as error:
        service.render(PromptSelection(name="knowledge_decision", version="v1"))

    assert error.value.code == "missing_prompt_variable"
    assert error.value.message == "缺少 Prompt 变量: product_name"


def test_prompt_service_prepends_system_message_without_mutating_request() -> None:
    service = PromptService(PromptDao(MemoryPromptMapper()))
    original_message = Message(role="user", content="question")
    request = LLMRequest(
        model="general-primary",
        messages=[original_message],
        prompt=PromptSelection(
            name="knowledge_decision",
            version="v1",
            variables={"product_name": "Portal"},
        ),
    )

    messages = service.build_messages(request)

    assert [message.role for message in messages] == ["system", "user"]
    assert request.messages == [original_message]


def test_trace_service_calculates_cost_and_persists_trace() -> None:
    service = build_trace_service()

    trace = service.record(
        request_id="request-1",
        requested_model="general-primary",
        actual_model="general-primary",
        prompt=None,
        usage=Usage(input_tokens=1_000_000, output_tokens=1_000_000),
        latency_ms=10,
        attempts=1,
        status="success",
    )

    assert trace.cost_usd == 5.0
    assert trace.timestamp.tzinfo is not None
    assert service.list_traces() == [trace]
