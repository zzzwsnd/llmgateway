from datetime import datetime, timezone

from app.core.config import load_gateway_config
from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.trace_dao import TraceDao
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.model.enums import ModelEnum
from app.model.entity import CallTrace


def test_model_mapper_exposes_models_from_gateway_config() -> None:
    dao = ModelDao(MemoryModelMapper(load_gateway_config()))

    primary = dao.get_config(ModelEnum.GENERAL_PRIMARY)
    backup = dao.get_config(ModelEnum.GENERAL_BACKUP)

    assert primary is not None
    assert primary.api_key_env == "DEEPSEEK_API_KEY"
    assert backup is not None
    assert backup.api_key_env == "DEEPSEEK_API_KEY"


def test_default_prompt_mapper_returns_versioned_template() -> None:
    template = PromptDao(MemoryPromptMapper()).get("knowledge_decision", "v1")

    assert template is not None
    assert "${product_name}" in template.system_template


def test_trace_mapper_returns_a_snapshot() -> None:
    dao = TraceDao(MemoryTraceMapper())
    trace = CallTrace(
        request_id="request-1",
        timestamp=datetime.now(timezone.utc),
        requested_model="general-primary",
        input_tokens=1,
        output_tokens=2,
        cost_usd=0.0,
        latency_ms=3,
        attempts=1,
        status="success",
    )
    dao.save(trace)

    snapshot = dao.list_all()
    snapshot.clear()

    assert dao.list_all() == [trace]
