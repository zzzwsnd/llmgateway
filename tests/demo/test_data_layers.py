from datetime import datetime, timezone

import pytest

from app.core.config import load_gateway_config
from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.trace_dao import TraceDao
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.mapper.call_schema import CALL_SCHEMA_STATEMENTS
from app.model.enums import ModelEnum
from app.model.entity import (
    AttemptStatus,
    AttemptType,
    CallAttempt,
    CallRecord,
    JsonValidationStatus,
)


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


def test_call_attempt_schema_persists_only_prompt_identity_and_sha256() -> None:
    ddl = "\n".join(CALL_SCHEMA_STATEMENTS)

    assert "prompt_sha256 TEXT" in ddl
    assert "ADD COLUMN IF NOT EXISTS prompt_sha256" in ddl
    assert "prompt_body" not in ddl
    assert "prompt_template" not in ddl


@pytest.mark.asyncio
async def test_trace_mapper_returns_a_snapshot() -> None:
    dao = TraceDao(MemoryTraceMapper())
    now = datetime.now(timezone.utc)
    call = CallRecord(
        call_id="request-1",
        requested_model="general-primary",
        status="success",
        created_at=now,
        updated_at=now,
        started_at=now,
        finished_at=now,
    )
    await dao.create_call(call)

    snapshot = await dao.list_all()
    snapshot.clear()

    stored = await dao.list_all()
    assert len(stored) == 1
    assert stored[0].request_id == call.call_id


@pytest.mark.asyncio
async def test_trace_mapper_rejects_attempt_until_call_is_running() -> None:
    mapper = MemoryTraceMapper()
    now = datetime.now(timezone.utc)
    await mapper.insert_call(
        CallRecord(
            call_id="call-pending",
            requested_model="general-primary",
            status="pending",
            created_at=now,
            updated_at=now,
        )
    )

    with pytest.raises(RuntimeError, match="call is running"):
        await mapper.insert_attempt(
            CallAttempt(
                attempt_id="attempt-1",
                call_id="call-pending",
                attempt_no=0,
                attempt_type=AttemptType.INITIAL,
                model="general-primary",
                status=AttemptStatus.RUNNING,
                json_validation_status=JsonValidationStatus.NOT_REQUESTED,
                started_at=now,
            )
        )
