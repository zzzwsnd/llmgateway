from datetime import datetime, timezone

import pytest

from app.mapper.call_schema import CALL_SCHEMA_STATEMENTS
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.mapper.postgres_trace_mapper import PostgresTraceMapper
from app.model.entity import (
    AttemptStatus,
    AttemptType,
    CallAttempt,
    CallRecord,
    CallStatus,
    JsonValidationStatus,
)


def _call(*, status: CallStatus = CallStatus.RUNNING) -> CallRecord:
    now = datetime.now(timezone.utc)
    return CallRecord(
        call_id="call-1",
        requested_model="general-primary",
        status=status,
        created_at=now,
        updated_at=now,
        started_at=now,
    )


def _attempt(*, attempt_id: str, attempt_type: AttemptType) -> CallAttempt:
    return CallAttempt(
        attempt_id=attempt_id,
        call_id="call-1",
        attempt_no=0,
        attempt_type=attempt_type,
        model="general-primary",
        status=AttemptStatus.RUNNING,
        json_validation_status=JsonValidationStatus.NOT_REQUESTED,
        started_at=datetime.now(timezone.utc),
    )


def test_postgres_trace_mapper_targets_only_two_audit_tables() -> None:
    ddl = "\n".join(CALL_SCHEMA_STATEMENTS)

    assert PostgresTraceMapper.call_table == "calls"
    assert PostgresTraceMapper.attempt_table == "call_attempts"
    assert "CREATE TABLE IF NOT EXISTS calls" in ddl
    assert "CREATE TABLE IF NOT EXISTS call_attempts" in ddl
    assert "CREATE TABLE IF NOT EXISTS gateway_call_events" not in ddl
    assert "CREATE TABLE IF NOT EXISTS gateway_call_results" not in ddl


@pytest.mark.asyncio
async def test_memory_mapper_assigns_attempt_numbers_within_a_call() -> None:
    mapper = MemoryTraceMapper()
    await mapper.insert_call(_call())

    first = await mapper.insert_attempt(
        _attempt(attempt_id="attempt-1", attempt_type=AttemptType.INITIAL)
    )
    second = await mapper.insert_attempt(
        _attempt(attempt_id="attempt-2", attempt_type=AttemptType.RETRY)
    )

    assert first.attempt_no == 1
    assert second.attempt_no == 2


@pytest.mark.asyncio
async def test_memory_mapper_rejects_terminal_call_overwrite() -> None:
    mapper = MemoryTraceMapper()
    current = _call(status=CallStatus.SUCCESS)
    await mapper.insert_call(current)

    with pytest.raises(RuntimeError, match="terminal call"):
        await mapper.update_call(
            current.model_copy(
                update={
                    "status": CallStatus.FAILED,
                    "updated_at": datetime.now(timezone.utc),
                }
            )
        )


@pytest.mark.asyncio
async def test_memory_mapper_projects_trace_from_attempt_aggregates() -> None:
    mapper = MemoryTraceMapper()
    current = _call()
    await mapper.insert_call(current)
    running = await mapper.insert_attempt(
        _attempt(attempt_id="attempt-1", attempt_type=AttemptType.INITIAL)
    )
    await mapper.update_attempt(
        running.model_copy(
            update={
                "status": AttemptStatus.SUCCESS,
                "input_tokens": 7,
                "output_tokens": 11,
                "cost_usd": 0.25,
                "latency_ms": 40,
                "finished_at": datetime.now(timezone.utc),
            }
        )
    )
    await mapper.update_call(
        current.model_copy(
            update={
                "status": CallStatus.SUCCESS,
                "updated_at": datetime.now(timezone.utc),
                "finished_at": datetime.now(timezone.utc),
            }
        )
    )

    trace = await mapper.find_trace("call-1")

    assert trace is not None
    assert trace.actual_model == "general-primary"
    assert trace.input_tokens == 7
    assert trace.output_tokens == 11
    assert trace.cost_usd == 0.25
    assert trace.attempts == 1
