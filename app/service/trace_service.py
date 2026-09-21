from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from app.core.logging import logger
from app.dao.model_dao import ModelDao
from app.dao.trace_dao import TraceDao
from app.model.entity import (
    AttemptStatus,
    AttemptType,
    CallAttempt,
    CallRecord,
    CallStatus,
    CallTrace,
    JsonValidationStatus,
)
from app.model.enums import ModelEnum
from app.model.request import PromptSelection
from app.model.response import Usage
from app.model.session import SessionInterface


class TraceService:
    def __init__(self, model_dao: ModelDao, trace_dao: TraceDao) -> None:
        self._model_dao = model_dao
        self._trace_dao = trace_dao

    async def start_call(
        self,
        call_id: str,
        requested_model: ModelEnum,
        prompt: PromptSelection | None,
        *,
        stream: bool = False,
        retry_of_call_id: str | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
        interface: SessionInterface = SessionInterface.LLM,
        owner_id: str | None = None,
        status: CallStatus = CallStatus.RUNNING,
    ) -> CallRecord:
        now = datetime.now(timezone.utc)
        call = CallRecord(
            call_id=call_id,
            retry_of_call_id=retry_of_call_id,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            interface=interface,
            stream=stream,
            owner_id=owner_id,
            requested_model=requested_model,
            prompt_name=prompt.name if prompt else None,
            prompt_version=prompt.version if prompt else None,
            status=status,
            created_at=now,
            updated_at=now,
            started_at=now if status is CallStatus.RUNNING else None,
        )
        return await self._trace_dao.create_call(call)

    async def start_attempt(
        self,
        call_id: str,
        model: ModelEnum,
        prompt: PromptSelection | None,
        attempt_type: AttemptType,
        *,
        json_requested: bool,
        prompt_sha256: str | None = None,
    ) -> CallAttempt:
        attempt = CallAttempt(
            attempt_id=str(uuid4()),
            call_id=call_id,
            attempt_no=0,
            attempt_type=attempt_type,
            model=model,
            prompt_name=prompt.name if prompt else None,
            prompt_version=prompt.version if prompt else None,
            prompt_sha256=prompt_sha256,
            status=AttemptStatus.RUNNING,
            json_validation_status=JsonValidationStatus.NOT_REQUESTED,
            started_at=datetime.now(timezone.utc),
        )
        return await self._trace_dao.create_attempt(attempt)

    async def finish_attempt(
        self,
        attempt: CallAttempt,
        *,
        status: AttemptStatus,
        usage: Usage,
        latency_ms: int,
        first_delta_latency_ms: int | None = None,
        error_code: str | None = None,
        json_validation_status: JsonValidationStatus | None = None,
        json_validation_errors: list[dict[str, str]] | None = None,
    ) -> CallAttempt:
        completed = attempt.model_copy(
            update={
                "status": status,
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cost_usd": self._calculate_cost(attempt.model, usage),
                "latency_ms": latency_ms,
                "first_delta_latency_ms": first_delta_latency_ms,
                "error_code": error_code,
                "json_validation_status": (
                    json_validation_status or attempt.json_validation_status
                ),
                "json_validation_errors": json_validation_errors or [],
                "finished_at": datetime.now(timezone.utc),
            }
        )
        return await self._trace_dao.save_attempt(completed)

    async def finish_call(
        self,
        call_id: str,
        *,
        status: Literal["success", "failed", "cancelled"],
        error_code: str | None = None,
        replay_degraded: bool | None = None,
    ) -> CallTrace:
        current = await self._trace_dao.get_call(call_id)
        if current is None:
            raise RuntimeError("Call audit record does not exist")
        now = datetime.now(timezone.utc)
        changes: dict[str, object] = {
            "status": CallStatus(status),
            "error_code": error_code,
            "updated_at": now,
            "finished_at": now,
        }
        if replay_degraded is not None:
            changes["replay_degraded"] = replay_degraded
        await self._trace_dao.save_call(current.model_copy(update=changes))
        trace = await self._trace_dao.get_trace(call_id)
        if trace is None:
            raise RuntimeError("Call audit record does not exist")
        logger.info("llm_call_trace=%s", trace.model_dump_json())
        return trace

    async def list_traces(self) -> list[CallTrace]:
        return await self._trace_dao.list_all()

    async def list_attempts(self, call_id: str) -> list[CallAttempt]:
        return await self._trace_dao.list_attempts(call_id)

    def _calculate_cost(self, model: ModelEnum, usage: Usage) -> float:
        price = self._model_dao.get_price(model)
        if price is None:
            raise KeyError(model)
        return (
            usage.input_tokens * price.input_per_million
            + usage.output_tokens * price.output_per_million
        ) / 1_000_000
