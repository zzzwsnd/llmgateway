from datetime import datetime

from app.mapper.session_mapper import SessionMapper
from app.model.session import SessionStatus, StreamSession


class SessionDao:
    def __init__(self, mapper: SessionMapper) -> None:
        self._mapper = mapper

    async def heartbeat(self, session_id: str, *, generation_id: str) -> bool:
        return await self._mapper.heartbeat(session_id, generation_id=generation_id)

    async def record_metrics(
        self,
        session_id: str,
        *,
        generation_id: str,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
    ) -> bool:
        return await self._mapper.record_metrics(
            session_id,
            generation_id=generation_id,
            upstream_first_delta_latency_ms=upstream_first_delta_latency_ms,
            first_content_latency_ms=first_content_latency_ms,
        )

    async def insert_or_get(
        self, session: StreamSession
    ) -> tuple[StreamSession, bool]:
        return await self._mapper.insert_or_get(session)

    async def get(self, session_id: str) -> StreamSession | None:
        return await self._mapper.get(session_id)

    async def get_by_idempotency(self, key: str) -> StreamSession | None:
        return await self._mapper.get_by_idempotency(key)

    async def transition(
        self,
        session_id: str,
        *,
        generation_id: str,
        expected_statuses: set[SessionStatus],
        expected_version: int,
        target_status: SessionStatus,
        result_text: str | None = None,
        error_code: str | None = None,
        replay_degraded: bool | None = None,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
        total_duration_ms: int | None = None,
    ) -> StreamSession | None:
        return await self._mapper.transition(
            session_id,
            generation_id=generation_id,
            expected_statuses=expected_statuses,
            expected_version=expected_version,
            target_status=target_status,
            result_text=result_text,
            error_code=error_code,
            replay_degraded=replay_degraded,
            upstream_first_delta_latency_ms=upstream_first_delta_latency_ms,
            first_content_latency_ms=first_content_latency_ms,
            total_duration_ms=total_duration_ms,
        )

    async def list_reconcilable(
        self, updated_before: datetime, limit: int = 100
    ) -> list[StreamSession]:
        return await self._mapper.list_reconcilable(updated_before, limit)
