from datetime import datetime, timezone

from app.mapper.runtime_mapper import RuntimeMapper
from app.model.session import RuntimeEvent, RuntimeEventType


class RuntimeDao:
    def __init__(self, mapper: RuntimeMapper) -> None:
        self._mapper = mapper

    async def close(self) -> None:
        await self._mapper.close()

    async def append_event(self, session_id: str, event: RuntimeEvent) -> RuntimeEvent:
        return await self._mapper.append_event(session_id, event)

    async def append_control(
        self,
        session_id: str,
        *,
        event_type: RuntimeEventType,
        generation_id: str,
        connection_id: str | None = None,
    ) -> RuntimeEvent:
        return await self._mapper.append_event(
            session_id,
            RuntimeEvent(
                type=event_type,
                generation_id=generation_id,
                connection_id=connection_id,
                created_at=datetime.now(timezone.utc),
            ),
        )

    async def read_events(
        self,
        session_id: str,
        *,
        after: str | None,
        block_milliseconds: int = 0,
        count: int = 100,
    ) -> list[RuntimeEvent]:
        return await self._mapper.read_events(
            session_id,
            after=after,
            block_milliseconds=block_milliseconds,
            count=count,
        )

    async def has_events(self, session_id: str) -> bool:
        return await self._mapper.has_events(session_id)

    async def latest_event_id(self, session_id: str) -> str | None:
        return await self._mapper.latest_event_id(session_id)

    async def expire_events(self, session_id: str, ttl_seconds: int) -> None:
        await self._mapper.expire_events(session_id, ttl_seconds)
