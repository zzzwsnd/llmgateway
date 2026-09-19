from typing import Protocol

from app.model.session import StreamSession


class CompensationService(Protocol):
    async def compensate(self, session: StreamSession) -> None: ...


class NoOpCompensationService:
    async def compensate(self, session: StreamSession) -> None:
        return None

