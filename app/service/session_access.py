from typing import Protocol

from app.model.session import SessionAction, StreamSession


class SessionAccessPolicy(Protocol):
    def assert_allowed(
        self,
        actor_id: str | None,
        session: StreamSession,
        action: SessionAction,
    ) -> None: ...


class AllowAllSessionAccessPolicy:
    def assert_allowed(
        self,
        actor_id: str | None,
        session: StreamSession,
        action: SessionAction,
    ) -> None:
        return None

