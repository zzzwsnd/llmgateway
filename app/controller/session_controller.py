from typing import Annotated

from fastapi import APIRouter, Header, Query, Response, status
from fastapi.responses import StreamingResponse

from app.model.session import (
    CreateStreamSessionRequest,
    CreateStreamSessionResponse,
    StreamSessionResponse,
)
from app.service.stream_session_service import StreamSessionService


def create_session_router(service: StreamSessionService) -> APIRouter:
    router = APIRouter(prefix="/v1/stream-sessions", tags=["stream-sessions"])

    @router.post(
        "",
        response_model=CreateStreamSessionResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_session(
        body: CreateStreamSessionRequest,
        idempotency_key: Annotated[
            str | None, Header(alias="Idempotency-Key", max_length=200)
        ] = None,
    ) -> CreateStreamSessionResponse:
        return await service.create(body, idempotency_key=idempotency_key)

    @router.get("/{session_id}", response_model=StreamSessionResponse)
    async def get_session(session_id: str) -> StreamSessionResponse:
        return await service.get(session_id)

    @router.get("/{session_id}/events")
    async def subscribe(
        session_id: str,
        last_event_id: Annotated[
            str | None, Header(alias="Last-Event-ID", max_length=100)
        ] = None,
        after: Annotated[str | None, Query(max_length=100)] = None,
    ) -> StreamingResponse:
        cursor = last_event_id if last_event_id is not None else after
        events = await service.open_stream(session_id, after=cursor)
        return StreamingResponse(
            events,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @router.delete(
        "/{session_id}/connections/{connection_id}",
        status_code=status.HTTP_204_NO_CONTENT,
    )
    async def detach(session_id: str, connection_id: str) -> Response:
        await service.detach(session_id, connection_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @router.post("/{session_id}/cancel", response_model=StreamSessionResponse)
    async def cancel(session_id: str) -> StreamSessionResponse:
        return await service.cancel(session_id)

    return router
