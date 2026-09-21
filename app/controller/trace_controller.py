from fastapi import APIRouter

from app.model.entity import CallTrace
from app.service.trace_service import TraceService


def create_trace_router(service: TraceService) -> APIRouter:
    router = APIRouter()

    @router.get("/v1/traces", response_model=list[CallTrace])
    async def list_traces() -> list[CallTrace]:
        return await service.list_traces()

    return router
