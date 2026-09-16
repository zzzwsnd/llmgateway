from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.model.request import LLMRequest
from app.model.response import LLMResponse
from app.service.llm_service import LLMService


def create_llm_router(service: LLMService) -> APIRouter:
    router = APIRouter()

    @router.post("/v1/llm", response_model=LLMResponse)
    async def create_llm_response(request: LLMRequest) -> LLMResponse:
        return await service.complete(request)

    @router.post("/v1/llm/stream")
    async def create_stream(request: LLMRequest) -> StreamingResponse:
        return StreamingResponse(
            service.stream(request),
            media_type="text/event-stream",
        )

    return router
