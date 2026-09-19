from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.model.responses import ResponseCreateRequest, ResponseObject
from app.service.responses_service import ResponsesService


def create_responses_router(service: ResponsesService) -> APIRouter:
    router = APIRouter()

    @router.post("/v1/responses", response_model=ResponseObject)
    async def create_response(request: ResponseCreateRequest):
        if request.stream:
            return StreamingResponse(service.stream(request), media_type="text/event-stream")
        return await service.complete(request)

    return router
