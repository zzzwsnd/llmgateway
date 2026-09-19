from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.model.chat_completions import ChatCompletionRequest, ChatCompletionResponse
from app.service.chat_completions_service import ChatCompletionsService


def create_chat_completions_router(service: ChatCompletionsService) -> APIRouter:
    router = APIRouter()

    @router.post("/v1/chat/completions", response_model=ChatCompletionResponse)
    async def create_chat_completion(request: ChatCompletionRequest):
        if request.stream:
            return StreamingResponse(service.stream(request), media_type="text/event-stream")
        return await service.complete(request)

    return router
