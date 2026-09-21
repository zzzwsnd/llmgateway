import time
from collections.abc import AsyncIterator
from uuid import uuid4

from app.core.utils import encode_sse
from app.model.chat_completions import (
    ChatCompletionAssistantMessage,
    ChatCompletionChoice,
    ChatCompletionChunk,
    ChatCompletionChunkChoice,
    ChatCompletionDelta,
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionUsage,
    ChatStreamError,
    ChatStreamErrorDetail,
)
from app.model.enums import LLMProtocolEnum
from app.model.request import LLMRequest
from app.model.session import SessionInterface
from app.service.llm_service import LLMService


class ChatCompletionsService:
    def __init__(self, llm_service: LLMService) -> None:
        self._llm_service = llm_service

    async def complete(self, request: ChatCompletionRequest) -> ChatCompletionResponse:
        response = await self._llm_service.complete(
            self.to_llm_request(request),
            required_protocol=LLMProtocolEnum.CHAT_COMPLETIONS,
            interface=SessionInterface.CHAT_COMPLETIONS,
        )
        return ChatCompletionResponse(
            id=f"chatcmpl-{uuid4().hex}",
            created=int(time.time()),
            model=response.model,
            choices=[
                ChatCompletionChoice(
                    message=ChatCompletionAssistantMessage(content=response.content)
                )
            ],
            usage=ChatCompletionUsage(
                prompt_tokens=response.usage.input_tokens,
                completion_tokens=response.usage.output_tokens,
                total_tokens=response.usage.input_tokens + response.usage.output_tokens,
            ),
        )

    def stream(self, request: ChatCompletionRequest) -> AsyncIterator[str]:
        events = self._llm_service.stream_events(
            self.to_llm_request(request),
            required_protocol=LLMProtocolEnum.CHAT_COMPLETIONS,
            interface=SessionInterface.CHAT_COMPLETIONS,
        )
        return self._stream(events, request.model)

    @staticmethod
    def to_llm_request(request: ChatCompletionRequest) -> LLMRequest:
        return LLMRequest(
            model=request.model,
            messages=request.messages,
            stream=request.stream,
            response_schema=request.response_schema,
            response_schema_name=request.response_schema_name,
            structured_output_format=(
                request.response_format.type if request.response_format is not None else None
            ),
            temperature=request.temperature,
            max_output_tokens=request.max_tokens,
        )

    async def _stream(self, events: AsyncIterator, model: object) -> AsyncIterator[str]:
        completion_id = f"chatcmpl-{uuid4().hex}"
        created = int(time.time())
        role_chunk = ChatCompletionChunk(
            id=completion_id,
            created=created,
            model=model,
            choices=[
                ChatCompletionChunkChoice(
                    delta=ChatCompletionDelta(role="assistant")
                )
            ],
        ).model_dump(mode="json", exclude_none=True)
        role_chunk["choices"][0]["finish_reason"] = None
        yield encode_sse(role_chunk)
        async for event in events:
            if event.type == "text_delta":
                chunk = ChatCompletionChunk(
                    id=completion_id,
                    created=created,
                    model=model,
                    choices=[
                        ChatCompletionChunkChoice(
                            delta=ChatCompletionDelta(content=event.delta)
                        )
                    ],
                ).model_dump(mode="json", exclude_none=True)
                chunk["choices"][0]["finish_reason"] = None
                yield encode_sse(chunk)
            elif event.type == "completed":
                yield encode_sse(
                    ChatCompletionChunk(
                        id=completion_id,
                        created=created,
                        model=event.model or model,
                        choices=[ChatCompletionChunkChoice(delta=ChatCompletionDelta(), finish_reason="stop")],
                    ).model_dump(mode="json", exclude_none=True)
                )
                yield "data: [DONE]\n\n"
                return
            else:
                yield encode_sse(
                    ChatStreamError(
                        error=ChatStreamErrorDetail(
                            message="The stream could not be completed"
                        )
                    ).model_dump(mode="json")
                )
                yield "data: [DONE]\n\n"
                return
