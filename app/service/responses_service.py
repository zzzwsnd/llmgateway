import time
from collections.abc import AsyncIterator
from uuid import uuid4

from app.core.utils import encode_sse
from app.model.enums import LLMProtocolEnum, ModelEnum
from app.model.request import LLMRequest, Message
from app.model.response import Usage
from app.model.responses import (
    ResponseCompletedEvent,
    ResponseContentPartAddedEvent,
    ResponseContentPartDoneEvent,
    ResponseCreateRequest,
    ResponseCreatedEvent,
    ResponseError,
    ResponseFailedEvent,
    ResponseObject,
    ResponseOutputMessage,
    ResponseOutputItemAddedEvent,
    ResponseOutputItemDoneEvent,
    ResponseOutputText,
    ResponseOutputTextDeltaEvent,
    ResponseOutputTextDoneEvent,
    ResponseUsage,
)
from app.service.llm_service import LLMService


class ResponsesService:
    def __init__(self, llm_service: LLMService) -> None:
        self._llm_service = llm_service

    async def complete(self, request: ResponseCreateRequest) -> ResponseObject:
        response = await self._llm_service.complete(
            self.to_llm_request(request), required_protocol=LLMProtocolEnum.RESPONSES
        )
        return self._response_object(
            response.model,
            response.content,
            response.usage,
        )

    def stream(self, request: ResponseCreateRequest) -> AsyncIterator[str]:
        events = self._llm_service.stream_events(
            self.to_llm_request(request), required_protocol=LLMProtocolEnum.RESPONSES
        )
        return self._stream(events, request.model)

    @staticmethod
    def to_llm_request(request: ResponseCreateRequest) -> LLMRequest:
        messages = (
            [Message(role="user", content=request.input)]
            if isinstance(request.input, str)
            else [Message(role=message.role, content=message.content) for message in request.input]
        )
        return LLMRequest(
            model=request.model,
            messages=messages,
            stream=request.stream,
            response_schema=request.response_schema,
            response_schema_name=request.response_schema_name,
            structured_output_format=(
                request.text.format.type if request.text is not None else None
            ),
            temperature=request.temperature,
            max_output_tokens=request.max_output_tokens,
        )

    @staticmethod
    def _response_object(model: ModelEnum, content: str, usage: Usage | None) -> ResponseObject:
        response_id = f"resp_{uuid4().hex}"
        return ResponseObject(
            id=response_id,
            created_at=int(time.time()),
            status="completed",
            model=model,
            output=[
                ResponseOutputMessage(
                    id=f"msg_{uuid4().hex}",
                    content=[ResponseOutputText(text=content)],
                )
            ],
            usage=(
                ResponseUsage(
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    total_tokens=usage.input_tokens + usage.output_tokens,
                )
                if usage is not None
                else None
            ),
        )

    async def _stream(
        self, events: AsyncIterator, requested_model: ModelEnum
    ) -> AsyncIterator[str]:
        response_id = f"resp_{uuid4().hex}"
        message_id = f"msg_{uuid4().hex}"
        created_at = int(time.time())
        sequence_number = 0
        yield encode_sse(
            ResponseCreatedEvent(
                sequence_number=sequence_number,
                response=ResponseObject(
                    id=response_id,
                    created_at=created_at,
                    status="in_progress",
                    model=requested_model,
                    output=[],
                ),
            ).model_dump(mode="json", exclude_none=True)
        )
        sequence_number += 1
        yield encode_sse(
            ResponseOutputItemAddedEvent(
                sequence_number=sequence_number,
                item=ResponseOutputMessage(
                    id=message_id,
                    status="in_progress",
                    content=[],
                ),
            ).model_dump(mode="json", exclude_none=True)
        )
        sequence_number += 1
        yield encode_sse(
            ResponseContentPartAddedEvent(
                sequence_number=sequence_number,
                item_id=message_id,
                part=ResponseOutputText(text=""),
            ).model_dump(mode="json", exclude_none=True)
        )
        content = ""
        async for event in events:
            if event.type == "text_delta":
                sequence_number += 1
                delta = event.delta or ""
                content += delta
                yield encode_sse(
                    ResponseOutputTextDeltaEvent(
                        sequence_number=sequence_number,
                        item_id=message_id,
                        delta=delta,
                    ).model_dump(mode="json")
                )
            elif event.type == "completed":
                sequence_number += 1
                yield encode_sse(
                    ResponseOutputTextDoneEvent(
                        sequence_number=sequence_number,
                        item_id=message_id,
                        text=content,
                    ).model_dump(mode="json")
                )
                sequence_number += 1
                output_text = ResponseOutputText(text=content)
                yield encode_sse(
                    ResponseContentPartDoneEvent(
                        sequence_number=sequence_number,
                        item_id=message_id,
                        part=output_text,
                    ).model_dump(mode="json", exclude_none=True)
                )
                sequence_number += 1
                output_message = ResponseOutputMessage(
                    id=message_id,
                    content=[output_text],
                )
                yield encode_sse(
                    ResponseOutputItemDoneEvent(
                        sequence_number=sequence_number,
                        item=output_message,
                    ).model_dump(mode="json", exclude_none=True)
                )
                sequence_number += 1
                completed = ResponseObject(
                    id=response_id,
                    created_at=created_at,
                    status="completed",
                    model=event.model or requested_model,
                    output=[
                        output_message
                    ],
                )
                yield encode_sse(
                    ResponseCompletedEvent(
                        sequence_number=sequence_number, response=completed
                    ).model_dump(mode="json", exclude_none=True)
                )
                return
            else:
                sequence_number += 1
                failed = ResponseObject(
                    id=response_id,
                    created_at=created_at,
                    status="failed",
                    model=requested_model,
                    output=[],
                    error=ResponseError(message="The stream could not be completed"),
                )
                yield encode_sse(
                    ResponseFailedEvent(
                        sequence_number=sequence_number, response=failed
                    ).model_dump(mode="json", exclude_none=True)
                )
                return
