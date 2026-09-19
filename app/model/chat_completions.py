from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.model.enums import ModelEnum
from app.model.request import Message


class ChatResponseFormatJsonSchemaConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    schema_: dict[str, Any] = Field(alias="schema")
    strict: Literal[True] = True


class ChatResponseFormatJsonSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["json_schema"]
    json_schema: ChatResponseFormatJsonSchemaConfig


class ChatResponseFormatJsonObject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["json_object"]


ChatResponseFormat = Annotated[
    ChatResponseFormatJsonSchema | ChatResponseFormatJsonObject,
    Field(discriminator="type"),
]


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: ModelEnum
    messages: list[Message] = Field(min_length=1, max_length=100)
    stream: bool = False
    temperature: float | None = Field(default=None, ge=0, le=2)
    max_tokens: int | None = Field(default=None, gt=0)
    response_format: ChatResponseFormat | None = None

    @model_validator(mode="after")
    def check_supported_combination(self) -> "ChatCompletionRequest":
        if self.stream and self.response_format is not None:
            raise ValueError("Structured output is not supported with streaming")
        return self

    @property
    def response_schema(self) -> dict[str, Any] | None:
        if isinstance(self.response_format, ChatResponseFormatJsonSchema):
            return self.response_format.json_schema.schema_
        if isinstance(self.response_format, ChatResponseFormatJsonObject):
            return {"type": "object"}
        return None

    @property
    def response_schema_name(self) -> str | None:
        if isinstance(self.response_format, ChatResponseFormatJsonSchema):
            return self.response_format.json_schema.name
        return None


class ChatCompletionUsage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)


class ChatCompletionAssistantMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: Literal["assistant"] = "assistant"
    content: str


class ChatCompletionChoice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: int = 0
    message: ChatCompletionAssistantMessage
    finish_reason: Literal["stop"] = "stop"


class ChatCompletionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    object: Literal["chat.completion"] = "chat.completion"
    created: int = Field(ge=0)
    model: ModelEnum
    choices: list[ChatCompletionChoice]
    usage: ChatCompletionUsage


class ChatCompletionDelta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: str | None = None
    role: Literal["assistant"] | None = None


class ChatCompletionChunkChoice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: int = 0
    delta: ChatCompletionDelta
    finish_reason: Literal["stop"] | None = None


class ChatCompletionChunk(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    object: Literal["chat.completion.chunk"] = "chat.completion.chunk"
    created: int = Field(ge=0)
    model: ModelEnum
    choices: list[ChatCompletionChunkChoice]


class ChatStreamErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str
    type: Literal["server_error"] = "server_error"
    code: Literal["upstream_stream_failed"] = "upstream_stream_failed"


class ChatStreamError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ChatStreamErrorDetail
