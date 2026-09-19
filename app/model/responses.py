from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.model.enums import ModelEnum


class ResponseInputMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1, max_length=20_000)


class ResponseTextFormatJsonSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["json_schema"]
    name: str = Field(min_length=1, max_length=64)
    schema_: dict[str, Any] = Field(alias="schema")
    strict: Literal[True] = True


class ResponseTextFormatJsonObject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["json_object"]


ResponseTextFormat = Annotated[
    ResponseTextFormatJsonSchema | ResponseTextFormatJsonObject,
    Field(discriminator="type"),
]


class ResponseTextConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: ResponseTextFormat


class ResponseCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: ModelEnum
    input: Annotated[str, Field(min_length=1, max_length=20_000)] | Annotated[
        list[ResponseInputMessage], Field(min_length=1, max_length=100)
    ]
    stream: bool = False
    temperature: float | None = Field(default=None, ge=0, le=2)
    max_output_tokens: int | None = Field(default=None, gt=0)
    text: ResponseTextConfig | None = None

    @model_validator(mode="after")
    def check_supported_combination(self) -> "ResponseCreateRequest":
        if self.stream and self.text is not None:
            raise ValueError("Structured output is not supported with streaming")
        return self

    @property
    def response_schema(self) -> dict[str, Any] | None:
        if self.text is None:
            return None
        if isinstance(self.text.format, ResponseTextFormatJsonSchema):
            return self.text.format.schema_
        return {"type": "object"}

    @property
    def response_schema_name(self) -> str | None:
        if self.text is not None and isinstance(
            self.text.format, ResponseTextFormatJsonSchema
        ):
            return self.text.format.name
        return None


class ResponseInputTokensDetails(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cached_tokens: int = Field(default=0, ge=0)
    cache_write_tokens: int = Field(default=0, ge=0)


class ResponseOutputTokensDetails(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reasoning_tokens: int = Field(default=0, ge=0)


class ResponseUsage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_tokens: int = Field(ge=0)
    input_tokens_details: ResponseInputTokensDetails = Field(
        default_factory=ResponseInputTokensDetails
    )
    output_tokens: int = Field(ge=0)
    output_tokens_details: ResponseOutputTokensDetails = Field(
        default_factory=ResponseOutputTokensDetails
    )
    total_tokens: int = Field(ge=0)


class ResponseOutputText(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["output_text"] = "output_text"
    text: str
    annotations: list[dict[str, Any]] = Field(default_factory=list)


class ResponseOutputMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: Literal["message"] = "message"
    status: Literal["in_progress", "completed"] = "completed"
    role: Literal["assistant"] = "assistant"
    content: list[ResponseOutputText]


class ResponseError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Literal["server_error"] = "server_error"
    message: str


class ResponseObject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    object: Literal["response"] = "response"
    created_at: float = Field(ge=0)
    status: Literal["in_progress", "completed", "failed"]
    model: ModelEnum
    output: list[ResponseOutputMessage]
    parallel_tool_calls: bool = False
    tool_choice: Literal["none"] = "none"
    tools: list[dict[str, Any]] = Field(default_factory=list)
    error: ResponseError | None = None
    usage: ResponseUsage | None = None


class ResponseCreatedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.created"] = "response.created"
    sequence_number: int = Field(ge=0)
    response: ResponseObject


class ResponseOutputItemAddedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.output_item.added"] = "response.output_item.added"
    sequence_number: int = Field(ge=0)
    output_index: Literal[0] = 0
    item: ResponseOutputMessage


class ResponseContentPartAddedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.content_part.added"] = "response.content_part.added"
    sequence_number: int = Field(ge=0)
    item_id: str
    output_index: Literal[0] = 0
    content_index: Literal[0] = 0
    part: ResponseOutputText


class ResponseOutputTextDeltaEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.output_text.delta"] = "response.output_text.delta"
    sequence_number: int = Field(ge=0)
    item_id: str
    output_index: Literal[0] = 0
    content_index: Literal[0] = 0
    delta: str
    logprobs: list[dict[str, Any]] = Field(default_factory=list)


class ResponseOutputTextDoneEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.output_text.done"] = "response.output_text.done"
    sequence_number: int = Field(ge=0)
    item_id: str
    output_index: Literal[0] = 0
    content_index: Literal[0] = 0
    text: str
    logprobs: list[dict[str, Any]] = Field(default_factory=list)


class ResponseContentPartDoneEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.content_part.done"] = "response.content_part.done"
    sequence_number: int = Field(ge=0)
    item_id: str
    output_index: Literal[0] = 0
    content_index: Literal[0] = 0
    part: ResponseOutputText


class ResponseOutputItemDoneEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.output_item.done"] = "response.output_item.done"
    sequence_number: int = Field(ge=0)
    output_index: Literal[0] = 0
    item: ResponseOutputMessage


class ResponseCompletedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.completed"] = "response.completed"
    sequence_number: int = Field(ge=0)
    response: ResponseObject


class ResponseFailedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["response.failed"] = "response.failed"
    sequence_number: int = Field(ge=0)
    response: ResponseObject
