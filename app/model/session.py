from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.model.chat_completions import ChatCompletionRequest
from app.model.enums import ModelEnum
from app.model.request import LLMRequest
from app.model.responses import ResponseCreateRequest


class SessionInterface(str, Enum):
    LLM = "llm"
    CHAT_COMPLETIONS = "chat_completions"
    RESPONSES = "responses"


class SessionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in {
            SessionStatus.COMPLETED,
            SessionStatus.FAILED,
            SessionStatus.CANCELLED,
        }


class SessionAction(str, Enum):
    CREATE = "create"
    READ = "read"
    SUBSCRIBE = "subscribe"
    DETACH = "detach"
    CANCEL = "cancel"


class SessionEventType(str, Enum):
    HEARTBEAT = "_heartbeat"
    CONNECTED = "session.connected"
    DELTA = "content.delta"
    SNAPSHOT = "session.snapshot"
    COMPLETED = "session.completed"
    FAILED = "session.failed"
    CANCELLED = "session.cancelled"


class RuntimeEventType(str, Enum):
    DELTA = SessionEventType.DELTA.value
    COMPLETED = SessionEventType.COMPLETED.value
    FAILED = SessionEventType.FAILED.value
    CANCELLED = SessionEventType.CANCELLED.value
    CONTROL_CANCEL = "control.cancel"
    CONTROL_DETACH = "control.detach"


class CreateLLMSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interface: Literal[SessionInterface.LLM]
    request: LLMRequest

    @model_validator(mode="after")
    def reject_structured_streaming(self) -> "CreateLLMSession":
        if self.request.response_schema is not None:
            raise ValueError("Structured output is not supported with streaming")
        return self


class CreateChatSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interface: Literal[SessionInterface.CHAT_COMPLETIONS]
    request: ChatCompletionRequest

    @model_validator(mode="after")
    def reject_structured_streaming(self) -> "CreateChatSession":
        if self.request.response_format is not None:
            raise ValueError("Structured output is not supported with streaming")
        return self


class CreateResponsesSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interface: Literal[SessionInterface.RESPONSES]
    request: ResponseCreateRequest

    @model_validator(mode="after")
    def reject_structured_streaming(self) -> "CreateResponsesSession":
        if self.request.text is not None:
            raise ValueError("Structured output is not supported with streaming")
        return self


CreateStreamSessionRequest = Annotated[
    CreateLLMSession | CreateChatSession | CreateResponsesSession,
    Field(discriminator="interface"),
]


class StreamSession(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    session_id: str = Field(min_length=1)
    interface: SessionInterface
    requested_model: ModelEnum
    status: SessionStatus
    generation_id: str = Field(min_length=1)
    request_fingerprint: str = Field(min_length=1)
    idempotency_key: str | None = None
    owner_id: str | None = None
    version: int = Field(default=0, ge=0)
    result_text: str | None = None
    error_code: str | None = None
    replay_degraded: bool = False
    created_at: AwareDatetime
    updated_at: AwareDatetime
    started_at: AwareDatetime | None = None
    completed_at: AwareDatetime | None = None
    cancelled_at: AwareDatetime | None = None
    upstream_first_delta_latency_ms: int | None = Field(default=None, ge=0)
    first_content_latency_ms: int | None = Field(default=None, ge=0)
    total_duration_ms: int | None = Field(default=None, ge=0)


class SessionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str | None = None
    type: SessionEventType
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: AwareDatetime


class RuntimeEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str | None = None
    type: RuntimeEventType
    generation_id: str = Field(min_length=1)
    connection_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: AwareDatetime


class SessionConnection(BaseModel):
    """Ephemeral subscriber state; never persisted in Redis or PostgreSQL."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    connection_id: str
    session_id: str
    connected_at: AwareDatetime


class CreateStreamSessionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    status: SessionStatus
    status_url: str
    events_url: str


class StreamSessionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    interface: SessionInterface
    model: ModelEnum
    status: SessionStatus
    result_text: str | None = None
    error_code: str | None = None
    replay_degraded: bool = False
    replay_available: bool
    active_connections: int | None = None
    producer_health: Literal["active", "missing", "unknown"]
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    upstream_first_delta_latency_ms: int | None = Field(default=None, ge=0)
    first_content_latency_ms: int | None = Field(default=None, ge=0)
    total_duration_ms: int | None = Field(default=None, ge=0)
