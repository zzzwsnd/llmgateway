from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.model.enums import LLMProtocolEnum, ModelEnum, ModelProviderEnum
from app.model.session import SessionInterface


@dataclass(frozen=True)
class ModelConfig:
    model: ModelEnum
    provider: ModelProviderEnum
    provider_model: str
    base_url: str
    api_key_env: str
    protocol: LLMProtocolEnum
    supports_structured_output: bool
    structured_output_mode: Literal["json_schema", "json_object"] = "json_schema"


@dataclass(frozen=True)
class ModelPrice:
    input_per_million: float
    output_per_million: float


class Permission(BaseModel):
    """A named capability that can be granted to a role."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    description: str | None = None


class Role(BaseModel):
    """A named collection of permission names."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    permissions: frozenset[str] = Field(default_factory=frozenset)


class User(BaseModel):
    """An identity associated with one or more role names."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    user_id: str = Field(min_length=1)
    roles: frozenset[str] = Field(default_factory=frozenset)
    is_active: bool = True


class PromptTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    system_template: str


class CallStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in {
            CallStatus.SUCCESS,
            CallStatus.FAILED,
            CallStatus.CANCELLED,
        }


class AttemptType(str, Enum):
    INITIAL = "initial"
    RETRY = "retry"
    FALLBACK = "fallback"
    JSON_RETRY = "json_retry"


class AttemptStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    INVALID_OUTPUT = "invalid_output"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self is not AttemptStatus.RUNNING


class JsonValidationStatus(str, Enum):
    NOT_REQUESTED = "not_requested"
    VALID = "valid"
    REPAIRED = "repaired"
    INVALID = "invalid"


class CallRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    call_id: str = Field(min_length=1)
    retry_of_call_id: str | None = None
    idempotency_key: str | None = None
    request_fingerprint: str | None = None
    interface: SessionInterface = SessionInterface.LLM
    stream: bool = False
    owner_id: str | None = None
    requested_model: ModelEnum
    prompt_name: str | None = None
    prompt_version: str | None = None
    status: CallStatus
    error_code: str | None = None
    replay_degraded: bool = False
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class CallAttempt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    attempt_id: str = Field(min_length=1)
    call_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=0)
    attempt_type: AttemptType
    model: ModelEnum
    prompt_name: str | None = None
    prompt_version: str | None = None
    prompt_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    status: AttemptStatus
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cost_usd: float = Field(default=0, ge=0)
    latency_ms: int = Field(default=0, ge=0)
    first_delta_latency_ms: int | None = Field(default=None, ge=0)
    error_code: str | None = None
    json_validation_status: JsonValidationStatus
    json_validation_errors: list[dict[str, Any]] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime | None = None


class CallTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    timestamp: datetime
    requested_model: str
    actual_model: str | None = None
    prompt_name: str | None = None
    prompt_version: str | None = None
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cost_usd: float = Field(ge=0)
    latency_ms: int = Field(ge=0)
    attempts: int = Field(ge=0)
    status: CallStatus
    error_code: str | None = None
