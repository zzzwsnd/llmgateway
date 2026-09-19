from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.model.enums import LLMProtocolEnum, ModelEnum, ModelProviderEnum


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


class PromptTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    system_template: str


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
    status: Literal["success", "failed"]
    error_code: str | None = None
