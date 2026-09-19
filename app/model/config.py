from string import Formatter
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.model.enums import LLMProtocolEnum, ModelEnum, ModelProviderEnum


class ApplicationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    title: str = Field(min_length=1)
    version: str = Field(min_length=1)


class RetryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_retries_per_model: int = Field(ge=0)
    initial_delay_seconds: float = Field(ge=0)
    backoff_multiplier: float = Field(ge=1)


class JsonParsingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    retry_prompt: str = Field(min_length=1, max_length=20_000)

    @field_validator("retry_prompt")
    @classmethod
    def validate_retry_prompt(cls, value: str) -> str:
        allowed = {
            "missing_parameters",
            "invalid_parameters",
            "json_error",
            "schema",
        }
        try:
            fields = {
                field_name
                for _, field_name, _, _ in Formatter().parse(value)
                if field_name is not None
            }
        except ValueError as exc:
            raise ValueError("json_parsing.retry_prompt contains invalid braces") from exc

        missing = allowed - fields
        unknown = fields - allowed
        if missing:
            raise ValueError(
                "json_parsing.retry_prompt is missing placeholders: "
                + ", ".join(sorted(missing))
            )
        if unknown:
            raise ValueError(
                "json_parsing.retry_prompt contains unknown placeholders: "
                + ", ".join(sorted(unknown))
            )
        return value


class SessionRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    postgres_dsn_env: str = Field(default="GATEWAY_POSTGRES_DSN", min_length=1)
    redis_url_env: str = Field(default="GATEWAY_REDIS_URL", min_length=1)
    snowflake_worker_id_env: str = Field(
        default="GATEWAY_SNOWFLAKE_WORKER_ID", min_length=1
    )
    max_active_sessions: int = Field(default=100, ge=1)
    max_queued_sessions: int = Field(default=100, ge=0)
    heartbeat_seconds: float = Field(default=5, gt=0)
    event_ttl_seconds: int = Field(default=3600, ge=1)
    event_block_milliseconds: int = Field(default=1000, ge=1)
    startup_grace_seconds: float = Field(default=30, ge=0)
    producer_lost_grace_seconds: float = Field(default=30, ge=0)
    reconcile_interval_seconds: float = Field(default=10, gt=0)
    shutdown_grace_seconds: float = Field(default=15, ge=0)


class ProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: str = Field(min_length=1)
    api_key_env: str = Field(min_length=1)
    supported_protocols: set[LLMProtocolEnum] = Field(min_length=1)


class ModelCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    streaming: bool
    structured_output: bool
    tools: bool
    multimodal: bool


class ModelPricing(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    input_per_million: float = Field(ge=0)
    output_per_million: float = Field(ge=0)


class ModelRouteConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: ModelProviderEnum
    provider_model: str = Field(min_length=1)
    protocol: LLMProtocolEnum
    capabilities: ModelCapabilities
    pricing: ModelPricing
    structured_output_mode: Literal["json_schema", "json_object"] = "json_schema"
    fallback: ModelEnum | None = None


class GatewayConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    app: ApplicationConfig
    retry: RetryConfig
    json_parsing: JsonParsingConfig
    sessions: SessionRuntimeConfig = Field(default_factory=SessionRuntimeConfig)
    providers: dict[ModelProviderEnum, ProviderConfig] = Field(min_length=1)
    models: dict[ModelEnum, ModelRouteConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_relationships(self) -> "GatewayConfig":
        for model, route in self.models.items():
            provider = self.providers.get(route.provider)
            if provider is None:
                raise ValueError(f"model '{model.value}' references unknown provider '{route.provider.value}'")
            if route.protocol not in provider.supported_protocols:
                raise ValueError(
                    f"provider '{route.provider.value}' does not support "
                    f"protocol '{route.protocol.value}' for model '{model.value}'"
                )
            if route.fallback is None:
                continue
            fallback = self.models.get(route.fallback)
            if fallback is None:
                raise ValueError(
                    f"model '{model.value}' references missing fallback '{route.fallback.value}'"
                )
            if fallback.protocol is not route.protocol:
                raise ValueError(
                    f"fallback '{route.fallback.value}' for model '{model.value}' "
                    "must use the same protocol"
                )
        return self
