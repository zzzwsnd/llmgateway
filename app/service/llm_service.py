import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

from jsonschema import ValidationError as JsonSchemaError
from jsonschema import validate

from app.core.config import (
    BACKUP_MODEL,
    MAX_ATTEMPTS_PER_MODEL,
    RETRY_DELAY_SECONDS,
)
from app.core.errors import GatewayError, RetryableProviderError
from app.core.logging import logger
from app.core.utils import encode_sse
from app.dao.model_dao import ModelDao
from app.dao.provider_dao import ProviderDao
from app.model.entity import ModelConfig
from app.model.request import LLMRequest, Message
from app.model.response import LLMResponse, Usage
from app.service.prompt_service import PromptService
from app.service.trace_service import TraceService


class LLMService:
    def __init__(
        self,
        model_dao: ModelDao,
        provider_dao: ProviderDao,
        prompt_service: PromptService,
        trace_service: TraceService,
    ) -> None:
        self._model_dao = model_dao
        self._provider_dao = provider_dao
        self._prompt_service = prompt_service
        self._trace_service = trace_service

    async def complete(self, request: LLMRequest) -> LLMResponse:
        if request.stream:
            raise GatewayError(
                "use_stream_endpoint",
                "流式请求请使用 /v1/llm/stream",
                400,
            )

        requested_model = request.model
        request_id = str(uuid4())
        started = time.perf_counter()
        attempts = 0
        last_error: Exception | None = None
        messages = self._prompt_service.build_messages(request)

        for model_name in dict.fromkeys([requested_model, BACKUP_MODEL]):
            try:
                config = self._validate_model(model_name, request.response_schema)
            except GatewayError as exc:
                if model_name == requested_model:
                    raise
                last_error = exc
                continue

            for retry_number in range(MAX_ATTEMPTS_PER_MODEL):
                attempts += 1
                try:
                    completion = await self._provider_dao.complete(
                        config,
                        messages,
                        request.timeout_seconds,
                        request.response_schema,
                    )
                    parsed = self._parse_structured_output(
                        completion.content,
                        request.response_schema,
                    )
                    response = LLMResponse(
                        request_id=request_id,
                        model=model_name,
                        content=completion.content,
                        parsed=parsed,
                        usage=completion.usage,
                        latency_ms=self._elapsed_ms(started),
                        attempts=attempts,
                    )
                    self._trace_service.record(
                        request_id=request_id,
                        requested_model=requested_model,
                        actual_model=model_name,
                        prompt=request.prompt,
                        usage=completion.usage,
                        latency_ms=response.latency_ms,
                        attempts=attempts,
                        status="success",
                    )
                    return response
                except GatewayError:
                    raise
                except Exception as exc:
                    last_error = exc
                    if (
                        isinstance(exc, RetryableProviderError)
                        and retry_number < MAX_ATTEMPTS_PER_MODEL - 1
                    ):
                        await asyncio.sleep(RETRY_DELAY_SECONDS)
                        continue
                    break

        latency_ms = self._elapsed_ms(started)
        error_code = "model_unavailable"
        self._trace_service.record(
            request_id=request_id,
            requested_model=requested_model,
            actual_model=None,
            prompt=request.prompt,
            usage=Usage(input_tokens=0, output_tokens=0),
            latency_ms=latency_ms,
            attempts=attempts,
            status="failed",
            error_code=error_code,
        )
        raise GatewayError(error_code, "主模型和备用模型均不可用") from last_error

    def stream(self, request: LLMRequest) -> AsyncIterator[str]:
        if request.response_schema is not None:
            raise GatewayError(
                "unsupported_combination",
                "流式输出不支持 response_schema",
                400,
            )
        self._validate_model(request.model, None)
        messages = self._prompt_service.build_messages(request)
        return self._stream_events(request, messages)

    async def _stream_events(
        self,
        request: LLMRequest,
        messages: list[Message],
    ) -> AsyncIterator[str]:
        started = time.perf_counter()
        attempts = 0
        emitted = False
        last_error: Exception | None = None
        request_id = str(uuid4())

        for model_name in dict.fromkeys([request.model, BACKUP_MODEL]):
            try:
                config = self._validate_model(model_name, None)
                attempts += 1
                async for delta in self._provider_dao.stream(
                    config,
                    messages,
                    request.timeout_seconds,
                ):
                    emitted = True
                    yield encode_sse({"type": "content.delta", "delta": delta})

                self._trace_service.record(
                    request_id=request_id,
                    requested_model=request.model,
                    actual_model=model_name,
                    prompt=request.prompt,
                    usage=Usage(input_tokens=0, output_tokens=0),
                    latency_ms=self._elapsed_ms(started),
                    attempts=attempts,
                    status="success",
                )
                yield encode_sse({"type": "response.completed", "model": model_name})
                return
            except Exception as exc:
                last_error = exc
                if emitted or not isinstance(exc, RetryableProviderError):
                    break

        if last_error is not None:
            logger.error(
                "upstream stream failed",
                exc_info=(type(last_error), last_error, last_error.__traceback__),
            )
        self._trace_service.record(
            request_id=request_id,
            requested_model=request.model,
            actual_model=None,
            prompt=request.prompt,
            usage=Usage(input_tokens=0, output_tokens=0),
            latency_ms=self._elapsed_ms(started),
            attempts=attempts,
            status="failed",
            error_code="upstream_stream_failed",
        )
        yield encode_sse(
            {"type": "response.failed", "error": "upstream_stream_failed"}
        )

    def _validate_model(
        self,
        model: str,
        response_schema: dict[str, Any] | None,
    ) -> ModelConfig:
        config = self._model_dao.get_config(model)
        if config is None:
            raise GatewayError(
                "unknown_model",
                "模型不在 Gateway 允许列表中",
                400,
            )
        if response_schema is not None and not config.supports_structured_output:
            raise GatewayError(
                "structured_output_unsupported",
                "模型不支持 Structured Output",
                400,
            )
        return config

    @staticmethod
    def _parse_structured_output(
        content: str,
        response_schema: dict[str, Any] | None,
    ) -> dict[str, Any] | list[Any] | None:
        if response_schema is None:
            return None
        try:
            parsed = json.loads(content)
            validate(instance=parsed, schema=response_schema)
        except json.JSONDecodeError as exc:
            raise GatewayError("invalid_json", "模型没有返回合法 JSON") from exc
        except JsonSchemaError as exc:
            raise GatewayError(
                "schema_validation_failed",
                "模型结果不符合 response_schema",
            ) from exc
        return parsed

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return int((time.perf_counter() - started) * 1000)
