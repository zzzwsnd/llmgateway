import asyncio
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import replace
from typing import Any
from uuid import uuid4

from app.core.errors import (
    GatewayError,
    InvalidJsonSchemaError,
    JsonOutputValidationError,
    RetryableProviderError,
)
from app.core.json_utils import JsonOutputParser
from app.core.logging import logger
from app.core.utils import encode_sse
from app.model.config import GatewayConfig, ModelRouteConfig
from app.model.dto import ProviderRequest, ProviderStreamEvent
from app.model.enums import LLMProtocolEnum, ModelEnum
from app.model.request import LLMRequest, Message
from app.model.response import LLMResponse, Usage
from app.service.prompt_service import PromptService
from app.service.provider_factory import ProviderFactory
from app.service.trace_service import TraceService


class LLMService:
    def __init__(
        self,
        gateway_config: GatewayConfig,
        provider_factory: ProviderFactory,
        prompt_service: PromptService,
        trace_service: TraceService,
    ) -> None:
        self._gateway_config = gateway_config
        self._provider_factory = provider_factory
        self._prompt_service = prompt_service
        self._trace_service = trace_service
        self._json_output_parser = JsonOutputParser(gateway_config.json_parsing)

    async def complete(
        self,
        request: LLMRequest,
        required_protocol: LLMProtocolEnum | None = None,
    ) -> LLMResponse:
        if request.stream:
            raise GatewayError(
                "use_stream_endpoint",
                "Use /v1/llm/stream for streaming requests",
                400,
            )

        requested_model = request.model
        self._validate_model(
            requested_model, request.response_schema, required_protocol
        )
        self._validate_response_schema(request.response_schema)
        request_id = str(uuid4())
        started = time.perf_counter()
        attempts = 0
        last_error: Exception | None = None
        total_usage = Usage(input_tokens=0, output_tokens=0)
        messages = self._prompt_service.build_messages(request)
        provider_request = ProviderRequest(
            messages=messages,
            timeout_seconds=request.timeout_seconds,
            response_schema=request.response_schema,
            response_schema_name=request.response_schema_name,
            structured_output_format=request.structured_output_format,
            temperature=request.temperature,
            max_output_tokens=request.max_output_tokens,
        )

        for model_name in self._model_sequence(requested_model):
            try:
                config = self._validate_model(
                    model_name, request.response_schema, required_protocol
                )
            except GatewayError as exc:
                if model_name == requested_model:
                    raise
                last_error = exc
                continue

            for retry_number in range(
                self._gateway_config.retry.max_retries_per_model + 1
            ):
                attempts += 1
                try:
                    provider = self._provider_factory.get(config.provider)
                    completion = await provider.complete(config, provider_request)
                    total_usage = Usage(
                        input_tokens=(
                            total_usage.input_tokens + completion.usage.input_tokens
                        ),
                        output_tokens=(
                            total_usage.output_tokens + completion.usage.output_tokens
                        ),
                    )
                    content = completion.content
                    parsed = None
                    if request.response_schema is not None:
                        parse_result = self._json_output_parser.parse(
                            completion.content,
                            request.response_schema,
                        )
                        content = parse_result.content
                        parsed = parse_result.value
                    response = LLMResponse(
                        request_id=request_id,
                        model=model_name,
                        content=content,
                        parsed=parsed,
                        usage=total_usage,
                        latency_ms=self._elapsed_ms(started),
                        attempts=attempts,
                    )
                    self._trace_service.record(
                        request_id=request_id,
                        requested_model=requested_model,
                        actual_model=model_name,
                        prompt=request.prompt,
                        usage=total_usage,
                        latency_ms=response.latency_ms,
                        attempts=attempts,
                        status="success",
                    )
                    return response
                except JsonOutputValidationError as exc:
                    last_error = exc
                    if (
                        retry_number
                        < self._gateway_config.retry.max_retries_per_model
                    ):
                        retry_messages = list(messages)
                        if completion.content:
                            retry_messages.append(
                                Message(role="assistant", content=completion.content)
                            )
                        retry_messages.append(
                            Message(role="user", content=exc.retry_prompt)
                        )
                        provider_request = replace(
                            provider_request,
                            messages=retry_messages,
                        )
                        await asyncio.sleep(self._retry_delay(retry_number))
                        continue

                    gateway_error = GatewayError(
                        exc.code,
                        (
                            "Model did not return valid JSON"
                            if exc.code == "invalid_json"
                            else "Model output does not match response_schema"
                        ),
                    )
                    self._trace_service.record(
                        request_id=request_id,
                        requested_model=requested_model,
                        actual_model=model_name,
                        prompt=request.prompt,
                        usage=total_usage,
                        latency_ms=self._elapsed_ms(started),
                        attempts=attempts,
                        status="failed",
                        error_code=gateway_error.code,
                    )
                    raise gateway_error from exc
                except GatewayError as exc:
                    self._trace_service.record(
                        request_id=request_id,
                        requested_model=requested_model,
                        actual_model=model_name,
                        prompt=request.prompt,
                        usage=total_usage,
                        latency_ms=self._elapsed_ms(started),
                        attempts=attempts,
                        status="failed",
                        error_code=exc.code,
                    )
                    raise
                except Exception as exc:
                    last_error = exc
                    if (
                        isinstance(exc, RetryableProviderError)
                        and retry_number
                        < self._gateway_config.retry.max_retries_per_model
                    ):
                        await asyncio.sleep(self._retry_delay(retry_number))
                        continue
                    break

        latency_ms = self._elapsed_ms(started)
        error_code = "model_unavailable"
        self._trace_service.record(
            request_id=request_id,
            requested_model=requested_model,
            actual_model=None,
            prompt=request.prompt,
            usage=total_usage,
            latency_ms=latency_ms,
            attempts=attempts,
            status="failed",
            error_code=error_code,
        )
        raise GatewayError(
            error_code, "Primary model and configured fallback are unavailable"
        ) from last_error

    def stream_events(
        self,
        request: LLMRequest,
        required_protocol: LLMProtocolEnum | None = None,
    ) -> AsyncIterator[ProviderStreamEvent]:
        self.validate_stream_request(request, required_protocol)
        messages = self._prompt_service.build_messages(request)
        return self._stream_events(request, messages, required_protocol)

    def validate_stream_request(
        self,
        request: LLMRequest,
        required_protocol: LLMProtocolEnum | None = None,
    ) -> None:
        if request.response_schema is not None:
            raise GatewayError(
                "unsupported_combination",
                "Streaming does not support response_schema",
                400,
            )
        self._validate_model(request.model, None, required_protocol)

    def stream(self, request: LLMRequest) -> AsyncIterator[str]:
        return self._legacy_stream(self.stream_events(request))

    async def _stream_events(
        self,
        request: LLMRequest,
        messages: list[Message],
        required_protocol: LLMProtocolEnum | None,
    ) -> AsyncIterator[ProviderStreamEvent]:
        started = time.perf_counter()
        attempts = 0
        emitted = False
        last_error: Exception | None = None
        upstream_started: float | None = None
        request_id = str(uuid4())
        provider_request = ProviderRequest(
            messages=messages,
            timeout_seconds=request.timeout_seconds,
            response_schema_name=request.response_schema_name,
            structured_output_format=request.structured_output_format,
            temperature=request.temperature,
            max_output_tokens=request.max_output_tokens,
        )

        stop_model_sequence = False
        for model_name in self._model_sequence(request.model):
            try:
                config = self._validate_model(model_name, None, required_protocol)
                provider = self._provider_factory.get(config.provider)
            except Exception as exc:
                last_error = exc
                stop_model_sequence = True
                break

            # Resume state belongs to one upstream model and is reset on fallback.
            model_emitted = False
            resume_token: str | None = None
            for retry_number in range(
                self._gateway_config.retry.max_retries_per_model + 1
            ):
                attempts += 1
                attempt_request = replace(
                    provider_request, stream_resume_token=resume_token
                )
                provider_stream = None
                attempt_emitted = False
                try:
                    provider_stream = provider.stream(config, attempt_request)
                    if upstream_started is None:
                        upstream_started = time.perf_counter()
                    async for delta in provider_stream:
                        if not delta:
                            continue
                        first_delta_latency_ms = None
                        if not emitted:
                            first_delta_latency_ms = self._elapsed_ms(upstream_started)
                        emitted = True
                        model_emitted = True
                        attempt_emitted = True
                        yield ProviderStreamEvent(
                            type="text_delta",
                            delta=delta,
                            upstream_first_delta_latency_ms=first_delta_latency_ms,
                        )

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
                    yield ProviderStreamEvent(type="completed", model=model_name)
                    return
                except Exception as exc:
                    last_error = exc
                    retryable = isinstance(exc, RetryableProviderError)
                    next_resume_token = None
                    if retryable:
                        next_resume_token = exc.resume_token
                        if not attempt_emitted and next_resume_token is None:
                            next_resume_token = resume_token
                    can_retry = (
                        retryable
                        and retry_number
                        < self._gateway_config.retry.max_retries_per_model
                        and (not model_emitted or next_resume_token is not None)
                    )
                    if can_retry:
                        resume_token = next_resume_token
                        await asyncio.sleep(self._retry_delay(retry_number))
                        continue
                    if model_emitted or not retryable:
                        stop_model_sequence = True
                    break
                finally:
                    if provider_stream is not None:
                        close = getattr(provider_stream, "aclose", None)
                        if close is not None:
                            await close()

            if stop_model_sequence:
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
        yield ProviderStreamEvent(type="failed", error_code="upstream_stream_failed")

    async def _legacy_stream(
        self, events: AsyncIterator[ProviderStreamEvent]
    ) -> AsyncIterator[str]:
        async for event in events:
            if event.type == "text_delta":
                yield encode_sse({"type": "content.delta", "delta": event.delta})
            elif event.type == "completed":
                yield encode_sse({"type": "response.completed", "model": event.model})
            else:
                yield encode_sse(
                    {"type": "response.failed", "error": event.error_code}
                )

    def _model_sequence(self, requested_model: ModelEnum) -> Iterator[ModelEnum]:
        model: ModelEnum | None = requested_model
        visited: set[ModelEnum] = set()
        while model is not None and model not in visited:
            visited.add(model)
            yield model
            route = self._gateway_config.models.get(model)
            model = route.fallback if route is not None else None

    def _validate_model(
        self,
        model: ModelEnum,
        response_schema: dict[str, Any] | None,
        required_protocol: LLMProtocolEnum | None = None,
    ) -> ModelRouteConfig:
        config = self._gateway_config.models.get(model)
        if config is None:
            raise GatewayError("unknown_model", "Model is not configured", 400)
        if required_protocol is not None and config.protocol is not required_protocol:
            raise GatewayError(
                "protocol_mismatch",
                "Selected model does not support the required protocol",
                400,
            )
        if response_schema is not None and not config.capabilities.structured_output:
            raise GatewayError(
                "structured_output_unsupported",
                "Model does not support structured output",
                400,
            )
        return config

    def _validate_response_schema(
        self, response_schema: dict[str, Any] | None
    ) -> None:
        if response_schema is None:
            return
        try:
            self._json_output_parser.validate_schema(response_schema)
        except InvalidJsonSchemaError as exc:
            raise GatewayError(
                "invalid_response_schema",
                "response_schema is not a valid JSON Schema",
                400,
            ) from exc

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return int((time.perf_counter() - started) * 1000)

    def _retry_delay(self, retry_number: int) -> float:
        retry = self._gateway_config.retry
        return retry.initial_delay_seconds * retry.backoff_multiplier**retry_number
