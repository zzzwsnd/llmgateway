import asyncio
import hashlib
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import replace
from typing import Any, Literal
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
from app.model.entity import (
    AttemptStatus,
    AttemptType,
    CallAttempt,
    JsonValidationStatus,
)
from app.model.enums import LLMProtocolEnum, ModelEnum
from app.model.request import LLMRequest, Message, PromptSelection
from app.model.response import LLMResponse, Usage
from app.model.session import SessionInterface
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
        *,
        interface: SessionInterface = SessionInterface.LLM,
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

        await self._trace_service.start_call(
            call_id=request_id,
            requested_model=requested_model,
            prompt=request.prompt,
            interface=interface,
        )

        next_retry_type = AttemptType.RETRY
        attempt_prompt = request.prompt
        attempt_prompt_sha256 = None
        for model_index, model_name in enumerate(
            self._model_sequence(requested_model)
        ):
            try:
                config = self._validate_model(
                    model_name, request.response_schema, required_protocol
                )
                provider = self._provider_factory.get(config.provider)
            except GatewayError as exc:
                if model_name == requested_model:
                    raise
                last_error = exc
                continue

            for retry_number in range(
                self._gateway_config.retry.max_retries_per_model + 1
            ):
                if attempts == 0:
                    attempt_type = AttemptType.INITIAL
                elif retry_number == 0 and model_index > 0:
                    attempt_type = AttemptType.FALLBACK
                else:
                    attempt_type = next_retry_type
                next_retry_type = AttemptType.RETRY
                attempts += 1
                attempt_started = time.monotonic()
                attempt = await self._trace_service.start_attempt(
                    call_id=request_id,
                    model=model_name,
                    prompt=attempt_prompt,
                    attempt_type=attempt_type,
                    json_requested=request.response_schema is not None,
                    prompt_sha256=attempt_prompt_sha256,
                )
                completion = None
                try:
                    completion = await provider.complete(config, provider_request)
                except GatewayError as exc:
                    await self._finish_attempt_audit(
                        attempt,
                        status=AttemptStatus.FAILED,
                        usage=Usage(input_tokens=0, output_tokens=0),
                        latency_ms=self._attempt_elapsed_ms(attempt_started),
                        error_code=exc.code,
                    )
                    await self._finish_call_audit(
                        request_id, status="failed", error_code=exc.code
                    )
                    raise
                except Exception as exc:
                    last_error = exc
                    retryable = isinstance(exc, RetryableProviderError)
                    await self._finish_attempt_audit(
                        attempt,
                        status=AttemptStatus.FAILED,
                        usage=Usage(input_tokens=0, output_tokens=0),
                        latency_ms=self._attempt_elapsed_ms(attempt_started),
                        error_code=(
                            "retryable_provider_error"
                            if retryable
                            else "provider_error"
                        ),
                    )
                    if (
                        retryable
                        and retry_number
                        < self._gateway_config.retry.max_retries_per_model
                    ):
                        await asyncio.sleep(self._retry_delay(retry_number))
                        continue
                    break

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
                try:
                    if request.response_schema is not None:
                        parse_result = self._json_output_parser.parse(
                            completion.content,
                            request.response_schema,
                        )
                        content = parse_result.content
                        parsed = parse_result.value
                        json_status = (
                            JsonValidationStatus.REPAIRED
                            if parse_result.repaired
                            else JsonValidationStatus.VALID
                        )
                    else:
                        json_status = JsonValidationStatus.NOT_REQUESTED
                except JsonOutputValidationError as exc:
                    last_error = exc
                    await self._finish_attempt_audit(
                        attempt,
                        status=AttemptStatus.INVALID_OUTPUT,
                        usage=completion.usage,
                        latency_ms=self._attempt_elapsed_ms(attempt_started),
                        error_code=exc.code,
                        json_validation_status=JsonValidationStatus.INVALID,
                        json_validation_errors=self._json_validation_errors(exc),
                    )
                    if (
                        retry_number
                        < self._gateway_config.retry.max_retries_per_model
                    ):
                        next_retry_type = AttemptType.JSON_RETRY
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
                        attempt_prompt = PromptSelection(
                            name="json-repair",
                            version=(
                                self._gateway_config.json_parsing.retry_prompt.active_version
                            ),
                        )
                        attempt_prompt_sha256 = hashlib.sha256(
                            exc.retry_prompt.encode("utf-8")
                        ).hexdigest()
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
                    await self._finish_call_audit(
                        request_id,
                        status="failed",
                        error_code=gateway_error.code,
                    )
                    raise gateway_error from exc

                await self._finish_attempt_audit(
                    attempt,
                    status=AttemptStatus.SUCCESS,
                    usage=completion.usage,
                    latency_ms=self._attempt_elapsed_ms(attempt_started),
                    json_validation_status=json_status,
                )
                response = LLMResponse(
                    request_id=request_id,
                    model=model_name,
                    content=content,
                    parsed=parsed,
                    usage=total_usage,
                    latency_ms=self._elapsed_ms(started),
                    attempts=attempts,
                )
                await self._finish_call_audit(
                    request_id,
                    status="success",
                )
                return response

        error_code = "model_unavailable"
        await self._finish_call_audit(
            request_id,
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
        *,
        existing_call_id: str | None = None,
        interface: SessionInterface = SessionInterface.LLM,
    ) -> AsyncIterator[ProviderStreamEvent]:
        self.validate_stream_request(request, required_protocol)
        messages = self._prompt_service.build_messages(request)
        return self._stream_events(
            request,
            messages,
            required_protocol,
            existing_call_id=existing_call_id,
            interface=interface,
        )

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
        *,
        existing_call_id: str | None,
        interface: SessionInterface,
    ) -> AsyncIterator[ProviderStreamEvent]:
        started = time.perf_counter()
        attempts = 0
        emitted = False
        last_error: Exception | None = None
        upstream_started: float | None = None
        request_id = existing_call_id or str(uuid4())
        owns_call = existing_call_id is None
        provider_request = ProviderRequest(
            messages=messages,
            timeout_seconds=request.timeout_seconds,
            response_schema_name=request.response_schema_name,
            structured_output_format=request.structured_output_format,
            temperature=request.temperature,
            max_output_tokens=request.max_output_tokens,
        )

        if owns_call:
            await self._trace_service.start_call(
                call_id=request_id,
                requested_model=request.model,
                prompt=request.prompt,
                stream=True,
                interface=interface,
            )

        stop_model_sequence = False
        for model_index, model_name in enumerate(self._model_sequence(request.model)):
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
                if attempts == 0:
                    attempt_type = AttemptType.INITIAL
                elif retry_number == 0 and model_index > 0:
                    attempt_type = AttemptType.FALLBACK
                else:
                    attempt_type = AttemptType.RETRY
                attempts += 1
                attempt_started = time.monotonic()
                attempt = await self._trace_service.start_attempt(
                    call_id=request_id,
                    model=model_name,
                    prompt=request.prompt,
                    attempt_type=attempt_type,
                    json_requested=False,
                )
                attempt_request = replace(
                    provider_request, stream_resume_token=resume_token
                )
                provider_stream = None
                attempt_emitted = False
                attempt_first_delta_ms = None
                attempt_usage = Usage(input_tokens=0, output_tokens=0)
                try:
                    provider_stream = provider.stream(config, attempt_request)
                    if upstream_started is None:
                        upstream_started = time.perf_counter()
                    async for chunk in provider_stream:
                        if chunk.usage is not None:
                            attempt_usage = chunk.usage
                        delta = chunk.delta
                        if not delta:
                            continue
                        first_delta_latency_ms = None
                        if not emitted:
                            first_delta_latency_ms = self._elapsed_ms(upstream_started)
                        emitted = True
                        model_emitted = True
                        attempt_emitted = True
                        if attempt_first_delta_ms is None:
                            attempt_first_delta_ms = self._attempt_elapsed_ms(
                                attempt_started
                            )
                        yield ProviderStreamEvent(
                            type="text_delta",
                            delta=delta,
                            upstream_first_delta_latency_ms=first_delta_latency_ms,
                        )
                except (asyncio.CancelledError, GeneratorExit):
                    await self._finish_attempt_audit(
                        attempt,
                        status=AttemptStatus.CANCELLED,
                        usage=attempt_usage,
                        latency_ms=self._attempt_elapsed_ms(attempt_started),
                        first_delta_latency_ms=attempt_first_delta_ms,
                        error_code="stream_cancelled",
                    )
                    if owns_call:
                        await self._finish_call_audit(
                            request_id,
                            status="cancelled",
                            error_code="stream_cancelled",
                        )
                    raise
                except Exception as exc:
                    last_error = exc
                    await self._finish_attempt_audit(
                        attempt,
                        status=AttemptStatus.FAILED,
                        usage=attempt_usage,
                        latency_ms=self._attempt_elapsed_ms(attempt_started),
                        first_delta_latency_ms=attempt_first_delta_ms,
                        error_code=(
                            "retryable_provider_error"
                            if isinstance(exc, RetryableProviderError)
                            else "provider_error"
                        ),
                    )
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

                await self._finish_attempt_audit(
                    attempt,
                    status=AttemptStatus.SUCCESS,
                    usage=attempt_usage,
                    latency_ms=self._attempt_elapsed_ms(attempt_started),
                    first_delta_latency_ms=attempt_first_delta_ms,
                )
                latency_ms = self._elapsed_ms(started)
                if owns_call:
                    await self._finish_call_audit(request_id, status="success")
                yield ProviderStreamEvent(
                    type="completed",
                    model=model_name,
                    attempts=attempts,
                    latency_ms=latency_ms,
                )
                return

            if stop_model_sequence:
                break

        if last_error is not None:
            logger.error("upstream stream failed")
        latency_ms = self._elapsed_ms(started)
        if owns_call:
            await self._finish_call_audit(
                request_id,
                status="failed",
                error_code="upstream_stream_failed",
            )
        yield ProviderStreamEvent(
            type="failed",
            error_code="upstream_stream_failed",
            attempts=attempts,
            latency_ms=latency_ms,
        )

    async def _finish_attempt_audit(
        self,
        attempt: CallAttempt,
        *,
        status: AttemptStatus,
        usage: Usage,
        latency_ms: int,
        first_delta_latency_ms: int | None = None,
        error_code: str | None = None,
        json_validation_status: JsonValidationStatus | None = None,
        json_validation_errors: list[dict[str, str]] | None = None,
    ) -> None:
        try:
            await self._trace_service.finish_attempt(
                attempt,
                status=status,
                usage=usage,
                latency_ms=latency_ms,
                first_delta_latency_ms=first_delta_latency_ms,
                error_code=error_code,
                json_validation_status=json_validation_status,
                json_validation_errors=json_validation_errors,
            )
        except Exception as exc:
            raise self._audit_persistence_error() from exc

    async def _finish_call_audit(
        self,
        call_id: str,
        *,
        status: Literal["success", "failed", "cancelled"],
        error_code: str | None = None,
    ) -> None:
        try:
            await self._trace_service.finish_call(
                call_id,
                status=status,
                error_code=error_code,
            )
        except Exception as exc:
            raise self._audit_persistence_error() from exc

    @staticmethod
    def _audit_persistence_error() -> GatewayError:
        return GatewayError(
            "audit_persistence_failed",
            "Call audit persistence is unavailable",
            503,
        )

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

    @staticmethod
    def _attempt_elapsed_ms(started: float) -> int:
        return int((time.monotonic() - started) * 1000)

    @staticmethod
    def _json_validation_errors(
        error: JsonOutputValidationError,
    ) -> list[dict[str, str]]:
        issues = [
            {"path": path, "code": "missing"}
            for path in error.missing_parameters
        ]
        issues.extend(
            {
                "path": description.split(":", 1)[0],
                "code": "invalid",
            }
            for description in error.invalid_parameters
        )
        if not issues:
            issues.append({"path": "$", "code": error.code})
        return issues

    def _retry_delay(self, retry_number: int) -> float:
        retry = self._gateway_config.retry
        return retry.initial_delay_seconds * retry.backoff_multiplier**retry_number
