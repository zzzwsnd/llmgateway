from collections.abc import AsyncIterator
from typing import Any

from openai import AsyncOpenAI

from app.core.config import resolve_api_key
from app.core.errors import RetryableProviderError
from app.mapper.openai_mapper import close_provider_stream, raise_retryable_provider_error
from app.model.config import ModelRouteConfig, ProviderConfig
from app.model.dto import ProviderCompletion, ProviderRequest, ProviderStreamChunk
from app.model.response import Usage


class ResponsesMapper:
    async def complete(
        self, provider: ProviderConfig, model: ModelRouteConfig, request: ProviderRequest
    ) -> ProviderCompletion:
        try:
            response = await self._create_client(provider).responses.create(
                **self._request_data(model, request)
            )
        except Exception as exc:
            raise_retryable_provider_error(exc)

        _raise_for_terminal_status(getattr(response, "status", None))
        usage = response.usage
        return ProviderCompletion(
            content=response.output_text or "",
            usage=Usage(
                input_tokens=usage.input_tokens if usage else 0,
                output_tokens=usage.output_tokens if usage else 0,
            ),
        )

    async def stream(
        self, provider: ProviderConfig, model: ModelRouteConfig, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamChunk]:
        response = None
        try:
            response = await self._create_client(provider).responses.create(
                **self._request_data(model, request, stream=True)
            )
            async for event in response:
                if event.type == "response.output_text.delta" and event.delta:
                    yield ProviderStreamChunk(delta=event.delta)
                elif event.type == "response.completed":
                    raw_usage = getattr(
                        getattr(event, "response", None), "usage", None
                    )
                    if raw_usage is not None:
                        yield ProviderStreamChunk(
                            usage=Usage(
                                input_tokens=raw_usage.input_tokens,
                                output_tokens=raw_usage.output_tokens,
                            )
                        )
                elif event.type in {"response.failed", "response.incomplete"}:
                    raw_usage = getattr(
                        getattr(event, "response", None), "usage", None
                    )
                    if raw_usage is not None:
                        yield ProviderStreamChunk(
                            usage=Usage(
                                input_tokens=raw_usage.input_tokens,
                                output_tokens=raw_usage.output_tokens,
                            )
                        )
                    raise RetryableProviderError(
                        "Upstream Responses request did not complete"
                    )
        except Exception as exc:
            raise_retryable_provider_error(exc)
        finally:
            if response is not None:
                await close_provider_stream(response)

    @staticmethod
    def _create_client(provider: ProviderConfig) -> AsyncOpenAI:
        return AsyncOpenAI(api_key=resolve_api_key(provider), base_url=provider.base_url, max_retries=0)

    @staticmethod
    def _request_data(
        model: ModelRouteConfig, request: ProviderRequest, *, stream: bool = False
    ) -> dict[str, Any]:
        data: dict[str, Any] = {
            "model": model.provider_model,
            "input": [message.model_dump() for message in request.messages],
            "timeout": request.timeout_seconds,
        }
        if stream:
            data["stream"] = True
        if request.temperature is not None:
            data["temperature"] = request.temperature
        if request.max_output_tokens is not None:
            data["max_output_tokens"] = request.max_output_tokens
        if request.response_schema is not None:
            data["text"] = {
                "format": _response_format(
                    model,
                    request.response_schema,
                    request.response_schema_name,
                    request.structured_output_format,
                )
            }
        return data


def _response_format(
    model: ModelRouteConfig,
    schema: dict[str, Any],
    schema_name: str | None,
    structured_output_format: str | None = None,
) -> dict[str, Any]:
    if (
        structured_output_format == "json_object"
        or model.structured_output_mode == "json_object"
    ):
        return {"type": "json_object"}
    return {
        "type": "json_schema",
        "name": schema_name or "agent_response",
        "strict": True,
        "schema": schema,
    }


def _raise_for_terminal_status(status: str | None) -> None:
    if status in {"failed", "incomplete"}:
        raise RetryableProviderError("Upstream Responses request did not complete")
