import json
from collections.abc import AsyncIterator
from typing import Any

from openai import AsyncOpenAI

from app.core.config import resolve_api_key
from app.mapper.openai_mapper import close_provider_stream, raise_retryable_provider_error
from app.model.config import ModelRouteConfig, ProviderConfig
from app.model.dto import ProviderCompletion, ProviderRequest
from app.model.response import Usage


class ChatCompletionsMapper:
    async def complete(
        self, provider: ProviderConfig, model: ModelRouteConfig, request: ProviderRequest
    ) -> ProviderCompletion:
        try:
            completion = await self._create_client(provider).chat.completions.create(
                **self._request_data(model, request)
            )
        except Exception as exc:
            raise_retryable_provider_error(exc)

        usage = completion.usage
        return ProviderCompletion(
            content=completion.choices[0].message.content or "",
            usage=Usage(
                input_tokens=usage.prompt_tokens if usage else 0,
                output_tokens=usage.completion_tokens if usage else 0,
            ),
        )

    async def stream(
        self, provider: ProviderConfig, model: ModelRouteConfig, request: ProviderRequest
    ) -> AsyncIterator[str]:
        response = None
        try:
            response = await self._create_client(provider).chat.completions.create(
                **self._request_data(model, request, stream=True)
            )
            async for chunk in response:
                choice = chunk.choices[0] if chunk.choices else None
                if choice and choice.delta.content:
                    yield choice.delta.content
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
            "messages": [message.model_dump() for message in request.messages],
            "timeout": request.timeout_seconds,
        }
        if stream:
            data["stream"] = True
        if request.temperature is not None:
            data["temperature"] = request.temperature
        if request.max_output_tokens is not None:
            data["max_tokens"] = request.max_output_tokens
        if request.response_schema is not None:
            data["response_format"] = _response_format(
                model,
                request.response_schema,
                request.response_schema_name,
                request.structured_output_format,
            )
            if _uses_json_object(model, request):
                data["messages"] = [
                    {
                        "role": "system",
                        "content": (
                            "Return only a valid JSON object that conforms to this JSON Schema. "
                            "Do not return Markdown or any other text: "
                            f"{json.dumps(request.response_schema, ensure_ascii=False)}"
                        ),
                    },
                    *data["messages"],
                ]
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
        "json_schema": {
            "name": schema_name or "agent_response",
            "strict": True,
            "schema": schema,
        },
    }


def _uses_json_object(model: ModelRouteConfig, request: ProviderRequest) -> bool:
    return (
        request.structured_output_format == "json_object"
        or model.structured_output_mode == "json_object"
    )
