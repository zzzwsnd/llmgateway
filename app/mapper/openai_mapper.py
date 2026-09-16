import json
import os
from collections.abc import AsyncIterator
from typing import Any, Protocol

from openai import APIConnectionError, APITimeoutError, AsyncOpenAI, RateLimitError

from app.core.errors import GatewayError, RetryableProviderError
from app.model.dto import ProviderCompletion
from app.model.entity import ModelConfig
from app.model.request import Message
from app.model.response import Usage


class ProviderMapper(Protocol):
    async def complete(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
        response_schema: dict[str, Any] | None,
    ) -> ProviderCompletion: ...

    def stream(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
    ) -> AsyncIterator[str]: ...


class OpenAIMapper:
    def _create_client(self, config: ModelConfig) -> AsyncOpenAI:
        api_key = os.getenv(config.api_key_env)
        if not api_key:
            raise GatewayError("gateway_misconfigured", "Gateway 模型凭据未配置", 503)
        return AsyncOpenAI(api_key=api_key, base_url=config.base_url, max_retries=0)

    async def complete(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
        response_schema: dict[str, Any] | None,
    ) -> ProviderCompletion:
        request_data: dict[str, Any] = {
            "model": config.provider_model,
            "messages": [message.model_dump() for message in messages],
            "timeout": timeout_seconds,
        }
        if response_schema is not None:
            if config.structured_output_mode == "json_schema":
                request_data["response_format"] = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "agent_response",
                        "strict": True,
                        "schema": response_schema,
                    },
                }
            else:
                request_data["response_format"] = {"type": "json_object"}
                request_data["messages"] = [
                    {
                        "role": "system",
                        "content": (
                            "只返回一个合法 JSON 对象，必须严格符合下列 JSON Schema，"
                            "不要返回 Markdown 或额外文字："
                            f"{json.dumps(response_schema, ensure_ascii=False)}"
                        ),
                    },
                    *request_data["messages"],
                ]
        try:
            completion = await self._create_client(config).chat.completions.create(**request_data)
        except (APIConnectionError, APITimeoutError, RateLimitError, TimeoutError, ConnectionError) as exc:
            raise RetryableProviderError(str(exc)) from exc

        usage = completion.usage
        return ProviderCompletion(
            content=completion.choices[0].message.content or "",
            usage=Usage(
                input_tokens=usage.prompt_tokens if usage else 0,
                output_tokens=usage.completion_tokens if usage else 0,
            ),
        )

    async def stream(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
    ) -> AsyncIterator[str]:
        try:
            response = await self._create_client(config).chat.completions.create(
                model=config.provider_model,
                messages=[message.model_dump() for message in messages],
                stream=True,
                timeout=timeout_seconds,
            )
            async for chunk in response:
                choice = chunk.choices[0] if chunk.choices else None
                if choice and choice.delta.content:
                    yield choice.delta.content
        except (APIConnectionError, APITimeoutError, RateLimitError, TimeoutError, ConnectionError) as exc:
            raise RetryableProviderError(str(exc)) from exc
