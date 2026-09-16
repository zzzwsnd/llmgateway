from collections.abc import AsyncIterator
from typing import Any

from app.mapper.openai_mapper import ProviderMapper
from app.model.dto import ProviderCompletion
from app.model.entity import ModelConfig
from app.model.request import Message


class ProviderDao:
    def __init__(self, mapper: ProviderMapper) -> None:
        self._mapper = mapper

    async def complete(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
        response_schema: dict[str, Any] | None,
    ) -> ProviderCompletion:
        return await self._mapper.complete(config, messages, timeout_seconds, response_schema)

    def stream(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
    ) -> AsyncIterator[str]:
        return self._mapper.stream(config, messages, timeout_seconds)
