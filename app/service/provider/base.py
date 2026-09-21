from collections.abc import AsyncIterator
from typing import Protocol

from app.model.config import ModelRouteConfig, ProviderConfig
from app.model.dto import ProviderCompletion, ProviderRequest, ProviderStreamChunk
from app.service.protocol_factory import ProtocolFactory


class ModelProvider(Protocol):
    async def complete(
        self, model: ModelRouteConfig, request: ProviderRequest
    ) -> ProviderCompletion:
        raise NotImplementedError

    def stream(
        self, model: ModelRouteConfig, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamChunk]:
        raise NotImplementedError


class BaseModelProvider:
    def __init__(self, config: ProviderConfig, protocol_factory: ProtocolFactory) -> None:
        self._config = config
        self._protocol_factory = protocol_factory

    async def complete(
        self, model: ModelRouteConfig, request: ProviderRequest
    ) -> ProviderCompletion:
        return await self._protocol_factory.get(model.protocol).complete(
            self._config, model, request
        )

    def stream(
        self, model: ModelRouteConfig, request: ProviderRequest
    ) -> AsyncIterator[ProviderStreamChunk]:
        return self._protocol_factory.get(model.protocol).stream(self._config, model, request)
