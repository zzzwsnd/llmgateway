from collections.abc import AsyncIterator
from typing import Protocol

from app.model.config import ModelRouteConfig, ProviderConfig
from app.model.dto import ProviderCompletion, ProviderRequest


class ProviderDao(Protocol):
    async def complete(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> ProviderCompletion:
        raise NotImplementedError

    def stream(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> AsyncIterator[str]:
        raise NotImplementedError


class ChatCompletionsMapper(Protocol):
    async def complete(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> ProviderCompletion:
        raise NotImplementedError

    def stream(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> AsyncIterator[str]:
        raise NotImplementedError


class ResponsesMapper(Protocol):
    async def complete(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> ProviderCompletion:
        raise NotImplementedError

    def stream(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> AsyncIterator[str]:
        raise NotImplementedError


class ChatCompletionsDao:
    def __init__(self, mapper: ChatCompletionsMapper) -> None:
        self._mapper = mapper

    async def complete(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> ProviderCompletion:
        return await self._mapper.complete(provider, model, request)

    def stream(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> AsyncIterator[str]:
        return self._mapper.stream(provider, model, request)


class ResponsesDao:
    def __init__(self, mapper: ResponsesMapper) -> None:
        self._mapper = mapper

    async def complete(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> ProviderCompletion:
        return await self._mapper.complete(provider, model, request)

    def stream(
        self,
        provider: ProviderConfig,
        model: ModelRouteConfig,
        request: ProviderRequest,
    ) -> AsyncIterator[str]:
        return self._mapper.stream(provider, model, request)
