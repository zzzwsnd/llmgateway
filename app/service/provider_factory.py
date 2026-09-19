from collections.abc import Mapping

from app.core.errors import GatewayError
from app.model.enums import ModelProviderEnum
from app.service.provider.base import ModelProvider


class ProviderFactory:
    def __init__(self, providers: Mapping[ModelProviderEnum, ModelProvider]) -> None:
        if any(not isinstance(provider, ModelProviderEnum) for provider in providers):
            raise TypeError("provider registrations must use ModelProviderEnum keys")
        self._providers: dict[ModelProviderEnum, ModelProvider] = dict(providers)

    def get(self, provider: ModelProviderEnum) -> ModelProvider:
        implementation = self._providers.get(provider)
        if implementation is None:
            raise GatewayError("provider_not_registered", "Provider is not registered", 500)
        return implementation
