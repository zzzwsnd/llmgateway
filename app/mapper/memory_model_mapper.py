from typing import Protocol

from app.core.config import get_gateway_config
from app.model.config import GatewayConfig
from app.model.entity import ModelConfig, ModelPrice
from app.model.enums import ModelEnum


class ModelMapper(Protocol):
    def find_config(self, model: ModelEnum) -> ModelConfig | None: ...

    def find_price(self, model: ModelEnum) -> ModelPrice | None: ...


class MemoryModelMapper:
    def __init__(self, gateway_config: GatewayConfig | None = None) -> None:
        config = gateway_config or get_gateway_config()
        self._configs = {
            model: ModelConfig(
                model=model,
                provider=route.provider,
                provider_model=route.provider_model,
                base_url=config.providers[route.provider].base_url,
                api_key_env=config.providers[route.provider].api_key_env,
                protocol=route.protocol,
                supports_structured_output=route.capabilities.structured_output,
                structured_output_mode=route.structured_output_mode,
            )
            for model, route in config.models.items()
        }
        self._prices = {
            model: ModelPrice(
                input_per_million=route.pricing.input_per_million,
                output_per_million=route.pricing.output_per_million,
            )
            for model, route in config.models.items()
        }

    def find_config(self, model: ModelEnum) -> ModelConfig | None:
        return self._configs.get(model)

    def find_price(self, model: ModelEnum) -> ModelPrice | None:
        return self._prices.get(model)
