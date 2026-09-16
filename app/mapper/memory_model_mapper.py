import os
from collections.abc import Mapping
from typing import Protocol

from app.model.entity import ModelConfig, ModelPrice


class ModelMapper(Protocol):
    def find_config(self, model: str) -> ModelConfig | None: ...

    def find_price(self, model: str) -> ModelPrice | None: ...


class MemoryModelMapper:
    def __init__(
        self,
        configs: Mapping[str, ModelConfig] | None = None,
        prices: Mapping[str, ModelPrice] | None = None,
    ) -> None:
        self._configs = dict(configs) if configs is not None else self._default_configs()
        self._prices = dict(prices) if prices is not None else self._default_prices()

    def find_config(self, model: str) -> ModelConfig | None:
        return self._configs.get(model)

    def find_price(self, model: str) -> ModelPrice | None:
        return self._prices.get(model)

    @staticmethod
    def _default_configs() -> dict[str, ModelConfig]:
        return {
            "general-primary": ModelConfig(
                provider_model=os.getenv("PRIMARY_PROVIDER_MODEL", "deepseek-v4-flash"),
                base_url=os.getenv("PRIMARY_BASE_URL", "https://api.deepseek.com"),
                api_key_env="DEEPSEEK_API_KEY",
                supports_structured_output=True,
                structured_output_mode="json_object",
            ),
            "general-backup": ModelConfig(
                provider_model=os.getenv("BACKUP_PROVIDER_MODEL", "deepseek-chat"),
                base_url=os.getenv("BACKUP_BASE_URL", "https://api.deepseek.com"),
                api_key_env="DEEPSEEK_BACKUP_API_KEY",
                supports_structured_output=True,
                structured_output_mode="json_object",
            ),
        }

    @staticmethod
    def _default_prices() -> dict[str, ModelPrice]:
        return {
            "general-primary": ModelPrice(input_per_million=1.0, output_per_million=4.0),
            "general-backup": ModelPrice(input_per_million=0.8, output_per_million=3.2),
        }
