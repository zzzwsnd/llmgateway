import os
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import ValidationError

from app.core.errors import GatewayError
from app.model.config import GatewayConfig, ProviderConfig


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_gateway_config(path: Path | None = None) -> GatewayConfig:
    config_path = path or _resolve_gateway_config_path()
    try:
        raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        return GatewayConfig.model_validate(raw_config)
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise GatewayError("invalid_gateway_config", f"Invalid gateway configuration: {exc}", 500) from exc


@lru_cache(maxsize=1)
def get_gateway_config() -> GatewayConfig:
    return load_gateway_config()


def resolve_api_key(provider: ProviderConfig) -> str:
    api_key = os.getenv(provider.api_key_env)
    if not api_key:
        raise GatewayError("gateway_misconfigured", "Provider API key is not configured", 503)
    return api_key


def _resolve_gateway_config_path() -> Path:
    configured_path = os.getenv("GATEWAY_CONFIG_FILE")
    if configured_path:
        return Path(configured_path)

    local_path = _PROJECT_ROOT / "config" / "gateway.yaml"
    if local_path.is_file():
        return local_path
    return _PROJECT_ROOT / "config" / "gateway.example.yaml"
