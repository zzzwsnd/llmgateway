from app.model.config import GatewayConfig
from app.model.model_discovery import ModelListResponse, ModelMetadata


class ModelService:
    def __init__(self, gateway_config: GatewayConfig) -> None:
        self._gateway_config = gateway_config

    def list_models(self) -> ModelListResponse:
        return ModelListResponse(
            data=[
                ModelMetadata(
                    id=model,
                    owned_by=route.provider,
                    protocol=route.protocol,
                    capabilities=route.capabilities,
                )
                for model, route in self._gateway_config.models.items()
            ]
        )
