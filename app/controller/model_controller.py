from fastapi import APIRouter

from app.model.model_discovery import ModelListResponse
from app.service.model_service import ModelService


def create_model_router(service: ModelService) -> APIRouter:
    router = APIRouter()

    @router.get("/v1/models", response_model=ModelListResponse)
    def list_models() -> ModelListResponse:
        return service.list_models()

    return router
