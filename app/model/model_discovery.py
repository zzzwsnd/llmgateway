from typing import Literal

from pydantic import BaseModel, ConfigDict

from app.model.config import ModelCapabilities
from app.model.enums import LLMProtocolEnum, ModelEnum, ModelProviderEnum


class ModelMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: ModelEnum
    object: Literal["model"] = "model"
    owned_by: ModelProviderEnum
    protocol: LLMProtocolEnum
    capabilities: ModelCapabilities


class ModelListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    object: Literal["list"] = "list"
    data: list[ModelMetadata]
