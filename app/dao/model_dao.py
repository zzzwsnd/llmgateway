from app.mapper.memory_model_mapper import ModelMapper
from app.model.entity import ModelConfig, ModelPrice
from app.model.enums import ModelEnum


class ModelDao:
    def __init__(self, mapper: ModelMapper) -> None:
        self._mapper = mapper

    def get_config(self, model: ModelEnum) -> ModelConfig | None:
        return self._mapper.find_config(model)

    def get_price(self, model: ModelEnum) -> ModelPrice | None:
        return self._mapper.find_price(model)
