from app.mapper.memory_model_mapper import ModelMapper
from app.model.entity import ModelConfig, ModelPrice


class ModelDao:
    def __init__(self, mapper: ModelMapper) -> None:
        self._mapper = mapper

    def get_config(self, model: str) -> ModelConfig | None:
        return self._mapper.find_config(model)

    def get_price(self, model: str) -> ModelPrice | None:
        return self._mapper.find_price(model)
