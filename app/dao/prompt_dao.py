from app.mapper.memory_prompt_mapper import PromptMapper
from app.model.entity import PromptTemplate


class PromptDao:
    def __init__(self, mapper: PromptMapper) -> None:
        self._mapper = mapper

    def get(self, name: str, version: str) -> PromptTemplate | None:
        return self._mapper.find(name, version)
