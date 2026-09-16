from collections.abc import Mapping
from typing import Protocol

from app.model.entity import PromptTemplate


class PromptMapper(Protocol):
    def find(self, name: str, version: str) -> PromptTemplate | None: ...


class MemoryPromptMapper:
    def __init__(
        self,
        templates: Mapping[tuple[str, str], PromptTemplate] | None = None,
    ) -> None:
        self._templates = dict(templates) if templates is not None else self._default_templates()

    def find(self, name: str, version: str) -> PromptTemplate | None:
        return self._templates.get((name, version))

    @staticmethod
    def _default_templates() -> dict[tuple[str, str], PromptTemplate]:
        template = PromptTemplate(
            name="knowledge_decision",
            version="v1",
            system_template=(
                "你是${product_name}的知识库决策器。资料不足时搜索，"
                "资料充分时结束回答。不得编造制度内容。"
            ),
        )
        return {(template.name, template.version): template}
