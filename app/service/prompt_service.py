from string import Template

from app.core.errors import GatewayError
from app.dao.prompt_dao import PromptDao
from app.model.request import LLMRequest, Message, PromptSelection


class PromptService:
    def __init__(self, prompt_dao: PromptDao) -> None:
        self._prompt_dao = prompt_dao

    def render(self, selection: PromptSelection) -> Message:
        template = self._prompt_dao.get(selection.name, selection.version)
        if template is None:
            raise GatewayError("unknown_prompt_template", "Prompt 模板不存在", 400)
        try:
            content = Template(template.system_template).substitute(selection.variables)
        except KeyError as exc:
            raise GatewayError(
                "missing_prompt_variable",
                f"缺少 Prompt 变量: {exc.args[0]}",
                400,
            ) from exc
        return Message(role="system", content=content)

    def build_messages(self, request: LLMRequest) -> list[Message]:
        if request.prompt is None:
            return list(request.messages)
        return [self.render(request.prompt), *request.messages]
