from enum import Enum


class ModelEnum(str, Enum):
    GENERAL_PRIMARY = "general-primary"
    GENERAL_BACKUP = "general-backup"
    GENERAL_RESPONSES = "general-responses"


class ModelProviderEnum(str, Enum):
    DEEPSEEK = "deepseek"
    OPENAI = "openai"


class LLMProtocolEnum(str, Enum):
    CHAT_COMPLETIONS = "chat_completions"
    RESPONSES = "responses"
