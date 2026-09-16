from dataclasses import dataclass

from app.model.response import Usage


@dataclass(frozen=True)
class ProviderCompletion:
    content: str
    usage: Usage
