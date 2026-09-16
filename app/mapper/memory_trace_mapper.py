from typing import Protocol

from app.model.entity import CallTrace


class TraceMapper(Protocol):
    def insert(self, trace: CallTrace) -> None: ...

    def find_all(self) -> list[CallTrace]: ...


class MemoryTraceMapper:
    def __init__(self) -> None:
        self._traces: list[CallTrace] = []

    def insert(self, trace: CallTrace) -> None:
        self._traces.append(trace)

    def find_all(self) -> list[CallTrace]:
        return list(self._traces)
