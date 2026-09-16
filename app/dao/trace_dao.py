from app.mapper.memory_trace_mapper import TraceMapper
from app.model.entity import CallTrace


class TraceDao:
    def __init__(self, mapper: TraceMapper) -> None:
        self._mapper = mapper

    def save(self, trace: CallTrace) -> None:
        self._mapper.insert(trace)

    def list_all(self) -> list[CallTrace]:
        return self._mapper.find_all()
