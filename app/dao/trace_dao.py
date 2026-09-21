from app.mapper.memory_trace_mapper import TraceMapper
from app.model.entity import CallAttempt, CallRecord, CallTrace


class TraceDao:
    def __init__(self, mapper: TraceMapper) -> None:
        self._mapper = mapper

    async def create_call(self, call: CallRecord) -> CallRecord:
        return await self._mapper.insert_call(call)

    async def get_call(self, call_id: str) -> CallRecord | None:
        return await self._mapper.get_call(call_id)

    async def save_call(self, call: CallRecord) -> CallRecord:
        return await self._mapper.update_call(call)

    async def create_attempt(self, attempt: CallAttempt) -> CallAttempt:
        return await self._mapper.insert_attempt(attempt)

    async def save_attempt(self, attempt: CallAttempt) -> CallAttempt:
        return await self._mapper.update_attempt(attempt)

    async def list_attempts(self, call_id: str) -> list[CallAttempt]:
        return await self._mapper.find_attempts(call_id)

    async def list_all(self) -> list[CallTrace]:
        return await self._mapper.find_all()

    async def get_trace(self, call_id: str) -> CallTrace | None:
        return await self._mapper.find_trace(call_id)
