import asyncio
from typing import Protocol

from app.model.entity import CallAttempt, CallRecord, CallStatus, CallTrace


class TraceMapper(Protocol):
    async def insert_call(self, call: CallRecord) -> CallRecord: ...

    async def get_call(self, call_id: str) -> CallRecord | None: ...

    async def update_call(self, call: CallRecord) -> CallRecord: ...

    async def insert_attempt(self, attempt: CallAttempt) -> CallAttempt: ...

    async def update_attempt(self, attempt: CallAttempt) -> CallAttempt: ...

    async def find_attempts(self, call_id: str) -> list[CallAttempt]: ...

    async def find_all(self) -> list[CallTrace]: ...

    async def find_trace(self, call_id: str) -> CallTrace | None: ...


class MemoryTraceMapper:
    def __init__(self) -> None:
        self._calls: dict[str, CallRecord] = {}
        self._attempts: dict[str, CallAttempt] = {}
        self._lock = asyncio.Lock()

    async def insert_call(self, call: CallRecord) -> CallRecord:
        async with self._lock:
            if call.call_id in self._calls:
                raise RuntimeError("Call audit record already exists")
            stored = call.model_copy(deep=True)
            self._calls[call.call_id] = stored
            return stored.model_copy(deep=True)

    async def get_call(self, call_id: str) -> CallRecord | None:
        async with self._lock:
            call = self._calls.get(call_id)
            return call.model_copy(deep=True) if call is not None else None

    async def update_call(self, call: CallRecord) -> CallRecord:
        async with self._lock:
            current = self._calls.get(call.call_id)
            if current is None:
                raise RuntimeError("Call audit record does not exist")
            if current.status.is_terminal:
                if current.status == call.status:
                    return current.model_copy(deep=True)
                raise RuntimeError("A terminal call audit record is immutable")
            stored = call.model_copy(deep=True)
            self._calls[call.call_id] = stored
            return stored.model_copy(deep=True)

    async def insert_attempt(self, attempt: CallAttempt) -> CallAttempt:
        async with self._lock:
            if attempt.attempt_id in self._attempts:
                raise RuntimeError("Call attempt already exists")
            parent = self._calls.get(attempt.call_id)
            if parent is None:
                raise RuntimeError("Call audit record does not exist")
            if parent.status is not CallStatus.RUNNING:
                raise RuntimeError("Cannot start an attempt unless the call is running")
            next_number = 1 + max(
                (
                    item.attempt_no
                    for item in self._attempts.values()
                    if item.call_id == attempt.call_id
                ),
                default=0,
            )
            stored = attempt.model_copy(update={"attempt_no": next_number}, deep=True)
            self._attempts[stored.attempt_id] = stored
            return stored.model_copy(deep=True)

    async def update_attempt(self, attempt: CallAttempt) -> CallAttempt:
        async with self._lock:
            current = self._attempts.get(attempt.attempt_id)
            if current is None:
                raise RuntimeError("Call attempt does not exist")
            if current.status.is_terminal:
                if current.status == attempt.status:
                    return current.model_copy(deep=True)
                raise RuntimeError("A terminal call attempt is immutable")
            stored = attempt.model_copy(deep=True)
            self._attempts[attempt.attempt_id] = stored
            return stored.model_copy(deep=True)

    async def find_all(self) -> list[CallTrace]:
        async with self._lock:
            calls = sorted(
                self._calls.values(),
                key=lambda item: (item.created_at, item.call_id),
                reverse=True,
            )
            return [self._project_trace(call) for call in calls]

    async def find_trace(self, call_id: str) -> CallTrace | None:
        async with self._lock:
            call = self._calls.get(call_id)
            return self._project_trace(call) if call is not None else None

    async def find_attempts(self, call_id: str) -> list[CallAttempt]:
        async with self._lock:
            return [
                item.model_copy(deep=True)
                for item in sorted(
                    self._attempts.values(), key=lambda value: value.attempt_no
                )
                if item.call_id == call_id
            ]

    def _project_trace(self, call: CallRecord) -> CallTrace:
        attempts = sorted(
            (
                item
                for item in self._attempts.values()
                if item.call_id == call.call_id
            ),
            key=lambda item: item.attempt_no,
        )
        successful = [item for item in attempts if item.status.value == "success"]
        actual_model = successful[-1].model if successful else None
        if call.started_at is not None and call.finished_at is not None:
            latency_ms = max(
                0,
                int((call.finished_at - call.started_at).total_seconds() * 1000),
            )
        else:
            latency_ms = 0
        return CallTrace(
            request_id=call.call_id,
            timestamp=call.finished_at or call.updated_at,
            requested_model=call.requested_model,
            actual_model=actual_model,
            prompt_name=call.prompt_name,
            prompt_version=call.prompt_version,
            input_tokens=sum(item.input_tokens for item in attempts),
            output_tokens=sum(item.output_tokens for item in attempts),
            cost_usd=sum(item.cost_usd for item in attempts),
            latency_ms=latency_ms,
            attempts=len(attempts),
            status=call.status,
            error_code=call.error_code,
        )
