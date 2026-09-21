from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Protocol

from app.model.config import RedisConnectionConfig
from app.model.session import RuntimeEvent


class RuntimeMapper(Protocol):
    async def close(self) -> None: ...

    async def append_event(self, session_id: str, event: RuntimeEvent) -> RuntimeEvent: ...

    async def read_events(
        self,
        session_id: str,
        *,
        after: str | None,
        block_milliseconds: int = 0,
        count: int = 100,
    ) -> list[RuntimeEvent]: ...

    async def has_events(self, session_id: str) -> bool: ...

    async def latest_event_id(self, session_id: str) -> str | None: ...

    async def expire_events(self, session_id: str, ttl_seconds: int) -> None: ...


class MemoryRuntimeMapper:
    def __init__(self) -> None:
        self._events: dict[str, list[RuntimeEvent]] = {}
        self._event_expiry: dict[str, float] = {}
        self._event_sequence = 0
        self._lock = asyncio.Lock()
        self._event_available = asyncio.Condition()

    async def close(self) -> None:
        return None

    async def append_event(self, session_id: str, event: RuntimeEvent) -> RuntimeEvent:
        async with self._lock:
            self._drop_expired_events(session_id)
            self._event_sequence += 1
            stored = event.model_copy(
                update={"event_id": f"{self._event_sequence}-0"}, deep=True
            )
            self._events.setdefault(session_id, []).append(stored)
        async with self._event_available:
            self._event_available.notify_all()
        return stored.model_copy(deep=True)

    async def read_events(
        self,
        session_id: str,
        *,
        after: str | None,
        block_milliseconds: int = 0,
        count: int = 100,
    ) -> list[RuntimeEvent]:
        events = await self._read_available(session_id, after, count)
        if events or block_milliseconds <= 0:
            return events
        try:
            async with self._event_available:
                await asyncio.wait_for(
                    self._event_available.wait(), block_milliseconds / 1000
                )
        except TimeoutError:
            return []
        return await self._read_available(session_id, after, count)

    async def _read_available(
        self, session_id: str, after: str | None, count: int
    ) -> list[RuntimeEvent]:
        async with self._lock:
            self._drop_expired_events(session_id)
            minimum = self._event_number(after) if after is not None else -1
            return [
                event.model_copy(deep=True)
                for event in self._events.get(session_id, [])
                if self._event_number(event.event_id) > minimum
            ][:count]

    async def has_events(self, session_id: str) -> bool:
        async with self._lock:
            self._drop_expired_events(session_id)
            return bool(self._events.get(session_id))

    async def latest_event_id(self, session_id: str) -> str | None:
        async with self._lock:
            self._drop_expired_events(session_id)
            events = self._events.get(session_id, [])
            return events[-1].event_id if events else None

    async def expire_events(self, session_id: str, ttl_seconds: int) -> None:
        async with self._lock:
            if session_id in self._events:
                self._event_expiry[session_id] = time.monotonic() + ttl_seconds

    def _drop_expired_events(self, session_id: str) -> None:
        expires_at = self._event_expiry.get(session_id)
        if expires_at is not None and expires_at <= time.monotonic():
            self._event_expiry.pop(session_id, None)
            self._events.pop(session_id, None)

    @staticmethod
    def _event_number(event_id: str | None) -> int:
        if event_id is None:
            return -1
        return int(event_id.split("-", maxsplit=1)[0])


class RedisRuntimeMapper:
    def __init__(
        self,
        config: RedisConnectionConfig,
        key_prefix: str = "gateway",
        event_ttl_seconds: int = 3600,
    ) -> None:
        self._config = config
        self._prefix = key_prefix.rstrip(":")
        self._event_ttl_seconds = max(event_ttl_seconds, 86400)
        self._client_instance = None

    async def close(self) -> None:
        if self._client_instance is not None:
            try:
                await self._client_instance.aclose()
            finally:
                self._client_instance = None

    async def append_event(self, session_id: str, event: RuntimeEvent) -> RuntimeEvent:
        data = event.model_dump(mode="json", exclude={"event_id"})
        key = self._events_key(session_id)
        async with self._client().pipeline(transaction=True) as pipeline:
            pipeline.xadd(key, {"event": json.dumps(data, separators=(",", ":"))})
            pipeline.expire(key, self._event_ttl_seconds)
            event_id = (await pipeline.execute())[0]
        return event.model_copy(update={"event_id": event_id}, deep=True)

    async def read_events(
        self,
        session_id: str,
        *,
        after: str | None,
        block_milliseconds: int = 0,
        count: int = 100,
    ) -> list[RuntimeEvent]:
        key = self._events_key(session_id)
        if block_milliseconds > 0:
            streams = await self._client().xread(
                {key: after or "0-0"},
                block=block_milliseconds,
                count=count,
            )
            rows = streams[0][1] if streams else []
        else:
            minimum = f"({after}" if after is not None else "-"
            rows = await self._client().xrange(key, min=minimum, max="+", count=count)
        return [self._decode_event(event_id, fields) for event_id, fields in rows]

    async def has_events(self, session_id: str) -> bool:
        return bool(await self._client().exists(self._events_key(session_id)))

    async def latest_event_id(self, session_id: str) -> str | None:
        rows = await self._client().xrevrange(
            self._events_key(session_id), max="+", min="-", count=1
        )
        return rows[0][0] if rows else None

    async def expire_events(self, session_id: str, ttl_seconds: int) -> None:
        await self._client().expire(self._events_key(session_id), ttl_seconds)

    def _client(self):
        if self._client_instance is None:
            from redis.asyncio import Redis

            self._client_instance = Redis(
                host=_required_environment(
                    self._config.host_env,
                    "Redis session runtime is not configured",
                ),
                port=_required_environment_int(
                    self._config.port_env,
                    "Redis port must be an integer between 1 and 65535",
                    minimum=1,
                    maximum=65535,
                ),
                password=_required_environment(
                    self._config.password_env,
                    "Redis session runtime is not configured",
                ),
                db=_required_environment_int(
                    self._config.database_env,
                    "Redis database must be a non-negative integer",
                    minimum=0,
                ),
                decode_responses=True,
            )
        return self._client_instance

    def _events_key(self, session_id: str) -> str:
        return f"{self._prefix}:session:{session_id}:events"

    @staticmethod
    def _decode_event(event_id: str, fields: dict[str, str]) -> RuntimeEvent:
        raw = fields["event"]
        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)
        data["event_id"] = event_id
        return RuntimeEvent.model_validate(data)


def _required_environment(name: str, error_message: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(error_message)
    return value


def _required_environment_int(
    name: str,
    error_message: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    raw_value = _required_environment(name, error_message)
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(error_message) from exc
    if value < minimum or maximum is not None and value > maximum:
        raise RuntimeError(error_message)
    return value
