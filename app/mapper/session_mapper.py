import asyncio
import os
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Protocol

from app.model.session import SessionStatus, StreamSession


class SessionMapper(Protocol):
    async def heartbeat(self, session_id: str, *, generation_id: str) -> bool: ...

    async def record_metrics(
        self,
        session_id: str,
        *,
        generation_id: str,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
    ) -> bool: ...

    async def insert_or_get(
        self, session: StreamSession
    ) -> tuple[StreamSession, bool]: ...

    async def get(self, session_id: str) -> StreamSession | None: ...

    async def get_by_idempotency(self, key: str) -> StreamSession | None: ...

    async def transition(
        self,
        session_id: str,
        *,
        generation_id: str,
        expected_statuses: set[SessionStatus],
        expected_version: int,
        target_status: SessionStatus,
        result_text: str | None = None,
        error_code: str | None = None,
        replay_degraded: bool | None = None,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
        total_duration_ms: int | None = None,
    ) -> StreamSession | None: ...

    async def list_reconcilable(
        self, updated_before: datetime, limit: int
    ) -> list[StreamSession]: ...


class MemorySessionMapper:
    def __init__(self) -> None:
        self._sessions: dict[str, StreamSession] = {}
        self._idempotency: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def heartbeat(self, session_id: str, *, generation_id: str) -> bool:
        async with self._lock:
            current = self._sessions.get(session_id)
            if (
                current is None
                or current.generation_id != generation_id
                or current.status.is_terminal
            ):
                return False
            self._sessions[session_id] = current.model_copy(
                update={"updated_at": datetime.now(timezone.utc), "version": current.version + 1},
                deep=True,
            )
            return True

    async def record_metrics(
        self,
        session_id: str,
        *,
        generation_id: str,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
    ) -> bool:
        async with self._lock:
            current = self._sessions.get(session_id)
            if (
                current is None
                or current.generation_id != generation_id
                or current.status.is_terminal
            ):
                return False
            self._sessions[session_id] = self._validated_update(
                current,
                {
                    "upstream_first_delta_latency_ms": (
                        current.upstream_first_delta_latency_ms
                        if current.upstream_first_delta_latency_ms is not None
                        else upstream_first_delta_latency_ms
                    ),
                    "first_content_latency_ms": (
                        current.first_content_latency_ms
                        if current.first_content_latency_ms is not None
                        else first_content_latency_ms
                    ),
                },
            )
            return True

    async def insert_or_get(
        self, session: StreamSession
    ) -> tuple[StreamSession, bool]:
        async with self._lock:
            if session.idempotency_key is not None:
                existing_id = self._idempotency.get(session.idempotency_key)
                if existing_id is not None:
                    return self._copy(self._sessions[existing_id]), False
            existing = self._sessions.get(session.session_id)
            if existing is not None:
                return self._copy(existing), False
            stored = self._copy(session)
            self._sessions[session.session_id] = stored
            if session.idempotency_key is not None:
                self._idempotency[session.idempotency_key] = session.session_id
            return self._copy(stored), True

    async def get(self, session_id: str) -> StreamSession | None:
        async with self._lock:
            session = self._sessions.get(session_id)
            return self._copy(session) if session is not None else None

    async def get_by_idempotency(self, key: str) -> StreamSession | None:
        async with self._lock:
            session_id = self._idempotency.get(key)
            return self._copy(self._sessions[session_id]) if session_id is not None else None

    async def transition(
        self,
        session_id: str,
        *,
        generation_id: str,
        expected_statuses: set[SessionStatus],
        expected_version: int,
        target_status: SessionStatus,
        result_text: str | None = None,
        error_code: str | None = None,
        replay_degraded: bool | None = None,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
        total_duration_ms: int | None = None,
    ) -> StreamSession | None:
        async with self._lock:
            current = self._sessions.get(session_id)
            if (
                current is None
                or current.status.is_terminal
                or current.generation_id != generation_id
                or current.version != expected_version
                or current.status not in expected_statuses
            ):
                return None

            now = datetime.now(timezone.utc)
            changes: dict[str, object] = {
                "status": target_status,
                "version": current.version + 1,
                "updated_at": now,
                "result_text": result_text,
                "error_code": error_code,
            }
            if replay_degraded is not None:
                changes["replay_degraded"] = replay_degraded
            if current.upstream_first_delta_latency_ms is None:
                changes["upstream_first_delta_latency_ms"] = (
                    upstream_first_delta_latency_ms
                )
            if current.first_content_latency_ms is None:
                changes["first_content_latency_ms"] = first_content_latency_ms
            if current.total_duration_ms is None:
                if total_duration_ms is None and target_status.is_terminal:
                    total_duration_ms = max(
                        0,
                        int((now - current.created_at).total_seconds() * 1000),
                    )
                changes["total_duration_ms"] = total_duration_ms
            if target_status is SessionStatus.RUNNING and current.started_at is None:
                changes["started_at"] = now
            if target_status in {SessionStatus.COMPLETED, SessionStatus.FAILED}:
                changes["completed_at"] = now
            if target_status is SessionStatus.CANCELLED:
                changes["cancelled_at"] = now
            updated = self._validated_update(current, changes)
            self._sessions[session_id] = updated
            return self._copy(updated)

    async def list_reconcilable(
        self, updated_before: datetime, limit: int
    ) -> list[StreamSession]:
        async with self._lock:
            values: Iterable[StreamSession] = self._sessions.values()
            matches = [
                self._copy(item)
                for item in values
                if not item.status.is_terminal and item.updated_at <= updated_before
            ]
            matches.sort(key=lambda item: item.updated_at)
            return matches[:limit]

    @staticmethod
    def _copy(session: StreamSession) -> StreamSession:
        return session.model_copy(deep=True)

    @staticmethod
    def _validated_update(
        session: StreamSession, changes: dict[str, object]
    ) -> StreamSession:
        values = session.model_dump(mode="python")
        values.update(changes)
        return StreamSession.model_validate(values)


class PostgresSessionMapper:
    def __init__(self, dsn_env: str) -> None:
        self._dsn_env = dsn_env
        self._schema_ready = False
        self._schema_lock = asyncio.Lock()

    async def heartbeat(self, session_id: str, *, generation_id: str) -> bool:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                """
                UPDATE gateway_stream_sessions
                SET updated_at = %s, version = version + 1
                WHERE session_id = %s AND generation_id = %s
                  AND status IN ('pending', 'running', 'cancelling')
                RETURNING session_id
                """,
                (datetime.now(timezone.utc), session_id, generation_id),
            )
            await connection.commit()
            return row is not None

    async def record_metrics(
        self,
        session_id: str,
        *,
        generation_id: str,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
    ) -> bool:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                """
                UPDATE gateway_stream_sessions
                SET upstream_first_delta_latency_ms = COALESCE(
                        upstream_first_delta_latency_ms,
                        %(upstream_first_delta_latency_ms)s
                    ),
                    first_content_latency_ms = COALESCE(
                        first_content_latency_ms,
                        %(first_content_latency_ms)s
                    )
                WHERE session_id = %(session_id)s
                  AND generation_id = %(generation_id)s
                  AND status NOT IN ('completed', 'failed', 'cancelled')
                RETURNING session_id
                """,
                {
                    "session_id": session_id,
                    "generation_id": generation_id,
                    "upstream_first_delta_latency_ms": (
                        upstream_first_delta_latency_ms
                    ),
                    "first_content_latency_ms": first_content_latency_ms,
                },
            )
            await connection.commit()
            return row is not None

    async def insert_or_get(
        self, session: StreamSession
    ) -> tuple[StreamSession, bool]:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                """
                INSERT INTO gateway_stream_sessions (
                    session_id, idempotency_key, request_fingerprint, interface,
                    requested_model, owner_id, status, generation_id, version,
                    result_text, error_code, replay_degraded, created_at, updated_at,
                    started_at, completed_at, cancelled_at,
                    upstream_first_delta_latency_ms, first_content_latency_ms,
                    total_duration_ms
                ) VALUES (
                    %(session_id)s, %(idempotency_key)s, %(request_fingerprint)s,
                    %(interface)s, %(requested_model)s, %(owner_id)s, %(status)s,
                    %(generation_id)s, %(version)s, %(result_text)s, %(error_code)s,
                    %(replay_degraded)s, %(created_at)s, %(updated_at)s,
                    %(started_at)s, %(completed_at)s, %(cancelled_at)s,
                    %(upstream_first_delta_latency_ms)s,
                    %(first_content_latency_ms)s, %(total_duration_ms)s
                )
                ON CONFLICT DO NOTHING
                RETURNING *
                """,
                self._parameters(session),
            )
            if row is not None:
                await connection.commit()
                return self._from_row(row), True

            if session.idempotency_key is not None:
                row = await self._fetchone(
                    connection,
                    "SELECT * FROM gateway_stream_sessions WHERE idempotency_key = %s",
                    (session.idempotency_key,),
                )
            else:
                row = await self._fetchone(
                    connection,
                    "SELECT * FROM gateway_stream_sessions WHERE session_id = %s",
                    (session.session_id,),
                )
            await connection.commit()
            if row is None:
                raise RuntimeError("session insert did not return or resolve a row")
            return self._from_row(row), False

    async def get(self, session_id: str) -> StreamSession | None:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                "SELECT * FROM gateway_stream_sessions WHERE session_id = %s",
                (session_id,),
            )
            return self._from_row(row) if row is not None else None

    async def get_by_idempotency(self, key: str) -> StreamSession | None:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                "SELECT * FROM gateway_stream_sessions WHERE idempotency_key = %s",
                (key,),
            )
            return self._from_row(row) if row is not None else None

    async def transition(
        self,
        session_id: str,
        *,
        generation_id: str,
        expected_statuses: set[SessionStatus],
        expected_version: int,
        target_status: SessionStatus,
        result_text: str | None = None,
        error_code: str | None = None,
        replay_degraded: bool | None = None,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
        total_duration_ms: int | None = None,
    ) -> StreamSession | None:
        await self._ensure_schema()
        now = datetime.now(timezone.utc)
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                """
                UPDATE gateway_stream_sessions
                SET status = %(target_status)s,
                    version = version + 1,
                    updated_at = %(now)s,
                    result_text = %(result_text)s,
                    error_code = %(error_code)s,
                    replay_degraded = COALESCE(%(replay_degraded)s, replay_degraded),
                    upstream_first_delta_latency_ms = COALESCE(
                        upstream_first_delta_latency_ms,
                        %(upstream_first_delta_latency_ms)s
                    ),
                    first_content_latency_ms = COALESCE(
                        first_content_latency_ms, %(first_content_latency_ms)s
                    ),
                    total_duration_ms = COALESCE(
                        total_duration_ms,
                        %(total_duration_ms)s,
                        CASE
                            WHEN %(target_status)s IN ('completed', 'failed', 'cancelled')
                            THEN GREATEST(
                                0,
                                FLOOR(
                                    EXTRACT(EPOCH FROM (%(now)s - created_at)) * 1000
                                )::BIGINT
                            )
                            ELSE NULL
                        END
                    ),
                    started_at = CASE
                        WHEN %(target_status)s = 'running' AND started_at IS NULL
                        THEN %(now)s ELSE started_at END,
                    completed_at = CASE
                        WHEN %(target_status)s IN ('completed', 'failed')
                        THEN %(now)s ELSE completed_at END,
                    cancelled_at = CASE
                        WHEN %(target_status)s = 'cancelled'
                        THEN %(now)s ELSE cancelled_at END
                WHERE session_id = %(session_id)s
                  AND generation_id = %(generation_id)s
                  AND version = %(expected_version)s
                  AND status = ANY(%(expected_statuses)s)
                  AND status NOT IN ('completed', 'failed', 'cancelled')
                RETURNING *
                """,
                {
                    "target_status": target_status.value,
                    "now": now,
                    "result_text": result_text,
                    "error_code": error_code,
                    "replay_degraded": replay_degraded,
                    "upstream_first_delta_latency_ms": (
                        upstream_first_delta_latency_ms
                    ),
                    "first_content_latency_ms": first_content_latency_ms,
                    "total_duration_ms": total_duration_ms,
                    "session_id": session_id,
                    "generation_id": generation_id,
                    "expected_version": expected_version,
                    "expected_statuses": [item.value for item in expected_statuses],
                },
            )
            await connection.commit()
            return self._from_row(row) if row is not None else None

    async def list_reconcilable(
        self, updated_before: datetime, limit: int
    ) -> list[StreamSession]:
        await self._ensure_schema()
        async with await self._connect() as connection:
            rows = await self._fetchall(
                connection,
                """
                SELECT * FROM gateway_stream_sessions
                WHERE status IN ('pending', 'running', 'cancelling')
                  AND updated_at <= %s
                ORDER BY updated_at
                LIMIT %s
                """,
                (updated_before, limit),
            )
            return [self._from_row(row) for row in rows]

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        async with self._schema_lock:
            if self._schema_ready:
                return
            async with await self._connect() as connection:
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gateway_stream_sessions (
                        session_id TEXT PRIMARY KEY,
                        idempotency_key TEXT UNIQUE,
                        request_fingerprint TEXT NOT NULL,
                        interface TEXT NOT NULL,
                        requested_model TEXT NOT NULL,
                        owner_id TEXT,
                        status TEXT NOT NULL,
                        generation_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        result_text TEXT,
                        error_code TEXT,
                        replay_degraded BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL,
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ,
                        cancelled_at TIMESTAMPTZ,
                        upstream_first_delta_latency_ms BIGINT CHECK (
                            upstream_first_delta_latency_ms >= 0
                        ),
                        first_content_latency_ms BIGINT CHECK (
                            first_content_latency_ms >= 0
                        ),
                        total_duration_ms BIGINT CHECK (total_duration_ms >= 0)
                    )
                    """
                )
                await connection.execute(
                    """
                    ALTER TABLE gateway_stream_sessions
                        ADD COLUMN IF NOT EXISTS upstream_first_delta_latency_ms
                            BIGINT CHECK (upstream_first_delta_latency_ms >= 0),
                        ADD COLUMN IF NOT EXISTS first_content_latency_ms
                            BIGINT CHECK (first_content_latency_ms >= 0),
                        ADD COLUMN IF NOT EXISTS total_duration_ms
                            BIGINT CHECK (total_duration_ms >= 0)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS gateway_stream_sessions_reconcile_idx
                    ON gateway_stream_sessions (status, updated_at)
                    WHERE status IN ('pending', 'running', 'cancelling')
                    """
                )
                await connection.commit()
            self._schema_ready = True

    async def _connect(self):
        from psycopg import AsyncConnection

        dsn = os.getenv(self._dsn_env)
        if not dsn:
            raise RuntimeError("PostgreSQL session store is not configured")
        return await AsyncConnection.connect(dsn)

    @staticmethod
    async def _fetchone(connection, query: str, parameters: object):
        from psycopg.rows import dict_row

        async with connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query, parameters)
            return await cursor.fetchone()

    @staticmethod
    async def _fetchall(connection, query: str, parameters: object):
        from psycopg.rows import dict_row

        async with connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query, parameters)
            return await cursor.fetchall()

    @staticmethod
    def _parameters(session: StreamSession) -> dict[str, object]:
        values = session.model_dump(mode="python")
        values["interface"] = session.interface.value
        values["requested_model"] = session.requested_model.value
        values["status"] = session.status.value
        return values

    @staticmethod
    def _from_row(row: dict[str, object]) -> StreamSession:
        return StreamSession.model_validate(row)
