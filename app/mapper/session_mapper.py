import asyncio
import os
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Protocol

from app.mapper.call_schema import CALL_SCHEMA_STATEMENTS, LEGACY_AUDIT_MIGRATION
from app.model.config import PostgresConnectionConfig
from app.model.session import SessionStatus, StreamSession


class SessionMapper(Protocol):
    async def heartbeat(self, session_id: str) -> bool: ...

    async def insert_or_get(
        self, session: StreamSession
    ) -> tuple[StreamSession, bool]: ...

    async def get(self, session_id: str) -> StreamSession | None: ...

    async def get_by_idempotency(self, key: str) -> StreamSession | None: ...

    async def transition(
        self,
        session_id: str,
        *,
        expected_statuses: set[SessionStatus],
        target_status: SessionStatus,
        result_text: str | None = None,
        error_code: str | None = None,
        replay_degraded: bool | None = None,
        actual_model: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        cost_usd: float | None = None,
        latency_ms: int | None = None,
        attempts: int | None = None,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
        total_duration_ms: int | None = None,
    ) -> StreamSession | None: ...

    async def reconcile(
        self,
        session_id: str,
        *,
        updated_before: datetime,
    ) -> StreamSession | None: ...

    async def list_reconcilable(
        self, updated_before: datetime, limit: int
    ) -> list[StreamSession]: ...


class MemorySessionMapper:
    def __init__(self) -> None:
        self._sessions: dict[str, StreamSession] = {}
        self._idempotency: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def heartbeat(self, session_id: str) -> bool:
        async with self._lock:
            current = self._sessions.get(session_id)
            if (
                current is None
                or current.status.is_terminal
            ):
                return False
            self._sessions[session_id] = current.model_copy(
                update={"updated_at": datetime.now(timezone.utc)},
                deep=True,
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
            stored = session.model_copy(update={"result_text": None}, deep=True)
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
            return self._copy(self._sessions[session_id]) if session_id else None

    async def transition(
        self,
        session_id: str,
        *,
        expected_statuses: set[SessionStatus],
        target_status: SessionStatus,
        result_text: str | None = None,
        error_code: str | None = None,
        replay_degraded: bool | None = None,
        actual_model: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        cost_usd: float | None = None,
        latency_ms: int | None = None,
        attempts: int | None = None,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
        total_duration_ms: int | None = None,
    ) -> StreamSession | None:
        del result_text, actual_model, input_tokens, output_tokens, cost_usd
        del latency_ms, attempts, upstream_first_delta_latency_ms
        del first_content_latency_ms
        async with self._lock:
            current = self._sessions.get(session_id)
            if (
                current is None
                or current.status.is_terminal
                or current.status not in expected_statuses
            ):
                return None
            now = datetime.now(timezone.utc)
            changes: dict[str, object] = {
                "status": target_status,
                "updated_at": now,
                "result_text": None,
                "error_code": error_code,
            }
            if replay_degraded is not None:
                changes["replay_degraded"] = replay_degraded
            if target_status is SessionStatus.RUNNING and current.started_at is None:
                changes["started_at"] = now
            if target_status in {SessionStatus.COMPLETED, SessionStatus.FAILED}:
                changes["completed_at"] = now
            if target_status is SessionStatus.CANCELLED:
                changes["cancelled_at"] = now
            if target_status.is_terminal:
                changes["total_duration_ms"] = (
                    total_duration_ms
                    if total_duration_ms is not None
                    else max(0, int((now - current.created_at).total_seconds() * 1000))
                )
            updated = self._validated_update(current, changes)
            self._sessions[session_id] = updated
            return self._copy(updated)

    async def reconcile(
        self,
        session_id: str,
        *,
        updated_before: datetime,
    ) -> StreamSession | None:
        async with self._lock:
            current = self._sessions.get(session_id)
            if (
                current is None
                or current.status.is_terminal
                or current.updated_at > updated_before
            ):
                return None
            now = datetime.now(timezone.utc)
            cancelled = current.status is SessionStatus.CANCELLING
            target = (
                SessionStatus.CANCELLED if cancelled else SessionStatus.FAILED
            )
            error_code = (
                None
                if cancelled
                else (
                    "session_start_timeout"
                    if current.status is SessionStatus.PENDING
                    else "producer_lost"
                )
            )
            changes: dict[str, object] = {
                "status": target,
                "updated_at": now,
                "error_code": error_code,
                "result_text": None,
                "total_duration_ms": max(
                    0, int((now - current.created_at).total_seconds() * 1000)
                ),
            }
            if cancelled:
                changes["cancelled_at"] = now
            else:
                changes["completed_at"] = now
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
    _SESSION_SELECT = """
        SELECT calls.*,
               successful.model AS actual_model,
               COALESCE(aggregates.input_tokens, 0) AS input_tokens,
               COALESCE(aggregates.output_tokens, 0) AS output_tokens,
               COALESCE(aggregates.cost_usd, 0) AS cost_usd,
               COALESCE(aggregates.attempts, 0) AS attempts,
               aggregates.first_delta_latency_ms
        FROM calls
        LEFT JOIN LATERAL (
            SELECT SUM(input_tokens) AS input_tokens,
                   SUM(output_tokens) AS output_tokens,
                   SUM(cost_usd) AS cost_usd,
                   COUNT(*) AS attempts,
                   MIN(first_delta_latency_ms) AS first_delta_latency_ms
            FROM call_attempts
            WHERE call_attempts.call_id = calls.call_id
        ) aggregates ON TRUE
        LEFT JOIN LATERAL (
            SELECT model
            FROM call_attempts
            WHERE call_attempts.call_id = calls.call_id
              AND status = 'success'
            ORDER BY attempt_no DESC
            LIMIT 1
        ) successful ON TRUE
    """

    def __init__(self, config: PostgresConnectionConfig) -> None:
        self._config = config
        self._schema_ready = False
        self._schema_lock = asyncio.Lock()

    async def heartbeat(self, session_id: str) -> bool:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                """
                UPDATE calls
                SET updated_at = %s
                WHERE call_id = %s AND stream = TRUE
                  AND status IN ('pending', 'running', 'cancelling')
                RETURNING call_id
                """,
                (datetime.now(timezone.utc), session_id),
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
                INSERT INTO calls (
                    call_id, retry_of_call_id, idempotency_key,
                    request_fingerprint, interface, stream, owner_id,
                    requested_model, prompt_name, prompt_version, status,
                    error_code, replay_degraded, created_at, updated_at,
                    started_at, finished_at
                ) VALUES (
                    %(call_id)s, %(retry_of_call_id)s, %(idempotency_key)s,
                    %(request_fingerprint)s, %(interface)s, TRUE, %(owner_id)s,
                    %(requested_model)s, %(prompt_name)s, %(prompt_version)s,
                    %(status)s, %(error_code)s, %(replay_degraded)s,
                    %(created_at)s, %(updated_at)s,
                    %(started_at)s, %(finished_at)s
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
                query = self._SESSION_SELECT + " WHERE calls.idempotency_key = %s"
                parameters = (session.idempotency_key,)
            else:
                query = self._SESSION_SELECT + " WHERE calls.call_id = %s"
                parameters = (session.session_id,)
            row = await self._fetchone(connection, query, parameters)
            await connection.commit()
            if row is None:
                raise RuntimeError("session insert did not return or resolve a row")
            return self._from_row(row), False

    async def get(self, session_id: str) -> StreamSession | None:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                self._SESSION_SELECT
                + " WHERE calls.call_id = %s AND calls.stream = TRUE",
                (session_id,),
            )
        return self._from_row(row) if row is not None else None

    async def get_by_idempotency(self, key: str) -> StreamSession | None:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                self._SESSION_SELECT
                + " WHERE calls.idempotency_key = %s AND calls.stream = TRUE",
                (key,),
            )
        return self._from_row(row) if row is not None else None

    async def transition(
        self,
        session_id: str,
        *,
        expected_statuses: set[SessionStatus],
        target_status: SessionStatus,
        result_text: str | None = None,
        error_code: str | None = None,
        replay_degraded: bool | None = None,
        actual_model: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        cost_usd: float | None = None,
        latency_ms: int | None = None,
        attempts: int | None = None,
        upstream_first_delta_latency_ms: int | None = None,
        first_content_latency_ms: int | None = None,
        total_duration_ms: int | None = None,
    ) -> StreamSession | None:
        del result_text, actual_model, input_tokens, output_tokens, cost_usd
        del latency_ms, attempts, upstream_first_delta_latency_ms
        del first_content_latency_ms, total_duration_ms
        await self._ensure_schema()
        now = datetime.now(timezone.utc)
        target = _database_status(target_status)
        expected = [_database_status(item) for item in expected_statuses]
        async with await self._connect() as connection:
            changed = await self._fetchone(
                connection,
                """
                UPDATE calls
                SET status = %(target_status)s,
                    updated_at = %(now)s,
                    error_code = %(error_code)s,
                    replay_degraded = COALESCE(
                        %(replay_degraded)s, replay_degraded
                    ),
                    started_at = CASE
                        WHEN %(target_status)s = 'running' AND started_at IS NULL
                        THEN %(now)s ELSE started_at END,
                    finished_at = CASE
                        WHEN %(target_status)s IN ('success', 'failed', 'cancelled')
                        THEN %(now)s ELSE finished_at END
                WHERE call_id = %(session_id)s
                  AND stream = TRUE
                  AND status = ANY(%(expected_statuses)s)
                  AND status NOT IN ('success', 'failed', 'cancelled')
                RETURNING call_id
                """,
                {
                    "target_status": target,
                    "now": now,
                    "error_code": error_code,
                    "replay_degraded": replay_degraded,
                    "session_id": session_id,
                    "expected_statuses": expected,
                },
            )
            if changed is None:
                await connection.commit()
                return None
            if target_status in {SessionStatus.FAILED, SessionStatus.CANCELLED}:
                await self._finalize_running_attempts(
                    connection,
                    session_id=session_id,
                    status=target_status,
                    error_code=error_code,
                    finished_at=now,
                )
            row = await self._fetchone(
                connection,
                self._SESSION_SELECT + " WHERE calls.call_id = %s",
                (session_id,),
            )
            await connection.commit()
        return self._from_row(row)

    async def reconcile(
        self,
        session_id: str,
        *,
        updated_before: datetime,
    ) -> StreamSession | None:
        await self._ensure_schema()
        async with await self._connect() as connection:
            current = await self._fetchone(
                connection,
                """
                SELECT status, updated_at
                FROM calls
                WHERE call_id = %s AND stream = TRUE
                FOR UPDATE
                """,
                (session_id,),
            )
            if (
                current is None
                or str(current["status"]) in {"success", "failed", "cancelled"}
                or current["updated_at"] > updated_before
            ):
                await connection.commit()
                return None
            current_status = str(current["status"])
            cancelled = current_status == "cancelling"
            target_status = "cancelled" if cancelled else "failed"
            error_code = (
                None
                if cancelled
                else (
                    "session_start_timeout"
                    if current_status == "pending"
                    else "producer_lost"
                )
            )
            now = datetime.now(timezone.utc)
            await connection.execute(
                """
                UPDATE calls
                SET status = %s,
                    updated_at = %s,
                    finished_at = %s,
                    error_code = %s
                WHERE call_id = %s
                """,
                (target_status, now, now, error_code, session_id),
            )
            await self._finalize_running_attempts(
                connection,
                session_id=session_id,
                status=(
                    SessionStatus.CANCELLED
                    if cancelled
                    else SessionStatus.FAILED
                ),
                error_code=error_code,
                finished_at=now,
            )
            row = await self._fetchone(
                connection,
                self._SESSION_SELECT + " WHERE calls.call_id = %s",
                (session_id,),
            )
            await connection.commit()
        return self._from_row(row)

    @staticmethod
    async def _finalize_running_attempts(
        connection,
        *,
        session_id: str,
        status: SessionStatus,
        error_code: str | None,
        finished_at: datetime,
    ) -> None:
        attempt_status = (
            "cancelled" if status is SessionStatus.CANCELLED else "failed"
        )
        attempt_error = (
            "stream_cancelled"
            if status is SessionStatus.CANCELLED
            else error_code or "call_failed"
        )
        await connection.execute(
            """
            UPDATE call_attempts
            SET status = %s,
                latency_ms = GREATEST(
                    latency_ms,
                    FLOOR(EXTRACT(EPOCH FROM (%s - started_at)) * 1000)::BIGINT
                ),
                error_code = %s,
                finished_at = %s
            WHERE call_id = %s AND status = 'running'
            """,
            (
                attempt_status,
                finished_at,
                attempt_error,
                finished_at,
                session_id,
            ),
        )

    async def list_reconcilable(
        self, updated_before: datetime, limit: int
    ) -> list[StreamSession]:
        await self._ensure_schema()
        async with await self._connect() as connection:
            rows = await self._fetchall(
                connection,
                self._SESSION_SELECT
                + """
                  WHERE calls.stream = TRUE
                    AND calls.status IN ('pending', 'running', 'cancelling')
                    AND calls.updated_at <= %s
                  ORDER BY calls.updated_at
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
                for statement in CALL_SCHEMA_STATEMENTS:
                    await connection.execute(statement)
                await connection.execute(LEGACY_AUDIT_MIGRATION)
                await connection.commit()
            self._schema_ready = True

    async def _connect(self):
        from psycopg import AsyncConnection

        error_message = "PostgreSQL call store is not configured"
        return await AsyncConnection.connect(
            host=_required_environment(self._config.host_env, error_message),
            port=_required_environment_int(
                self._config.port_env,
                "PostgreSQL port must be an integer between 1 and 65535",
                minimum=1,
                maximum=65535,
            ),
            dbname=_required_environment(self._config.dbname_env, error_message),
            user=_required_environment(self._config.user_env, error_message),
            password=_required_environment(self._config.password_env, error_message),
        )

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
        return {
            "call_id": session.session_id,
            "retry_of_call_id": session.retry_of_call_id,
            "idempotency_key": session.idempotency_key,
            "request_fingerprint": session.request_fingerprint,
            "interface": session.interface.value,
            "owner_id": session.owner_id,
            "requested_model": session.requested_model.value,
            "prompt_name": session.prompt_name,
            "prompt_version": session.prompt_version,
            "status": _database_status(session.status),
            "error_code": session.error_code,
            "replay_degraded": session.replay_degraded,
            "created_at": session.created_at,
            "updated_at": session.updated_at,
            "started_at": session.started_at,
            "finished_at": session.completed_at or session.cancelled_at,
        }

    @staticmethod
    def _from_row(row: dict[str, object]) -> StreamSession:
        status = _session_status(str(row["status"]))
        created_at = row["created_at"]
        started_at = row.get("started_at")
        finished_at = row.get("finished_at")
        total_duration_ms = None
        if isinstance(started_at, datetime) and isinstance(finished_at, datetime):
            total_duration_ms = max(
                0, int((finished_at - started_at).total_seconds() * 1000)
            )
        return StreamSession(
            session_id=str(row.get("call_id") or row.get("session_id")),
            retry_of_call_id=row.get("retry_of_call_id"),
            interface=row["interface"],
            requested_model=row["requested_model"],
            status=status,
            request_fingerprint=str(row["request_fingerprint"]),
            idempotency_key=row.get("idempotency_key"),
            owner_id=row.get("owner_id"),
            actual_model=row.get("actual_model"),
            prompt_name=row.get("prompt_name"),
            prompt_version=row.get("prompt_version"),
            input_tokens=int(row.get("input_tokens") or 0),
            output_tokens=int(row.get("output_tokens") or 0),
            cost_usd=float(row.get("cost_usd") or 0),
            attempts=int(row.get("attempts") or 0),
            result_text=None,
            error_code=row.get("error_code"),
            replay_degraded=bool(row.get("replay_degraded", False)),
            created_at=created_at,
            updated_at=row["updated_at"],
            started_at=started_at,
            completed_at=(
                finished_at
                if status in {SessionStatus.COMPLETED, SessionStatus.FAILED}
                else None
            ),
            cancelled_at=(
                finished_at if status is SessionStatus.CANCELLED else None
            ),
            upstream_first_delta_latency_ms=row.get("first_delta_latency_ms"),
            total_duration_ms=total_duration_ms,
        )


def _database_status(status: SessionStatus) -> str:
    return "success" if status is SessionStatus.COMPLETED else status.value


def _session_status(status: str) -> SessionStatus:
    return SessionStatus.COMPLETED if status == "success" else SessionStatus(status)


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
