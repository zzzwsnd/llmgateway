import asyncio
import os

from app.mapper.call_schema import (
    CALL_ATTEMPT_TABLE,
    CALL_SCHEMA_STATEMENTS,
    CALL_TABLE,
    LEGACY_AUDIT_MIGRATION,
)
from app.model.config import PostgresConnectionConfig
from app.model.entity import CallAttempt, CallRecord, CallTrace


class PostgresTraceMapper:
    call_table = CALL_TABLE
    attempt_table = CALL_ATTEMPT_TABLE

    def __init__(self, config: PostgresConnectionConfig) -> None:
        self._config = config
        self._schema_ready = False
        self._schema_lock = asyncio.Lock()

    async def insert_call(self, call: CallRecord) -> CallRecord:
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
                    %(request_fingerprint)s, %(interface)s, %(stream)s,
                    %(owner_id)s, %(requested_model)s, %(prompt_name)s,
                    %(prompt_version)s, %(status)s, %(error_code)s,
                    %(replay_degraded)s, %(created_at)s,
                    %(updated_at)s, %(started_at)s, %(finished_at)s
                )
                RETURNING *
                """,
                self._call_parameters(call),
            )
            await connection.commit()
        return CallRecord.model_validate(row)

    async def get_call(self, call_id: str) -> CallRecord | None:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                "SELECT * FROM calls WHERE call_id = %s",
                (call_id,),
            )
        return CallRecord.model_validate(row) if row is not None else None

    async def update_call(self, call: CallRecord) -> CallRecord:
        await self._ensure_schema()
        async with await self._connect() as connection:
            current = await self._fetchone(
                connection,
                "SELECT status FROM calls WHERE call_id = %s FOR UPDATE",
                (call.call_id,),
            )
            if current is None:
                raise RuntimeError("Call audit record does not exist")
            current_status = str(current["status"])
            if current_status in {"success", "failed", "cancelled"}:
                if current_status == call.status.value:
                    existing = await self._fetchone(
                        connection,
                        "SELECT * FROM calls WHERE call_id = %s",
                        (call.call_id,),
                    )
                    return CallRecord.model_validate(existing)
                raise RuntimeError("A terminal call audit record is immutable")
            row = await self._fetchone(
                connection,
                """
                UPDATE calls
                SET retry_of_call_id = %(retry_of_call_id)s,
                    idempotency_key = %(idempotency_key)s,
                    request_fingerprint = %(request_fingerprint)s,
                    interface = %(interface)s,
                    stream = %(stream)s,
                    owner_id = %(owner_id)s,
                    requested_model = %(requested_model)s,
                    prompt_name = %(prompt_name)s,
                    prompt_version = %(prompt_version)s,
                    status = %(status)s,
                    error_code = %(error_code)s,
                    replay_degraded = %(replay_degraded)s,
                    updated_at = %(updated_at)s,
                    started_at = %(started_at)s,
                    finished_at = %(finished_at)s
                WHERE call_id = %(call_id)s AND status = %(current_status)s
                RETURNING *
                """,
                {**self._call_parameters(call), "current_status": current_status},
            )
            if row is None:
                raise RuntimeError("Call audit state changed concurrently")
            await connection.commit()
        return CallRecord.model_validate(row)

    async def insert_attempt(self, attempt: CallAttempt) -> CallAttempt:
        from psycopg.types.json import Jsonb

        await self._ensure_schema()
        async with await self._connect() as connection:
            parent = await self._fetchone(
                connection,
                "SELECT status FROM calls WHERE call_id = %s FOR UPDATE",
                (attempt.call_id,),
            )
            if parent is None:
                raise RuntimeError("Call audit record does not exist")
            if str(parent["status"]) != "running":
                raise RuntimeError("Cannot start an attempt unless the call is running")
            numbered = attempt.model_copy(
                update={
                    "attempt_no": await self._next_attempt_number(
                        connection, attempt.call_id
                    )
                }
            )
            parameters = self._attempt_parameters(numbered)
            parameters["json_validation_errors"] = Jsonb(
                numbered.json_validation_errors
            )
            row = await self._fetchone(
                connection,
                """
                INSERT INTO call_attempts (
                    attempt_id, call_id, attempt_no, attempt_type, model,
                    prompt_name, prompt_version, prompt_sha256, status, input_tokens,
                    output_tokens, cost_usd, latency_ms,
                    first_delta_latency_ms, error_code,
                    json_validation_status, json_validation_errors,
                    started_at, finished_at
                ) VALUES (
                    %(attempt_id)s, %(call_id)s, %(attempt_no)s,
                    %(attempt_type)s, %(model)s, %(prompt_name)s,
                    %(prompt_version)s, %(prompt_sha256)s, %(status)s, %(input_tokens)s,
                    %(output_tokens)s, %(cost_usd)s, %(latency_ms)s,
                    %(first_delta_latency_ms)s, %(error_code)s,
                    %(json_validation_status)s, %(json_validation_errors)s,
                    %(started_at)s, %(finished_at)s
                )
                RETURNING *
                """,
                parameters,
            )
            await connection.commit()
        return CallAttempt.model_validate(row)

    async def update_attempt(self, attempt: CallAttempt) -> CallAttempt:
        from psycopg.types.json import Jsonb

        await self._ensure_schema()
        async with await self._connect() as connection:
            current = await self._fetchone(
                connection,
                "SELECT status FROM call_attempts WHERE attempt_id = %s FOR UPDATE",
                (attempt.attempt_id,),
            )
            if current is None:
                raise RuntimeError("Call attempt does not exist")
            current_status = str(current["status"])
            if current_status != "running":
                if current_status == attempt.status.value:
                    existing = await self._fetchone(
                        connection,
                        "SELECT * FROM call_attempts WHERE attempt_id = %s",
                        (attempt.attempt_id,),
                    )
                    return CallAttempt.model_validate(existing)
                raise RuntimeError("A terminal call attempt is immutable")
            parameters = self._attempt_parameters(attempt)
            parameters["json_validation_errors"] = Jsonb(
                attempt.json_validation_errors
            )
            parameters["current_status"] = current_status
            row = await self._fetchone(
                connection,
                """
                UPDATE call_attempts
                SET status = %(status)s,
                    input_tokens = %(input_tokens)s,
                    output_tokens = %(output_tokens)s,
                    cost_usd = %(cost_usd)s,
                    latency_ms = %(latency_ms)s,
                    first_delta_latency_ms = %(first_delta_latency_ms)s,
                    error_code = %(error_code)s,
                    json_validation_status = %(json_validation_status)s,
                    json_validation_errors = %(json_validation_errors)s,
                    finished_at = %(finished_at)s
                WHERE attempt_id = %(attempt_id)s
                  AND status = %(current_status)s
                RETURNING *
                """,
                parameters,
            )
            if row is None:
                raise RuntimeError("Call attempt state changed concurrently")
            await connection.commit()
        return CallAttempt.model_validate(row)

    async def find_all(self) -> list[CallTrace]:
        await self._ensure_schema()
        async with await self._connect() as connection:
            rows = await self._fetchall(
                connection,
                """
                SELECT
                    calls.call_id AS request_id,
                    COALESCE(calls.finished_at, calls.updated_at) AS timestamp,
                    calls.requested_model,
                    (
                        SELECT successful.model
                        FROM call_attempts successful
                        WHERE successful.call_id = calls.call_id
                          AND successful.status = 'success'
                        ORDER BY successful.attempt_no DESC
                        LIMIT 1
                    ) AS actual_model,
                    calls.prompt_name,
                    calls.prompt_version,
                    COALESCE(SUM(attempts.input_tokens), 0) AS input_tokens,
                    COALESCE(SUM(attempts.output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(attempts.cost_usd), 0) AS cost_usd,
                    CASE
                        WHEN calls.started_at IS NOT NULL
                         AND calls.finished_at IS NOT NULL
                        THEN GREATEST(
                            0,
                            FLOOR(EXTRACT(EPOCH FROM (
                                calls.finished_at - calls.started_at
                            )) * 1000)::BIGINT
                        )
                        ELSE 0
                    END AS latency_ms,
                    COUNT(attempts.attempt_id) AS attempts,
                    calls.status,
                    calls.error_code
                FROM calls
                LEFT JOIN call_attempts attempts
                    ON attempts.call_id = calls.call_id
                GROUP BY calls.call_id
                ORDER BY calls.created_at DESC, calls.call_id DESC
                """,
                (),
            )
        return [CallTrace.model_validate(row) for row in rows]

    async def find_trace(self, call_id: str) -> CallTrace | None:
        await self._ensure_schema()
        async with await self._connect() as connection:
            row = await self._fetchone(
                connection,
                """
                SELECT
                    calls.call_id AS request_id,
                    COALESCE(calls.finished_at, calls.updated_at) AS timestamp,
                    calls.requested_model,
                    (
                        SELECT successful.model
                        FROM call_attempts successful
                        WHERE successful.call_id = calls.call_id
                          AND successful.status = 'success'
                        ORDER BY successful.attempt_no DESC
                        LIMIT 1
                    ) AS actual_model,
                    calls.prompt_name,
                    calls.prompt_version,
                    COALESCE((
                        SELECT SUM(attempts.input_tokens)
                        FROM call_attempts attempts
                        WHERE attempts.call_id = calls.call_id
                    ), 0) AS input_tokens,
                    COALESCE((
                        SELECT SUM(attempts.output_tokens)
                        FROM call_attempts attempts
                        WHERE attempts.call_id = calls.call_id
                    ), 0) AS output_tokens,
                    COALESCE((
                        SELECT SUM(attempts.cost_usd)
                        FROM call_attempts attempts
                        WHERE attempts.call_id = calls.call_id
                    ), 0) AS cost_usd,
                    CASE
                        WHEN calls.started_at IS NOT NULL
                         AND calls.finished_at IS NOT NULL
                        THEN GREATEST(
                            0,
                            FLOOR(EXTRACT(EPOCH FROM (
                                calls.finished_at - calls.started_at
                            )) * 1000)::BIGINT
                        )
                        ELSE 0
                    END AS latency_ms,
                    (
                        SELECT COUNT(*)
                        FROM call_attempts attempts
                        WHERE attempts.call_id = calls.call_id
                    ) AS attempts,
                    calls.status,
                    calls.error_code
                FROM calls
                WHERE calls.call_id = %s
                """,
                (call_id,),
            )
        return CallTrace.model_validate(row) if row is not None else None

    async def find_attempts(self, call_id: str) -> list[CallAttempt]:
        await self._ensure_schema()
        async with await self._connect() as connection:
            rows = await self._fetchall(
                connection,
                """
                SELECT * FROM call_attempts
                WHERE call_id = %s
                ORDER BY attempt_no
                """,
                (call_id,),
            )
        return [CallAttempt.model_validate(row) for row in rows]

    async def _next_attempt_number(self, connection, call_id: str) -> int:
        row = await self._fetchone(
            connection,
            """
            SELECT COALESCE(MAX(attempt_no), 0) + 1 AS attempt_no
            FROM call_attempts
            WHERE call_id = %s
            """,
            (call_id,),
        )
        return int(row["attempt_no"])

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

        return await AsyncConnection.connect(
            host=_required_environment(self._config.host_env),
            port=_required_environment_int(self._config.port_env),
            dbname=_required_environment(self._config.dbname_env),
            user=_required_environment(self._config.user_env),
            password=_required_environment(self._config.password_env),
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
    def _call_parameters(call: CallRecord) -> dict[str, object]:
        values = call.model_dump(mode="json")
        return values

    @staticmethod
    def _attempt_parameters(attempt: CallAttempt) -> dict[str, object]:
        return attempt.model_dump(mode="json")


def _required_environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError("PostgreSQL call audit store is not configured")
    return value


def _required_environment_int(name: str) -> int:
    raw_value = _required_environment(name)
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError("PostgreSQL port must be an integer") from exc
    if not 1 <= value <= 65535:
        raise RuntimeError("PostgreSQL port must be between 1 and 65535")
    return value
