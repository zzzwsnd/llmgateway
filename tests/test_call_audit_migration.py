import os
from uuid import uuid4

import pytest

from app.mapper.call_schema import CALL_SCHEMA_STATEMENTS, LEGACY_AUDIT_MIGRATION


def _connect_postgres():
    from psycopg import connect

    names = {
        "host": "GATEWAY_POSTGRES_HOST",
        "port": "GATEWAY_POSTGRES_PORT",
        "dbname": "GATEWAY_POSTGRES_DBNAME",
        "user": "GATEWAY_POSTGRES_USER",
        "password": "GATEWAY_POSTGRES_PASSWORD",
    }
    values = {key: os.getenv(name) for key, name in names.items()}
    if any(value is None for value in values.values()):
        pytest.skip("local PostgreSQL audit migration test is not configured")
    values["port"] = int(values["port"])
    return connect(**values)


@pytest.fixture
def postgres_schema():
    from psycopg import sql

    connection = _connect_postgres()
    schema = f"audit_migration_{uuid4().hex}"
    connection.autocommit = True
    connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    connection.execute(
        sql.SQL("SET search_path TO {}").format(sql.Identifier(schema))
    )
    for statement in CALL_SCHEMA_STATEMENTS:
        connection.execute(statement)
    try:
        yield connection
    finally:
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )
        connection.close()


def _create_legacy_calls(connection) -> None:
    connection.execute(
        """
        CREATE TABLE gateway_calls (
            request_id TEXT PRIMARY KEY,
            idempotency_key TEXT,
            request_fingerprint TEXT,
            interface TEXT,
            session_id TEXT,
            owner_id TEXT,
            requested_model TEXT NOT NULL,
            prompt_name TEXT,
            prompt_version TEXT,
            status TEXT NOT NULL,
            execution_id TEXT,
            error_code TEXT,
            replay_degraded BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ
        )
        """
    )


def _create_legacy_sessions(connection) -> None:
    connection.execute(
        """
        CREATE TABLE gateway_stream_sessions (
            session_id TEXT PRIMARY KEY,
            idempotency_key TEXT,
            request_fingerprint TEXT,
            interface TEXT,
            owner_id TEXT,
            requested_model TEXT NOT NULL,
            status TEXT NOT NULL,
            generation_id TEXT,
            error_code TEXT,
            replay_degraded BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            cancelled_at TIMESTAMPTZ
        )
        """
    )


def test_legacy_migration_rejects_conflicting_existing_call_and_rolls_back(
    postgres_schema,
) -> None:
    connection = postgres_schema
    _create_legacy_calls(connection)
    connection.execute(
        """
        INSERT INTO calls (
            call_id, interface, requested_model, status,
            created_at, updated_at
        ) VALUES (
            'call-conflict', 'llm', 'general-backup', 'failed',
            '2026-09-20T00:00:00Z', '2026-09-20T00:00:01Z'
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gateway_calls (
            request_id, interface, requested_model, status,
            replay_degraded, created_at, updated_at
        ) VALUES (
            'call-conflict', 'llm', 'general-primary', 'success',
            FALSE, '2026-09-20T00:00:00Z', '2026-09-20T00:00:01Z'
        )
        """
    )

    with pytest.raises(Exception, match="metadata conflict"):
        connection.execute(LEGACY_AUDIT_MIGRATION)

    legacy_exists = connection.execute(
        "SELECT to_regclass('gateway_calls') IS NOT NULL"
    )
    assert legacy_exists.fetchone()[0] is True
    stored = connection.execute(
        "SELECT requested_model, status FROM calls WHERE call_id = 'call-conflict'"
    )
    assert stored.fetchone() == ("general-backup", "failed")


def test_legacy_migration_accepts_identical_overlap_and_is_repeatable(
    postgres_schema,
) -> None:
    connection = postgres_schema
    _create_legacy_calls(connection)
    _create_legacy_sessions(connection)
    connection.execute(
        """
        INSERT INTO gateway_calls (
            request_id, idempotency_key, request_fingerprint, interface,
            session_id, owner_id, requested_model, status, execution_id,
            replay_degraded, created_at, updated_at, started_at, finished_at
        ) VALUES (
            'session-overlap', 'key-1', 'fingerprint-1', 'llm',
            'session-overlap', 'owner-1', 'general-primary', 'completed',
            'generation-1', FALSE, '2026-09-20T00:00:00Z',
            '2026-09-20T00:00:01Z', '2026-09-20T00:00:00Z',
            '2026-09-20T00:00:01Z'
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gateway_stream_sessions (
            session_id, idempotency_key, request_fingerprint, interface,
            owner_id, requested_model, status, generation_id,
            replay_degraded, created_at, updated_at, started_at, completed_at
        ) VALUES (
            'session-overlap', 'key-1', 'fingerprint-1', 'llm',
            'owner-1', 'general-primary', 'completed', 'generation-1',
            FALSE, '2026-09-20T00:00:00Z', '2026-09-20T00:00:01Z',
            '2026-09-20T00:00:00Z', '2026-09-20T00:00:01Z'
        )
        """
    )

    connection.execute(LEGACY_AUDIT_MIGRATION)
    connection.execute(LEGACY_AUDIT_MIGRATION)

    rows = connection.execute(
        "SELECT call_id, status FROM calls ORDER BY call_id"
    ).fetchall()
    assert rows == [("session-overlap", "success")]
    assert connection.execute(
        "SELECT to_regclass('gateway_calls'), "
        "to_regclass('gateway_stream_sessions')"
    ).fetchone() == (None, None)
