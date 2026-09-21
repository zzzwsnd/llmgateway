CALL_TABLE = "calls"
CALL_ATTEMPT_TABLE = "call_attempts"


CALL_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS calls (
        call_id TEXT PRIMARY KEY,
        retry_of_call_id TEXT REFERENCES calls(call_id),
        idempotency_key TEXT UNIQUE,
        request_fingerprint TEXT,
        interface TEXT NOT NULL DEFAULT 'llm' CHECK (
            interface IN ('llm', 'chat_completions', 'responses')
        ),
        stream BOOLEAN NOT NULL DEFAULT FALSE,
        owner_id TEXT,
        requested_model TEXT NOT NULL,
        prompt_name TEXT,
        prompt_version TEXT,
        status TEXT NOT NULL CHECK (
            status IN (
                'pending', 'running', 'cancelling',
                'success', 'failed', 'cancelled'
            )
        ),
        error_code TEXT,
        replay_degraded BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ
    )
    """,
    """
    ALTER TABLE calls
        DROP COLUMN IF EXISTS execution_id
    """,
    """
    UPDATE calls SET interface = 'llm' WHERE interface IS NULL
    """,
    """
    ALTER TABLE calls
        ALTER COLUMN interface SET DEFAULT 'llm',
        ALTER COLUMN interface SET NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS call_attempts (
        attempt_id TEXT PRIMARY KEY,
        call_id TEXT NOT NULL REFERENCES calls(call_id),
        attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
        attempt_type TEXT NOT NULL CHECK (
            attempt_type IN ('initial', 'retry', 'fallback', 'json_retry')
        ),
        model TEXT NOT NULL,
        prompt_name TEXT,
        prompt_version TEXT,
        prompt_sha256 TEXT CHECK (prompt_sha256 ~ '^[0-9a-f]{64}$'),
        status TEXT NOT NULL CHECK (
            status IN ('running', 'success', 'failed', 'invalid_output', 'cancelled')
        ),
        input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
        output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
        cost_usd NUMERIC(20, 10) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
        latency_ms BIGINT NOT NULL DEFAULT 0 CHECK (latency_ms >= 0),
        first_delta_latency_ms BIGINT CHECK (first_delta_latency_ms >= 0),
        error_code TEXT,
        json_validation_status TEXT NOT NULL CHECK (
            json_validation_status IN ('not_requested', 'valid', 'repaired', 'invalid')
        ),
        json_validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        UNIQUE (call_id, attempt_no)
    )
    """,
    """
    ALTER TABLE call_attempts
        ADD COLUMN IF NOT EXISTS prompt_sha256 TEXT
            CHECK (prompt_sha256 ~ '^[0-9a-f]{64}$')
    """,
    """
    CREATE INDEX IF NOT EXISTS calls_created_at_idx
    ON calls (created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS calls_status_updated_at_idx
    ON calls (status, updated_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS calls_retry_of_call_id_idx
    ON calls (retry_of_call_id)
    WHERE retry_of_call_id IS NOT NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS call_attempts_call_id_idx
    ON call_attempts (call_id, attempt_no)
    """,
)


LEGACY_AUDIT_MIGRATION = """
DO $migration$
BEGIN
    IF to_regclass('gateway_calls') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM gateway_calls legacy
            JOIN calls current ON current.call_id = legacy.request_id
            WHERE ROW(
                current.idempotency_key,
                current.request_fingerprint,
                current.interface,
                current.stream,
                current.owner_id,
                current.requested_model,
                current.prompt_name,
                current.prompt_version,
                current.status,
                current.error_code,
                current.replay_degraded,
                current.created_at,
                current.updated_at,
                current.started_at,
                current.finished_at
            ) IS DISTINCT FROM ROW(
                legacy.idempotency_key,
                legacy.request_fingerprint,
                COALESCE(legacy.interface, 'llm'),
                legacy.session_id IS NOT NULL,
                legacy.owner_id,
                legacy.requested_model,
                legacy.prompt_name,
                legacy.prompt_version,
                CASE
                    WHEN legacy.status = 'completed' THEN 'success'
                    ELSE legacy.status
                END,
                legacy.error_code,
                legacy.replay_degraded,
                legacy.created_at,
                legacy.updated_at,
                legacy.started_at,
                legacy.finished_at
            )
        ) THEN
            RAISE EXCEPTION 'gateway_calls metadata conflict';
        END IF;

        INSERT INTO calls (
            call_id, idempotency_key, request_fingerprint, interface, stream,
            owner_id, requested_model, prompt_name, prompt_version, status,
            error_code, replay_degraded, created_at, updated_at, started_at,
            finished_at
        )
        SELECT
            request_id, idempotency_key, request_fingerprint,
            COALESCE(interface, 'llm'),
            session_id IS NOT NULL, owner_id, requested_model, prompt_name,
            prompt_version,
            CASE WHEN status = 'completed' THEN 'success' ELSE status END,
            error_code, replay_degraded, created_at, updated_at, started_at,
            finished_at
        FROM gateway_calls
        ON CONFLICT (call_id) DO NOTHING;

        IF (SELECT COUNT(*) FROM gateway_calls) <> (
            SELECT COUNT(*)
            FROM calls current
            WHERE current.call_id IN (SELECT request_id FROM gateway_calls)
        ) THEN
            RAISE EXCEPTION 'gateway_calls migration count validation failed';
        END IF;
    END IF;

    IF to_regclass('gateway_stream_sessions') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM gateway_stream_sessions legacy
            JOIN calls current ON current.call_id = legacy.session_id
            WHERE ROW(
                current.idempotency_key,
                current.request_fingerprint,
                current.interface,
                current.stream,
                current.owner_id,
                current.requested_model,
                current.status,
                current.error_code,
                current.replay_degraded,
                current.created_at,
                current.updated_at,
                current.started_at,
                current.finished_at
            ) IS DISTINCT FROM ROW(
                legacy.idempotency_key,
                legacy.request_fingerprint,
                COALESCE(legacy.interface, 'llm'),
                TRUE,
                legacy.owner_id,
                legacy.requested_model,
                CASE
                    WHEN legacy.status = 'completed' THEN 'success'
                    ELSE legacy.status
                END,
                legacy.error_code,
                legacy.replay_degraded,
                legacy.created_at,
                legacy.updated_at,
                legacy.started_at,
                COALESCE(legacy.completed_at, legacy.cancelled_at)
            )
        ) THEN
            RAISE EXCEPTION 'gateway_stream_sessions metadata conflict';
        END IF;

        INSERT INTO calls (
            call_id, idempotency_key, request_fingerprint, interface, stream,
            owner_id, requested_model, status, error_code, replay_degraded,
            created_at, updated_at, started_at, finished_at
        )
        SELECT
            session_id, idempotency_key, request_fingerprint,
            COALESCE(interface, 'llm'), TRUE,
            owner_id, requested_model,
            CASE WHEN status = 'completed' THEN 'success' ELSE status END,
            error_code, replay_degraded, created_at, updated_at, started_at,
            COALESCE(completed_at, cancelled_at)
        FROM gateway_stream_sessions
        ON CONFLICT (call_id) DO NOTHING;

        IF (SELECT COUNT(*) FROM gateway_stream_sessions) <> (
            SELECT COUNT(*)
            FROM calls current
            WHERE current.call_id IN (
                SELECT session_id FROM gateway_stream_sessions
            )
        ) THEN
            RAISE EXCEPTION
                'gateway_stream_sessions migration count validation failed';
        END IF;
    END IF;

    DROP TABLE IF EXISTS gateway_call_events;
    DROP TABLE IF EXISTS gateway_call_results;
    DROP TABLE IF EXISTS gateway_calls;
    DROP TABLE IF EXISTS gateway_stream_sessions;
END
$migration$;
"""
