from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections.abc import AsyncIterator
from contextlib import aclosing
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from app.core.errors import GatewayError
from app.core.logging import logger
from app.core.snowflake import SnowflakeIdGenerator, format_session_id
from app.core.utils import encode_session_sse
from app.dao.runtime_dao import RuntimeDao
from app.dao.session_dao import SessionDao
from app.model.config import SessionRuntimeConfig
from app.model.enums import LLMProtocolEnum
from app.model.session import (
    CreateChatSession, CreateLLMSession, CreateResponsesSession,
    CreateStreamSessionRequest, CreateStreamSessionResponse, RuntimeEvent,
    RuntimeEventType, SessionAction, SessionConnection, SessionEvent,
    SessionEventType, SessionStatus, StreamSession, StreamSessionResponse,
)
from app.service.chat_completions_service import ChatCompletionsService
from app.service.compensation_service import CompensationService
from app.service.llm_service import LLMService
from app.service.producer_supervisor import ProducerSupervisor
from app.service.responses_service import ResponsesService
from app.service.session_access import AllowAllSessionAccessPolicy, SessionAccessPolicy


class StreamSessionService:
    """Durable stream-session orchestration; runtime state is append/read only."""

    def __init__(
        self, *, session_dao: SessionDao, runtime_dao: RuntimeDao,
        llm_service: LLMService, supervisor: ProducerSupervisor,
        compensation_service: CompensationService, config: SessionRuntimeConfig,
        id_generator: SnowflakeIdGenerator,
        access_policy: SessionAccessPolicy | None = None,
    ) -> None:
        self._sessions, self._runtime = session_dao, runtime_dao
        self._llm, self._supervisor = llm_service, supervisor
        self._compensation, self._config = compensation_service, config
        self._ids = id_generator
        self._access = access_policy or AllowAllSessionAccessPolicy()
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._stopping = False

    async def start(self) -> None:
        self._stopping = False
        await self._supervisor.start()
        self._start_background(self._reconcile_loop(), "session-reconciler")

    async def stop(self) -> None:
        self._stopping = True
        for task in list(self._background_tasks):
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self._supervisor.stop()
        if not self._supervisor.has_active_tasks:
            try:
                await asyncio.wait_for(self._runtime.close(), timeout=1)
            except Exception:
                logger.warning("Failed to close stream runtime")

    async def create(self, body: CreateStreamSessionRequest, *, idempotency_key: str | None = None, actor_id: str | None = None) -> CreateStreamSessionResponse:
        request, required = self._prepare_request(body)
        self._llm.validate_stream_request(request, required)
        fingerprint = self._fingerprint(body)
        retry_of_call_id = body.retry_of_call_id
        if retry_of_call_id is not None:
            await self._validate_expired_retry(
                retry_of_call_id,
                fingerprint=fingerprint,
                interface=body.interface,
                idempotency_key=idempotency_key,
                actor_id=actor_id,
            )
        if idempotency_key is not None:
            try:
                existing = await self._sessions.get_by_idempotency(idempotency_key)
            except Exception as exc:
                raise GatewayError("session_store_unavailable", "Stream session store is unavailable", 503) from exc
            if existing is not None:
                self._access.assert_allowed(actor_id, existing, SessionAction.READ)
                if existing.request_fingerprint != fingerprint:
                    raise GatewayError("idempotency_conflict", "Idempotency-Key was already used for a different request", 409)
                return self._create_response(existing)
        if not self._supervisor.can_accept:
            await self._prune_queued()
        if not self._supervisor.can_accept:
            raise GatewayError("session_capacity_exceeded", "Session capacity is exhausted", 503)
        now = datetime.now(timezone.utc)
        session = StreamSession(
            session_id=format_session_id(self._ids.next_id()), interface=body.interface,
            requested_model=body.request.model, status=SessionStatus.PENDING,
            request_fingerprint=fingerprint,
            idempotency_key=idempotency_key, owner_id=actor_id,
            retry_of_call_id=retry_of_call_id,
            prompt_name=request.prompt.name if request.prompt else None,
            prompt_version=request.prompt.version if request.prompt else None,
            created_at=now, updated_at=now,
        )
        self._access.assert_allowed(actor_id, session, SessionAction.CREATE)
        try:
            stored, created = await self._sessions.insert_or_get(session)
        except Exception as exc:
            raise GatewayError("session_store_unavailable", "Stream session store is unavailable", 503) from exc
        if not created:
            self._access.assert_allowed(actor_id, stored, SessionAction.READ)
            if stored.request_fingerprint != fingerprint:
                raise GatewayError("idempotency_conflict", "Idempotency-Key was already used for a different request", 409)
            return self._create_response(stored)
        if not await self._supervisor.schedule(stored.session_id, lambda: self._run_producer(stored.session_id, request, required)):
            await self._fail_pending(stored, "session_capacity_exceeded")
            raise GatewayError("session_capacity_exceeded", "Session capacity is exhausted", 503)
        self._start_background(
            self._heartbeat_loop(stored.session_id),
            f"session-activity-{stored.session_id}",
        )
        return self._create_response(stored)

    async def get(self, session_id: str, *, actor_id: str | None = None) -> StreamSessionResponse:
        session = await self._get_required(session_id)
        self._access.assert_allowed(actor_id, session, SessionAction.READ)
        try:
            replay_available = await asyncio.wait_for(
                self._runtime.has_public_events(session_id), timeout=1
            )
        except Exception:
            replay_available = False
        return StreamSessionResponse(
            session_id=session.session_id, interface=session.interface,
            model=session.requested_model, status=session.status,
            result_text=None, error_code=session.error_code,
            replay_degraded=session.replay_degraded, replay_available=replay_available,
            active_connections=None,
            producer_health="unknown", created_at=session.created_at,
            updated_at=session.updated_at, completed_at=session.completed_at,
            upstream_first_delta_latency_ms=session.upstream_first_delta_latency_ms,
            first_content_latency_ms=session.first_content_latency_ms,
            total_duration_ms=session.total_duration_ms,
        )

    async def _get_required(self, session_id: str) -> StreamSession:
        try:
            session = await self._sessions.get(session_id)
        except Exception as exc:
            raise GatewayError("session_store_unavailable", "Stream session store is unavailable", 503) from exc
        if session is None:
            raise GatewayError("session_not_found", "Stream session was not found", 404)
        return session

    @staticmethod
    def _prepare_request(body):
        if isinstance(body, CreateLLMSession):
            return body.request, None
        if isinstance(body, CreateChatSession):
            return ChatCompletionsService.to_llm_request(body.request), LLMProtocolEnum.CHAT_COMPLETIONS
        if isinstance(body, CreateResponsesSession):
            return ResponsesService.to_llm_request(body.request), LLMProtocolEnum.RESPONSES
        raise TypeError("Unsupported stream session request")

    @staticmethod
    def _fingerprint(body) -> str:
        raw = json.dumps(
            body.model_dump(mode="json", exclude={"retry_of_call_id"}),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(raw).hexdigest()

    async def _validate_expired_retry(
        self,
        retry_of_call_id: str,
        *,
        fingerprint: str,
        interface: SessionInterface,
        idempotency_key: str | None,
        actor_id: str | None,
    ) -> None:
        original = await self._get_required(retry_of_call_id)
        self._access.assert_allowed(actor_id, original, SessionAction.READ)
        if original.owner_id != actor_id:
            raise GatewayError(
                "retry_request_mismatch",
                "The retry does not match the expired stream call",
                409,
            )
        if idempotency_key is None or idempotency_key == original.idempotency_key:
            raise GatewayError(
                "retry_requires_new_idempotency_key",
                "An expired stream retry requires a new Idempotency-Key",
                409,
            )
        if (
            not original.status.is_terminal
            or original.interface is not interface
            or original.request_fingerprint != fingerprint
        ):
            raise GatewayError(
                "retry_request_mismatch",
                "The retry does not match the expired stream call",
                409,
            )
        try:
            replay_available = await asyncio.wait_for(
                self._runtime.has_public_events(retry_of_call_id), timeout=1
            )
        except Exception as exc:
            raise GatewayError(
                "session_runtime_unavailable",
                "Session runtime is unavailable",
                503,
            ) from exc
        if replay_available and not original.replay_degraded:
            raise GatewayError(
                "stream_not_expired",
                "The original stream is still available",
                409,
            )

    @staticmethod
    def _create_response(session: StreamSession) -> CreateStreamSessionResponse:
        base = f"/v1/stream-sessions/{session.session_id}"
        return CreateStreamSessionResponse(session_id=session.session_id, status=session.status, status_url=base, events_url=f"{base}/events")

    async def subscribe_events(self, session_id: str, *, after: str | None = None, actor_id: str | None = None, _connection: SessionConnection | None = None) -> AsyncIterator[SessionEvent]:
        self._validate_cursor(after)
        session = await self._get_required(session_id)
        self._access.assert_allowed(actor_id, session, SessionAction.SUBSCRIBE)
        if session.status.is_terminal:
            try:
                replay_available = await asyncio.wait_for(
                    self._runtime.has_public_events(session_id, after=after), timeout=1
                )
            except Exception as exc:
                raise GatewayError(
                    "session_runtime_unavailable",
                    "Session runtime is unavailable",
                    503,
                ) from exc
            if not replay_available or session.replay_degraded:
                raise GatewayError(
                    "stream_expired",
                    f"Stream events expired for call {session_id}",
                    410,
                )
        connection = _connection or SessionConnection(connection_id=str(uuid4()), session_id=session_id, connected_at=datetime.now(timezone.utc))
        cursor = after
        try:
            control_cursor = await asyncio.wait_for(
                self._runtime.latest_event_id(session_id), timeout=1
            )
        except Exception:
            control_cursor = None
        yield SessionEvent(type=SessionEventType.CONNECTED, data={"connection_id": connection.connection_id}, created_at=connection.connected_at)
        while True:
            try:
                controls = await self._read_runtime(
                    session_id, after=control_cursor, block_milliseconds=0
                )
            except Exception:
                controls = []
            for control in controls:
                control_cursor = control.event_id
                if (
                    control.type is RuntimeEventType.CONTROL_DETACH
                    and control.connection_id == connection.connection_id
                ):
                    return
            runtime_failed = False
            try:
                events = await self._read_runtime(
                    session_id, after=cursor,
                    block_milliseconds=0 if session.status.is_terminal else self._config.event_block_milliseconds,
                )
            except Exception:
                events, runtime_failed = [], True
            for event in events:
                cursor = event.event_id
                if event.type is RuntimeEventType.CONTROL_DETACH:
                    if event.connection_id == connection.connection_id:
                        return
                    continue
                public = self._public_event(event)
                if public is None:
                    continue
                if public.type is SessionEventType.DELTA:
                    yield public
                    continue
                current = await self._get_required(session_id)
                if not current.status.is_terminal:
                    continue
                yield public
                return
            session = await self._get_required(session_id)
            # Drain every replay batch before trusting a database-only terminal.
            if events:
                continue
            if session.status.is_terminal:
                yield self._terminal_event(session)
                return
            yield self._event(SessionEventType.HEARTBEAT, {})
            if runtime_failed:
                await asyncio.sleep(self._config.event_block_milliseconds / 1000)

    def stream(self, session_id: str, *, after: str | None = None, actor_id: str | None = None, _connection: SessionConnection | None = None) -> AsyncIterator[str]:
        async def iterator() -> AsyncIterator[str]:
            async for event in self.subscribe_events(session_id, after=after, actor_id=actor_id, _connection=_connection):
                yield encode_session_sse(event)
        return iterator()

    async def open_stream(self, session_id: str, *, after: str | None = None, actor_id: str | None = None) -> AsyncIterator[str]:
        self._validate_cursor(after)
        session = await self._get_required(session_id)
        self._access.assert_allowed(actor_id, session, SessionAction.SUBSCRIBE)
        return self.stream(session_id, after=after, actor_id=actor_id)

    async def detach(self, session_id: str, connection_id: str, *, actor_id: str | None = None) -> None:
        session = await self._get_required(session_id)
        self._access.assert_allowed(actor_id, session, SessionAction.DETACH)
        try:
            await asyncio.wait_for(
                self._runtime.append_control(session_id, event_type=RuntimeEventType.CONTROL_DETACH, connection_id=connection_id),
                timeout=1,
            )
        except Exception as exc:
            raise GatewayError("session_runtime_unavailable", "Session runtime is unavailable", 503) from exc

    async def cancel(self, session_id: str, *, actor_id: str | None = None) -> StreamSessionResponse:
        while True:
            session = await self._get_required(session_id)
            self._access.assert_allowed(actor_id, session, SessionAction.CANCEL)
            if session.status.is_terminal:
                return await self.get(session_id, actor_id=actor_id)
            if session.status is SessionStatus.CANCELLING:
                cancelling = session
                break
            try:
                cancelling = await self._sessions.transition(
                    session_id,
                    expected_statuses={session.status},
                    target_status=SessionStatus.CANCELLING,
                )
            except Exception as exc:
                raise GatewayError("session_store_unavailable", "Stream session store is unavailable", 503) from exc
            if cancelling is not None:
                break
        runtime_error = None
        try:
            await asyncio.wait_for(
                self._runtime.append_control(session_id, event_type=RuntimeEventType.CONTROL_CANCEL),
                timeout=1,
            )
        except Exception as exc:
            runtime_error = exc
            logger.warning("Failed to append cancellation control for %s", session_id)
        if session.status is SessionStatus.PENDING:
            await self._supervisor.cancel_queued(session_id)
            await self._finish_cancel(cancelling)
        if runtime_error is not None:
            raise GatewayError("session_runtime_unavailable", "Session runtime is unavailable", 503) from runtime_error
        return await self.get(session_id, actor_id=actor_id)

    async def reconcile_once(self) -> None:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=min(self._config.startup_grace_seconds, self._config.producer_lost_grace_seconds))
        try:
            sessions = await self._sessions.list_reconcilable(cutoff)
        except Exception:
            logger.warning("Failed to list reconcilable sessions")
            return
        for session in sessions:
            if self._supervisor.owns(session.session_id):
                continue
            grace = (
                self._config.startup_grace_seconds
                if session.status is SessionStatus.PENDING
                else self._config.producer_lost_grace_seconds
            )
            session_cutoff = now - timedelta(seconds=grace)
            if session.updated_at > session_cutoff:
                continue
            try:
                terminal = await self._sessions.reconcile(
                    session.session_id,
                    updated_before=session_cutoff,
                )
            except Exception:
                logger.warning("Failed to reconcile session %s", session.session_id)
                continue
            if terminal is not None:
                await self._publish_terminal(terminal)
                if terminal.status is SessionStatus.CANCELLED:
                    await self._compensate(terminal)

    async def _reconcile_loop(self) -> None:
        while not self._stopping:
            try:
                await asyncio.sleep(self._config.reconcile_interval_seconds)
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Session reconciliation failed")

    async def _run_producer(self, session_id: str, request, required_protocol) -> None:
        session = await self._get_required(session_id)
        running = await self._transition_retry(session, SessionStatus.RUNNING)
        if running is None:
            return
        first_content_ms = upstream_first_ms = None
        replay_degraded = False

        async def consume() -> None:
            nonlocal first_content_ms, upstream_first_ms, replay_degraded
            async with aclosing(
                self._llm.stream_events(
                    request,
                    required_protocol,
                    existing_call_id=session_id,
                )
            ) as stream:
                async for event in stream:
                    current = await self._get_required(session_id)
                    if current.status is not SessionStatus.RUNNING:
                        raise asyncio.CancelledError
                    if event.type == "text_delta" and event.delta:
                        if upstream_first_ms is None:
                            upstream_first_ms = event.upstream_first_delta_latency_ms
                        published = await self._append_runtime(current, RuntimeEventType.DELTA, {"delta": event.delta})
                        if published is None:
                            replay_degraded = True
                        elif first_content_ms is None:
                            first_content_ms = max(0, int((datetime.now(timezone.utc) - session.created_at).total_seconds() * 1000))
                    elif event.type == "failed":
                        raise GatewayError(event.error_code or "upstream_stream_failed", "Upstream provider failed", 502)
                    elif event.type == "completed":
                        return

        provider = asyncio.create_task(consume(), name=f"provider-{session_id}")
        controls = asyncio.create_task(self._watch_cancel(running, provider), name=f"controls-{session_id}")
        target, error_code = SessionStatus.COMPLETED, None
        try:
            await provider
        except asyncio.CancelledError:
            provider.cancel()
            await asyncio.gather(provider, return_exceptions=True)
            target, error_code = SessionStatus.FAILED, "producer_shutdown" if self._stopping else "producer_cancelled"
        except Exception as exc:
            target, error_code = SessionStatus.FAILED, exc.code if isinstance(exc, GatewayError) else "upstream_stream_failed"
        finally:
            controls.cancel()
            await asyncio.gather(controls, return_exceptions=True)
        await self._finish(
            running, target, error_code=error_code,
            replay_degraded=replay_degraded, upstream_first_delta_latency_ms=upstream_first_ms,
            first_content_latency_ms=first_content_ms,
        )

    async def _watch_cancel(self, session: StreamSession, provider: asyncio.Task[None]) -> None:
        cursor = None
        while not provider.done():
            try:
                events = await self._read_runtime(
                    session.session_id, after=cursor,
                    block_milliseconds=min(self._config.event_block_milliseconds, max(1, int(self._config.heartbeat_seconds * 1000))),
                )
                for event in events:
                    cursor = event.event_id
                    if event.type is RuntimeEventType.CONTROL_CANCEL:
                        provider.cancel()
                        return
            except Exception:
                await asyncio.sleep(self._config.heartbeat_seconds)
            try:
                current = await self._sessions.get(session.session_id)
                if current is None or current.status is not SessionStatus.RUNNING:
                    provider.cancel()
                    return
            except Exception:
                logger.warning("Failed to check cancellation state for %s", session.session_id)

    async def _heartbeat_loop(self, session_id: str) -> None:
        while self._supervisor.owns(session_id):
            await asyncio.sleep(self._config.heartbeat_seconds)
            try:
                current = await self._get_required(session_id)
                if current.status.is_terminal:
                    await self._supervisor.cancel_queued(session_id)
                    return
                if current.status is SessionStatus.CANCELLING:
                    if await self._supervisor.cancel_queued(session_id):
                        await self._finish_cancel(current)
                        return
                if not await self._sessions.heartbeat(session_id):
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Session heartbeat failed for %s", session_id)

    async def _prune_queued(self) -> None:
        for session_id in self._supervisor.queued_session_ids:
            current = await self._get_required(session_id)
            if current.status.is_terminal or current.status is SessionStatus.CANCELLING:
                await self._supervisor.cancel_queued(session_id)
                if current.status is SessionStatus.CANCELLING:
                    await self._finish_cancel(current)

    async def _append_runtime(self, session: StreamSession, event_type: RuntimeEventType, data: dict) -> RuntimeEvent | None:
        try:
            return await asyncio.wait_for(
                self._runtime.append_event(session.session_id, RuntimeEvent(type=event_type, data=data, created_at=datetime.now(timezone.utc))),
                timeout=1,
            )
        except Exception:
            logger.warning("Failed to append runtime event for %s", session.session_id)
            return None

    async def _read_runtime(self, session_id: str, *, after: str | None, block_milliseconds: int) -> list[RuntimeEvent]:
        return await asyncio.wait_for(
            self._runtime.read_events(session_id, after=after, block_milliseconds=block_milliseconds),
            timeout=block_milliseconds / 1000 + 0.1,
        )

    async def _transition_retry(self, session: StreamSession, target: SessionStatus, **kwargs) -> StreamSession | None:
        current = session
        allowed = {
            SessionStatus.RUNNING: {SessionStatus.PENDING},
            SessionStatus.COMPLETED: {SessionStatus.RUNNING},
            SessionStatus.FAILED: {SessionStatus.PENDING, SessionStatus.RUNNING},
            SessionStatus.CANCELLED: {SessionStatus.CANCELLING},
        }[target]
        shutdown_deadline = None
        while True:
            if current.status not in allowed:
                return None
            if self._stopping and shutdown_deadline is None:
                shutdown_deadline = time.monotonic() + self._config.shutdown_grace_seconds
            try:
                result = await self._sessions.transition(current.session_id, expected_statuses={current.status}, target_status=target, **kwargs)
                if result is not None:
                    return result
                latest = await self._sessions.get(current.session_id)
                if latest is None or latest.status not in allowed:
                    return None
                current = latest
                if shutdown_deadline is not None and time.monotonic() >= shutdown_deadline:
                    return None
                continue
            except Exception:
                logger.warning("Failed to transition session %s", session.session_id)
                if self._stopping:
                    return None
            if shutdown_deadline is not None and time.monotonic() >= shutdown_deadline:
                return None
            await asyncio.sleep(0.01)

    async def _finish(self, session: StreamSession, target: SessionStatus, **kwargs) -> None:
        while True:
            terminal = await self._transition_retry(session, target, **kwargs)
            if terminal is not None:
                await self._publish_terminal(terminal)
                return
            try:
                current = await self._sessions.get(session.session_id)
            except Exception:
                if self._stopping:
                    return
                await asyncio.sleep(self._config.heartbeat_seconds)
                continue
            if current is None or current.status.is_terminal:
                return
            if current.status is SessionStatus.CANCELLING:
                await self._finish_cancel(current, **kwargs)
            return

    async def _finish_cancel(self, session: StreamSession, **kwargs) -> None:
        kwargs["error_code"] = None
        cancelled = await self._transition_retry(session, SessionStatus.CANCELLED, **kwargs)
        if cancelled is not None:
            await self._publish_terminal(cancelled)
            await self._compensate(cancelled)

    async def _compensate(self, session: StreamSession) -> None:
        try:
            await self._compensation.compensate(session)
        except Exception:
            logger.warning("Cancellation compensation failed for %s", session.session_id)

    async def _publish_terminal(self, session: StreamSession) -> None:
        event_type = {
            SessionStatus.COMPLETED: RuntimeEventType.COMPLETED,
            SessionStatus.FAILED: RuntimeEventType.FAILED,
            SessionStatus.CANCELLED: RuntimeEventType.CANCELLED,
        }[session.status]
        data = {"error_code": session.error_code} if session.error_code else {}
        await self._append_runtime(session, event_type, data)
        try:
            await asyncio.wait_for(
                self._runtime.expire_events(session.session_id, self._config.event_ttl_seconds), timeout=1,
            )
        except Exception:
            logger.warning("Failed to expire stream events for %s", session.session_id)

    async def _fail_pending(self, session: StreamSession, code: str) -> None:
        failed = await self._transition_retry(session, SessionStatus.FAILED, error_code=code)
        if failed is not None:
            await self._publish_terminal(failed)

    @staticmethod
    def _public_event(event: RuntimeEvent) -> SessionEvent | None:
        mapping = {RuntimeEventType.DELTA: SessionEventType.DELTA, RuntimeEventType.COMPLETED: SessionEventType.COMPLETED, RuntimeEventType.FAILED: SessionEventType.FAILED, RuntimeEventType.CANCELLED: SessionEventType.CANCELLED}
        event_type = mapping.get(event.type)
        return None if event_type is None else SessionEvent(event_id=event.event_id, type=event_type, data=event.data, created_at=event.created_at)

    @staticmethod
    def _event(event_type: SessionEventType, data: dict) -> SessionEvent:
        return SessionEvent(type=event_type, data=data, created_at=datetime.now(timezone.utc))

    def _terminal_event(self, session: StreamSession) -> SessionEvent:
        event_type = {SessionStatus.COMPLETED: SessionEventType.COMPLETED, SessionStatus.FAILED: SessionEventType.FAILED, SessionStatus.CANCELLED: SessionEventType.CANCELLED}[session.status]
        return self._event(event_type, {"error_code": session.error_code} if session.error_code else {})

    @staticmethod
    def _validate_cursor(cursor: str | None) -> None:
        if cursor is None:
            return
        valid_shape = re.fullmatch(r"[0-9]{1,20}-[0-9]{1,20}", cursor)
        if valid_shape is not None:
            milliseconds, sequence = (int(part) for part in cursor.split("-"))
            maximum_milliseconds = time.time_ns() // 1_000_000 + 300_000
        if (
            valid_shape is None
            or milliseconds > maximum_milliseconds
            or sequence > (1 << 64) - 1
        ):
            raise GatewayError("invalid_event_cursor", "Invalid stream event cursor", 400)

    def _start_background(self, coroutine, name: str) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task
