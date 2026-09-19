from __future__ import annotations

import threading
import time
from collections.abc import Callable


class ClockRollbackError(RuntimeError):
    """Raised when the system clock moves behind the last emitted timestamp."""


class SnowflakeIdGenerator:
    """Generate process-local Snowflake IDs with a configured worker identity."""

    _WORKER_ID_BITS = 10
    _SEQUENCE_BITS = 12
    _MAX_WORKER_ID = (1 << _WORKER_ID_BITS) - 1
    _MAX_SEQUENCE = (1 << _SEQUENCE_BITS) - 1
    _WORKER_SHIFT = _SEQUENCE_BITS
    _TIMESTAMP_SHIFT = _WORKER_ID_BITS + _SEQUENCE_BITS
    _MAX_TIMESTAMP = (1 << 41) - 1
    DEFAULT_EPOCH_MS = 1_704_067_200_000

    def __init__(
        self,
        worker_id: int,
        *,
        epoch_ms: int = DEFAULT_EPOCH_MS,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if type(worker_id) is not int or not 0 <= worker_id <= self._MAX_WORKER_ID:
            raise ValueError(f"worker_id must be between 0 and {self._MAX_WORKER_ID}")
        if epoch_ms < 0:
            raise ValueError("epoch_ms must be non-negative")
        self._worker_id = worker_id
        self._epoch_ms = epoch_ms
        self._clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._last_timestamp = -1
        self._sequence = 0
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            timestamp = self._clock()
            if timestamp < self._last_timestamp:
                raise ClockRollbackError(
                    "system clock moved backwards after a Snowflake ID was emitted"
                )
            if timestamp == self._last_timestamp:
                self._sequence += 1
                if self._sequence > self._MAX_SEQUENCE:
                    timestamp = self._wait_for_next_millisecond(timestamp)
                    self._sequence = 0
            else:
                self._sequence = 0
            elapsed = timestamp - self._epoch_ms
            if not 0 <= elapsed <= self._MAX_TIMESTAMP:
                raise ValueError("clock timestamp is outside the Snowflake epoch range")
            self._last_timestamp = timestamp
            return (
                (elapsed << self._TIMESTAMP_SHIFT)
                | (self._worker_id << self._WORKER_SHIFT)
                | self._sequence
            )

    def _wait_for_next_millisecond(self, previous: int) -> int:
        timestamp = self._clock()
        while timestamp <= previous:
            if timestamp < previous:
                raise ClockRollbackError("system clock moved backwards while waiting for a Snowflake ID")
            time.sleep(0.0001)
            timestamp = self._clock()
        return timestamp


def format_session_id(snowflake_id: int) -> str:
    if type(snowflake_id) is not int or snowflake_id < 0:
        raise ValueError("snowflake_id must be a non-negative integer")
    return f"session-{snowflake_id}"
