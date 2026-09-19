import asyncio
from collections.abc import Awaitable, Callable


ProducerFactory = Callable[[], Awaitable[None]]


class ProducerSupervisor:
    def __init__(
        self,
        *,
        max_active: int,
        max_queued: int,
        shutdown_grace_seconds: float,
    ) -> None:
        self._capacity = max_active + max_queued
        self._semaphore = asyncio.Semaphore(max_active)
        self._shutdown_grace_seconds = shutdown_grace_seconds
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._queued: set[str] = set()
        self._accepting = True

    @property
    def queued_session_ids(self) -> tuple[str, ...]:
        return tuple(self._queued)

    @property
    def has_active_tasks(self) -> bool:
        return any(not task.done() for task in self._tasks.values())

    def owns(self, session_id: str) -> bool:
        task = self._tasks.get(session_id)
        return task is not None and not task.done()

    @property
    def can_accept(self) -> bool:
        return self._accepting and len(self._tasks) < self._capacity

    async def start(self) -> None:
        self._accepting = True

    async def schedule(
        self, session_id: str, producer_factory: ProducerFactory
    ) -> bool:
        if not self.can_accept or session_id in self._tasks:
            return False
        self._queued.add(session_id)
        task = asyncio.create_task(
            self._run(session_id, producer_factory), name=f"stream-session-{session_id}"
        )
        self._tasks[session_id] = task
        task.add_done_callback(
            lambda completed, key=session_id: self._remove_completed(key, completed)
        )
        return True

    async def cancel(self, session_id: str) -> bool:
        task = self._tasks.get(session_id)
        if task is None or task.done():
            return False
        task.cancel()
        return True

    async def cancel_queued(self, session_id: str) -> bool:
        if session_id not in self._queued:
            return False
        task = self._tasks.get(session_id)
        if task is None:
            return False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._remove_completed(session_id, task)
        return True

    async def wait_idle(self) -> None:
        while self._tasks:
            tasks = list(self._tasks.values())
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0)

    async def stop(self) -> None:
        self._accepting = False
        if not self._tasks:
            return
        _, pending = await asyncio.wait(
            list(self._tasks.values()), timeout=self._shutdown_grace_seconds
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=self._shutdown_grace_seconds)

    async def _run(self, session_id: str, producer_factory: ProducerFactory) -> None:
        async with self._semaphore:
            self._queued.discard(session_id)
            await producer_factory()

    def _remove_completed(
        self, session_id: str, completed: asyncio.Task[None]
    ) -> None:
        self._queued.discard(session_id)
        if self._tasks.get(session_id) is completed:
            self._tasks.pop(session_id, None)
