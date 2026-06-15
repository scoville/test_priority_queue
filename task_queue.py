import asyncio
import contextlib
import inspect
import logging
from collections import deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

LOGGER = logging.getLogger(__name__)


class TaskLevel(IntEnum):
    """Level for queued task. Higher values dominate lower values."""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class QueuedTask:
    """A task waiting to be dequeued and executed by a consumer."""

    level: TaskLevel = field(
        metadata={"description": " Task level. Higher values dominate lower values."}
    )
    name: str = field(metadata={"description": "Task name for logging and debugging."})
    coroutine: Coroutine[Any, Any, Any] = field(
        metadata={"description": "Coroutine to run when scheduled by a consumer."}
    )
    can_interrupt_running: bool = field(
        default=False,
        metadata={
            "description": "If True and level is strictly greater than the running level when, "
            "this task in the preempt slot instead of the deques."
        },
    )
    timeout: float = field(
        default=120, metadata={"description": "Timeout for the coroutine in seconds."}
    )


class LevelFilteredTaskQueue:
    """Sequential asyncio task queue with level-based filtering and preemption.

    Tasks are executed one at a time in a dedicated background processor.
    Each incomming task has an associated priority level that determines
    whether it is accepted, queued, or rejected.

    The queue enforces the following behaviours:

    * Incomming Tasks with a lower level than the highest active level
      (running or queued) are rejected.
    * Newly incomming higher-level tasks evict any lower-level tasks
      waiting at the back of the queue.
    * A higher-level front-of-queue task may preempt the currently running task if
      ``can_interrupt_running`` is ``True``.
    * An optional idle callback is invoked after the queue becomes idle and
      no new task is added to queue before the callback is scheduled.

    This class is intended for use from coroutines running on a single
    asyncio event loop. It is not thread-safe.

    How to use:
    1. Create a ``QueuedTask`` object.
    2. Submit the task through:
        - `await level_filtered_task_queue.submit_task(queued_task)` in async function or
        - `async_multi_task_manager.create_task(level_filtered_task_queue.submit_task(queued_task))`
           in sync function
    """

    def __init__(self, on_queue_idle: Callable[..., Any] | None = None) -> None:
        """Constructor.

        :param on_queue_idle: sync or async callback invoked with no arguments when the queue
                            drains
                            eg. For manual turn taking, set user turn or for auto turn taking,
                            enable user mic
                            It can bind arguments ahead of time with functools.partial,
                            e.g. on_queue_idle=partial(callback_function, argument1, argument2,
                            ...).
                            If new task is added to queue while an async idle callback is running,
                            the callback is cancelled.
        """
        self._queue: deque[QueuedTask] = deque()

        # Callback to invoke when the queue drains
        self._on_queue_idle = on_queue_idle
        # Current running task
        self._running_task: QueuedTask | None = None
        # Current execution of asyncio.Task for current running task's coroutine
        self._current_execution: asyncio.Task | None = None
        # Whether processor is actively processing a queued task.
        self._is_queue_processing = False
        # Condition variable synchronizing queue access and processor wakeups
        self._cv = asyncio.Condition()
        # Incremented on each accepted submit; used to detect stale idle notifications
        self._activity_generation = 0
        # Long-lived processor that continuously process queued tasks.
        self._processor: asyncio.Task | None = None
        # Idle callback task that waits for the queue to drain and invokes the idle callback.
        self._idle_task: asyncio.Task | None = None

    def _ensure_processor_started_locked(self) -> None:
        """Start the long-lived processor. Caller must hold ``_cv``."""
        if self._processor is None or self._processor.done():
            self._processor = asyncio.create_task(self._run_processor())

    def _cancel_idle_task_locked(self) -> None:
        """Cancel an in-flight idle callback. Caller must hold ``_cv``."""
        if self._idle_task is not None and not self._idle_task.done():
            self._idle_task.cancel()

    def _highest_active_level_locked(self) -> TaskLevel | None:
        """Return the highest level among running and queued tasks. Caller must hold ``_cv``.

        :return: The highest level among running and queued tasks.
                 If there are no running or queued tasks, return None.
        """
        max_level = None
        if self._running_task:
            max_level = self._running_task.level
        for queued_task in self._queue:
            if max_level is None or queued_task.level > max_level:
                max_level = queued_task.level
        return max_level

    async def is_queue_processing(self) -> bool:
        """Return whether processor is actively executing or dequeuing work.

        :return:  ``True`` while work is being processed, otherwise ``False`` when the processor
                    is waiting for a new task.
        """
        async with self._cv:
            return self._is_queue_processing

    async def submit_task(self, task: QueuedTask) -> bool:
        """Enqueue a incomming task.
        :param task: incoming task to be enqueued.
        :return: ``True`` if accepted; ``False`` if rejected.
        """
        async with self._cv:
            self._ensure_processor_started_locked()
            highest_active_level = self._highest_active_level_locked()
            # Reject incomming task if a strictly higher-level task is already running or queued
            if highest_active_level is not None and task.level < highest_active_level:
                LOGGER.warning(
                    "Rejecting incoming task %s (level=%s) because its level is lower than "
                    "the highest active level %s",
                    task.name,
                    task.level.name,
                    highest_active_level.name,
                )
                return False

            # Evict all tasks from the back that have a strictly lower level than incoming task
            while self._queue and self._queue[-1].level < task.level:
                evicted = self._queue.pop()
                LOGGER.debug(
                    "Evicted queued task %s (level=%s) from queue because it is lower than the "
                    "incoming task %s (level=%s).",
                    evicted.name,
                    evicted.level.name,
                    task.name,
                    task.level.name,
                )

            # Allow tasks to be enqueued
            self._queue.append(task)
            self._activity_generation += 1
            self._cancel_idle_task_locked()

            # Check whether there is a current execution of asyncio.Task
            # for current running task's coroutine
            if self._current_execution and not self._current_execution.done():
                # Atomically inspect the next task in queue
                next_task_in_queue = self._queue[0]
                running_task = self._running_task
                # Preemption is triggered if the next task in queue outranks
                # the currently running task and is flagged for preemption
                if (
                    running_task is not None
                    and next_task_in_queue.level > running_task.level
                    and next_task_in_queue.can_interrupt_running
                ):
                    LOGGER.debug(
                        "Task %s (level=%s) in queue with can_interrupt_running=True interrupt "
                        "running task %s (level=%s)",
                        task.name,
                        task.level.name,
                        running_task.name,
                        running_task.level.name,
                    )

                    # Cancel running coroutine (triggers CancelledError)
                    self._current_execution.cancel()

            # Wake up a processor if it is waiting for a new task to be added to queue
            self._cv.notify()
            return True

    async def _run_processor(self) -> None:
        """Run the queue processor indefinitely.

        If the processor exits unexpectedly because of an unhandled exception,
        it is automatically restarted so queued work is not lost. Cancellation
        is propagated normally to allow clean shutdown.
        """
        while True:
            try:
                await self._process_queue()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Queue processor failed unexpectedly; restarting")
                await asyncio.sleep(0)
                async with self._cv:
                    if self._queue:
                        self._cv.notify()

    async def _process_queue(self) -> None:
        """Continuously process queued tasks.

        Waits for work when the queue is empty, then executes tasks one at a time in FIFO order
        after any level-based filtering performed during submission.

        Each task is executed with its configured timeout. When the queue becomes idle,
        the configured idle callback is invoked if no new work has arrived.
        """
        idle_callback = self._on_queue_idle

        while True:
            async with self._cv:
                while not self._queue:
                    self._running_task = None
                    self._current_execution = None
                    self._is_queue_processing = False
                    await self._cv.wait()

                self._is_queue_processing = True
                task = self._queue.popleft()
                self._running_task = task
                execution = asyncio.create_task(task.coroutine)
                self._current_execution = execution

            try:
                await asyncio.wait_for(asyncio.shield(execution), timeout=task.timeout)
                LOGGER.info("Finished: '%s' (Level %s)", task.name, task.level.name)
            except TimeoutError:
                LOGGER.warning("Timeout: '%s' (Level %s)", task.name, task.level.name)
                execution.cancel()
            except asyncio.CancelledError:
                LOGGER.info("Aborted: '%s' (Level %s)", task.name, task.level.name)
            except Exception as e:  # noqa: BLE001
                LOGGER.warning(
                    "Failed with Error: '%s' (Level %s): %s", task.name, task.level.name, e
                )
            finally:
                await self._await_execution_finished(execution)

            async with self._cv:
                if not self._queue:
                    self._running_task = None
                    self._current_execution = None
                    self._is_queue_processing = False

            if callable(idle_callback):
                await self._invoke_idle_if_still_idle(idle_callback)

    @staticmethod
    async def _await_execution_finished(execution: asyncio.Task[Any]) -> None:
        """Wait for a task to finish, including cancellation cleanup.

        Suppresses ``CancelledError`` so callers can safely await a cancelled task
        without propagating the exception.

        :param execution: The asyncio.Task to wait for.
        """
        if execution.done():
            with contextlib.suppress(asyncio.CancelledError):
                await execution
            return
        with contextlib.suppress(asyncio.CancelledError):
            await execution

    async def _invoke_idle_if_still_idle(self, idle_callback: Callable[..., Any]) -> None:
        """Invoke the idle callback only if the queue remains idle.

        A generation counter is used to detect whether new work arrived after the
        queue became empty but before the callback was scheduled. If work arrives
        during the callback, the callback is cancelled.

        :param idle_callback: The idle callback to execute.
        """
        async with self._cv:
            if self._queue or self._is_queue_processing:
                return
            idle_generation = self._activity_generation

        # Let submit_task waiters that were blocked on the condition run before we notify idle.
        await asyncio.sleep(0)

        async with self._cv:
            still_idle = (
                not self._queue
                and not self._is_queue_processing
                and self._activity_generation == idle_generation
            )
            if not still_idle:
                return
            idle_task = asyncio.create_task(self._execute_idle_callback(idle_callback))
            self._idle_task = idle_task

        try:
            await idle_task
        except asyncio.CancelledError:
            LOGGER.debug("Idle callback cancelled because new queued task arrived")
        finally:
            async with self._cv:
                if self._idle_task is idle_task:
                    self._idle_task = None

    @staticmethod
    async def _execute_idle_callback(idle_callback: Callable[..., Any]) -> None:
        """Execute the idle callback.

        Supports both synchronous and asynchronous callbacks. Awaitable results are
        awaited before returning.

        :param idle_callback: The idle callback to execute.
        """
        result = idle_callback()
        if inspect.isawaitable(result):
            await result
