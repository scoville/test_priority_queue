import asyncio
from functools import partial

import pytest
from task_queue import (
    LevelFilteredTaskQueue,
    QueuedTask,
    TaskLevel,
)


# Required Behavior 1: Only one task runs at a time
@pytest.mark.asyncio
async def test_only_one_task_runs_at_a_time() -> None:
    level_filtered_task_queue = LevelFilteredTaskQueue()

    active = 0
    max_active = 0
    lock = asyncio.Lock()

    async def work(_label: str, delay: float) -> None:
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(delay)
        async with lock:
            active -= 1

    task_a = QueuedTask(level=TaskLevel.NORMAL, name="a", coroutine=work("a", 0.05))
    task_b = QueuedTask(level=TaskLevel.NORMAL, name="b", coroutine=work("b", 0.05))

    assert await level_filtered_task_queue.submit_task(task_a)
    assert await level_filtered_task_queue.submit_task(task_b)
    await asyncio.sleep(0.2)
    assert max_active == 1


# Required Behavior 2a: Higher level task from queue do not interrupt running task if
# can_interrupt_running is False
@pytest.mark.asyncio
async def test_higher_level_not_interrupt_running_task_with_can_interrupt_running_false() -> None:
    level_filtered_task_queue = LevelFilteredTaskQueue()

    low_cancelled = asyncio.Event()
    order: list[str] = []

    async def low() -> None:
        try:
            await asyncio.sleep(0.2)
            order.append("low_done")
        except asyncio.CancelledError:
            low_cancelled.set()
            raise

    async def high() -> None:
        await asyncio.sleep(0)
        order.append("high_done")

    task_low = QueuedTask(level=TaskLevel.LOW, name="low", coroutine=low())
    task_high = QueuedTask(level=TaskLevel.HIGH, name="high", coroutine=high())

    # long sleep in low task, so it has been running when high task are submitted
    assert await level_filtered_task_queue.submit_task(task_low)
    await asyncio.sleep(0.02)
    assert await level_filtered_task_queue.submit_task(task_high)
    await asyncio.sleep(0.35)
    assert not low_cancelled.is_set()
    assert order == ["low_done", "high_done"]


# Required Behavior 2b: Higher level task from queue interrupt running task if
# can_interrupt_running is True
@pytest.mark.asyncio
async def test_higher_level_interrupt_running_task_with_can_interrupt_running_true() -> None:
    level_filtered_task_queue = LevelFilteredTaskQueue()

    low_cancelled = asyncio.Event()
    order: list[str] = []

    async def low() -> None:
        try:
            await asyncio.sleep(0.2)
            order.append("low_done")
        except asyncio.CancelledError:
            low_cancelled.set()
            raise

    async def high() -> None:
        await asyncio.sleep(0)
        order.append("high_done")

    task_low = QueuedTask(level=TaskLevel.LOW, name="low", coroutine=low())
    task_high = QueuedTask(
        level=TaskLevel.HIGH, name="high", coroutine=high(), can_interrupt_running=True
    )

    # long sleep in low task, so it has been running when high task are submitted
    assert await level_filtered_task_queue.submit_task(task_low)
    await asyncio.sleep(0.02)
    assert await level_filtered_task_queue.submit_task(task_high)
    await asyncio.sleep(0.35)
    assert low_cancelled.is_set()
    assert order == ["high_done"]


# Required Behavior 3: Higher-level incoming tasks evict lower-level tasks from queue
@pytest.mark.asyncio
async def test_higher_level_incoming_tasks_evicts_lower_queued() -> None:
    level_filtered_task_queue = LevelFilteredTaskQueue()

    ran: list[str] = []

    async def low() -> None:
        ran.append("low")
        await asyncio.sleep(0.15)

    async def high() -> None:
        ran.append("high")
        await asyncio.sleep(0.01)

    async def critical() -> None:
        await asyncio.sleep(0)
        ran.append("critical")

    task_low = QueuedTask(level=TaskLevel.LOW, name="low", coroutine=low())
    task_high = QueuedTask(level=TaskLevel.HIGH, name="high", coroutine=high())
    task_critical = QueuedTask(level=TaskLevel.CRITICAL, name="critical", coroutine=critical())

    # long sleep in low task, so it has beee running when high task and critical task are
    # submitted
    assert await level_filtered_task_queue.submit_task(task_low)
    await asyncio.sleep(0.02)
    # high task and critical task are in queue and thus high task is removed from queue
    assert await level_filtered_task_queue.submit_task(task_high)
    await asyncio.sleep(0.02)
    assert await level_filtered_task_queue.submit_task(task_critical)
    await asyncio.sleep(0.4)
    assert ran == ["low", "critical"]


# Required Behavior 4a: Do not enqueue an incoming task if any task queued has strictly
# higher priority
@pytest.mark.asyncio
async def test_do_not_enqueue_incoming_task_when_higher_queued() -> None:
    level_filtered_task_queue = LevelFilteredTaskQueue()

    ran: list[str] = []

    async def blocker() -> None:
        ran.append("blocker")
        await asyncio.sleep(0.2)

    async def high_queued() -> None:
        await asyncio.sleep(0)
        ran.append("high_queued")

    async def rejected() -> None:
        await asyncio.sleep(0)
        ran.append("rejected")

    task_blocker = QueuedTask(level=TaskLevel.NORMAL, name="blocker", coroutine=blocker())
    task_high_queued = QueuedTask(level=TaskLevel.HIGH, name="high_queued", coroutine=high_queued())
    task_rejected = QueuedTask(level=TaskLevel.NORMAL, name="rejected", coroutine=rejected())

    # long sleep in broker task, so it has been running when high_queued task and
    # rejected task are submitted
    assert await level_filtered_task_queue.submit_task(task_blocker)
    await asyncio.sleep(0.02)
    assert await level_filtered_task_queue.submit_task(task_high_queued)
    await asyncio.sleep(0.02)
    # high_queued task is now in queue and try to submit rejected task
    accepted = await level_filtered_task_queue.submit_task(task_rejected)
    # check whether rejected task is not enqueued
    assert not accepted
    await asyncio.sleep(0.35)
    assert ran == ["blocker", "high_queued"]


# Required Behavior 4b: Do not enqueue an incoming task if running task has
# strictly higher level
@pytest.mark.asyncio
async def test_do_not_enqueue_incoming_task_when_higher_running() -> None:
    level_filtered_task_queue = LevelFilteredTaskQueue()

    ran: list[str] = []

    async def blocker() -> None:
        ran.append("blocker")
        await asyncio.sleep(0.2)

    async def rejected() -> None:
        await asyncio.sleep(0)
        ran.append("rejected")

    task_blocker = QueuedTask(level=TaskLevel.HIGH, name="blocker", coroutine=blocker())
    task_rejected = QueuedTask(level=TaskLevel.NORMAL, name="rejected", coroutine=rejected())

    # long sleep in broker task, so it has been running when rejected task are submitted
    assert await level_filtered_task_queue.submit_task(task_blocker)
    await asyncio.sleep(0.02)
    accepted = await level_filtered_task_queue.submit_task(task_rejected)
    # check whether rejected task is not enqueued
    assert not accepted
    await asyncio.sleep(0.35)
    assert ran == ["blocker"]


# Required Behavior 5: Interrupt running task if its predefined task timeout happens
@pytest.mark.asyncio
async def test_task_times_out_when_exceeding_limit() -> None:
    level_filtered_task_queue = LevelFilteredTaskQueue()

    ran: list[str] = []

    async def slow() -> None:
        await asyncio.sleep(1.0)
        ran.append("slow_done")

    async def fast() -> None:
        await asyncio.sleep(0)
        ran.append("fast_done")

    task_slow = QueuedTask(level=TaskLevel.NORMAL, name="slow", coroutine=slow(), timeout=0.05)
    task_fast = QueuedTask(level=TaskLevel.NORMAL, name="fast", coroutine=fast())

    assert await level_filtered_task_queue.submit_task(task_slow)
    await asyncio.sleep(0.15)
    assert "slow_done" not in ran

    assert await level_filtered_task_queue.submit_task(task_fast)
    await asyncio.sleep(0.15)
    assert ran == ["fast_done"]


# Idle callback is not called between back-to-back tasks
@pytest.mark.asyncio
async def test_on_queue_idle_not_called_between_back_to_back_tasks() -> None:
    idle_count = 0
    first_finished = asyncio.Event()

    def on_idle() -> None:
        nonlocal idle_count
        idle_count += 1

    level_filtered_task_queue = LevelFilteredTaskQueue(on_queue_idle=on_idle)

    async def first() -> None:
        await asyncio.sleep(0.05)
        first_finished.set()

    async def second() -> None:
        await asyncio.sleep(0.05)

    assert await level_filtered_task_queue.submit_task(
        QueuedTask(level=TaskLevel.NORMAL, name="first", coroutine=first())
    )

    async def submit_second_on_finish() -> None:
        await first_finished.wait()
        await level_filtered_task_queue.submit_task(
            QueuedTask(level=TaskLevel.NORMAL, name="second", coroutine=second())
        )

    task = asyncio.create_task(submit_second_on_finish())
    await asyncio.sleep(0.08)
    assert idle_count == 0
    await asyncio.sleep(0.15)
    assert idle_count == 1
    await task


# Idle callback is called once when the queue drains
@pytest.mark.asyncio
async def test_on_queue_idle_runs_once_when_queue_drains() -> None:
    idle_count = 0

    def on_idle() -> None:
        nonlocal idle_count
        idle_count += 1

    level_filtered_task_queue = LevelFilteredTaskQueue(on_queue_idle=on_idle)

    async def work(_name: str) -> None:
        await asyncio.sleep(0.05)

    assert await level_filtered_task_queue.submit_task(
        QueuedTask(level=TaskLevel.NORMAL, name="a", coroutine=work("a"))
    )
    await asyncio.sleep(0.02)
    assert idle_count == 0

    assert await level_filtered_task_queue.submit_task(
        QueuedTask(level=TaskLevel.NORMAL, name="b", coroutine=work("b"))
    )
    await asyncio.sleep(0.2)
    assert idle_count == 1
    assert not await level_filtered_task_queue.is_queue_processing()


# Idle callback with partial binds arguments is called once when the queue drains
@pytest.mark.asyncio
async def test_on_queue_idle_partial_binds_args_before_level_filtered_task_queue() -> None:
    received: list[str] = []

    def on_idle(mode: str) -> None:
        received.append(mode)

    level_filtered_task_queue = LevelFilteredTaskQueue(on_queue_idle=partial(on_idle, "manual"))

    async def work() -> None:
        await asyncio.sleep(0.05)

    assert await level_filtered_task_queue.submit_task(
        QueuedTask(level=TaskLevel.NORMAL, name="a", coroutine=work())
    )
    await asyncio.sleep(0.15)
    assert received == ["manual"]


# Aysnc Idle callback is awaited before the queue processor waits for a new task
@pytest.mark.asyncio
async def test_on_queue_idle_awaits_async_callback() -> None:
    idle_count = 0
    callback_completed = asyncio.Event()

    async def on_idle() -> None:
        nonlocal idle_count
        await asyncio.sleep(0.05)
        idle_count += 1
        callback_completed.set()

    level_filtered_task_queue = LevelFilteredTaskQueue(on_queue_idle=on_idle)

    async def work() -> None:
        await asyncio.sleep(0.05)

    assert await level_filtered_task_queue.submit_task(
        QueuedTask(level=TaskLevel.NORMAL, name="a", coroutine=work())
    )
    await asyncio.wait_for(callback_completed.wait(), timeout=1.0)
    assert idle_count == 1
    assert not await level_filtered_task_queue.is_queue_processing()


# Async Idle callback with partial binds arguments is awaited before
# the queue processor waits for a new task
@pytest.mark.asyncio
async def test_on_queue_idle_partial_binds_args_for_async_callback() -> None:
    received: list[str] = []

    async def on_idle(mode: str) -> None:
        await asyncio.sleep(0.02)
        received.append(mode)

    level_filtered_task_queue = LevelFilteredTaskQueue(on_queue_idle=partial(on_idle, "manual"))

    async def work() -> None:
        await asyncio.sleep(0.05)

    assert await level_filtered_task_queue.submit_task(
        QueuedTask(level=TaskLevel.NORMAL, name="a", coroutine=work())
    )
    await asyncio.sleep(0.15)
    assert received == ["manual"]
