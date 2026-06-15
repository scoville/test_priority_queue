---
name: System Architecture Workflow
overview: System Architecture Workflow for the serialized async priority task queue in [task_queue.py](task_queue.py), covering LevelFilteredTaskQueue components, data flow, concurrency, optional on_queue_idle hook (functools.partial), and all required behaviors from [README.md](README.md) plus on_queue_idle tests in [test_task_queue.py](test_task_queue.py).
todos: []
isProject: false
---

# System Architecture Workflow

## High-Level Overview

This system is a **single-queue, serialized async task queue** with level-based filtering. Producers `await submit_task(QueuedTask)` on `LevelFilteredTaskQueue`; exactly one coroutine runs at a time, with optional preemption on submit, queue eviction, per-task timeouts, and an optional idle callback.

A **single long-lived processor** (`_run_processor` → `_process_queue`) is started lazily on the first accepted `submit_task` and blocks on `asyncio.Condition.wait()` when the queue is empty. New work wakes it via `notify()`.

```mermaid
flowchart TB
    subgraph producers [Producers]
        App[Application / Tests]
    end

    subgraph queue [LevelFilteredTaskQueue]
        CV[asyncio.Condition _cv]
        Deque["_queue: deque of QueuedTask"]
        Submit[submit_task]
        RunProc[_run_processor]
        Process[_process_queue]
        Running[_running_task]
        Exec[_current_execution]
        Guard[_is_queue_processing]
        Processor[_processor]
        IdleTask[_idle_task]
        Gen[_activity_generation]
        IdleHook["on_queue_idle (optional)"]
    end

    App -->|await submit_task| Submit
    App -->|partial callback at init| IdleHook
    App -->|await is_queue_processing| CV
    Submit --> Deque
    Submit -->|_ensure_processor_started_locked| RunProc
    Submit -->|notify| CV
    Submit -->|cancel if in flight| IdleTask
    RunProc --> Process
    Process -->|wait when empty| CV
    Process --> Deque
    Process --> Running
    Process --> Exec
    Process --> Guard
    Process -->|queue drains| IdleHook
    Deque --> CV
    Processor --> RunProc
```

## Core Components

| Component | Responsibility |
|-----------|----------------|
| [`TaskLevel`](task_queue.py) | Priority enum: `LOW(0) < NORMAL(1) < HIGH(2) < CRITICAL(3)` |
| [`QueuedTask`](task_queue.py) | Task payload: `level`, `name`, `coroutine`, `can_interrupt_running` (default `False`), `timeout` (default `120` seconds) |
| [`LevelFilteredTaskQueue`](task_queue.py) | Enqueue filtering, preemption, queue draining, execution, and optional idle notification |

### Queue State (private internals)

| Field | Purpose |
|-------|---------|
| `_queue` | `deque[QueuedTask]` — FIFO waiting tasks |
| `_running_task` | Currently executing `QueuedTask` |
| `_current_execution` | `asyncio.Task` wrapping the running coroutine |
| `_is_queue_processing` | `True` while dequeuing or executing; `False` while blocked on an empty queue |
| `_on_queue_idle` | Optional `Callable[..., Any] \| None` — sync or async hook invoked when the queue fully drains |
| `_cv` | `asyncio.Condition` — synchronizes all queue mutations, processor wakeups, and public snapshots |
| `_activity_generation` | Incremented on each accepted submit; used to detect stale idle notifications |
| `_processor` | Handle to the long-lived `_run_processor` asyncio task |
| `_idle_task` | Handle to an in-flight idle callback; cancelled by `submit_task` when new work arrives |

### Public async accessor

External reads of processing state go through a single method that acquires `_cv`:

| Method | Returns |
|--------|---------|
| `is_queue_processing()` | `True` while the processor is actively dequeuing or executing; `False` while blocked waiting for work |

Internal helper `_highest_active_level_locked()` computes the max `TaskLevel` among running and queued tasks; it is used by `submit_task` while already holding `_cv` and is not exposed publicly.

### Usage from sync code

`submit_task` is async and must run on the event loop. From a synchronous caller, schedule it with `asyncio.create_task(level_filtered_task_queue.submit_task(queued_task))` (or an equivalent task manager) rather than calling it directly.

### Constructor: `on_queue_idle`

Optional callback for application logic when all queued work finishes (e.g. manual turn-taking → set user turn; auto turn-taking → enable mic).

```python
from functools import partial

async def on_idle(mode: str, mic_controller) -> None:
    if mode == "manual":
        set_user_turn()
    else:
        await mic_controller.enable()

queue = LevelFilteredTaskQueue(
    on_queue_idle=partial(on_idle, "manual", mic_controller),
)
```

- **Type:** `Callable[..., Any] | None` (default `None` — no hook).
- **Invocation:** `idle_callback()` with no arguments; bind values ahead of time via `functools.partial`.
- **Sync or async:** return values are inspected with `inspect.isawaitable`; async callbacks are awaited.
- **When:** after the last task in a drain cycle finishes and `_queue` is empty.
- **Race safety:** double-check under `_cv` using `_activity_generation`, plus `asyncio.sleep(0)` to let blocked submitters run; in-flight idle callbacks are cancelled when new work is accepted.
- **Lock discipline:** the callback runs **outside** `_cv` so it can safely call `submit_task` without deadlock.

## Lifecycle: Submit to Completion

```mermaid
sequenceDiagram
    participant App
    participant Queue as LevelFilteredTaskQueue
    participant Deque as _queue
    participant CV as asyncio.Condition
    participant Proc as _run_processor
    participant Exec as asyncio.Task

    App->>Queue: await submit_task(task)
    Queue->>CV: async with _cv
    Queue->>Proc: _ensure_processor_started_locked (first submit only)
    Queue->>Deque: reject / evict / append
    Queue->>Queue: _activity_generation += 1
    Queue->>Queue: _cancel_idle_task_locked if idle in flight
    alt preempt conditions met
        Queue->>Exec: cancel _current_execution
    end
    Queue->>CV: notify()

    loop long-lived processor
        Queue->>CV: async with _cv
        alt queue empty
            Queue->>CV: _cv.wait()
        else
            Queue->>Deque: popleft
            Queue->>Queue: _running_task = task
            Queue->>Exec: create_task(coroutine)
        end
        Queue->>Exec: await wait_for(shield, timeout)
        Queue->>Exec: await _await_execution_finished
        alt success
            Exec-->>Queue: Finished log
        else timeout
            Exec-->>Queue: TimeoutError, cancel
        else preempted
            Exec-->>Queue: CancelledError
        end
        opt queue empty
            Queue->>Queue: _invoke_idle_if_still_idle
        end
    end
```

## Workflow 1: Task Submission (`submit_task`)

All submission logic runs under `asyncio.Condition` in [`LevelFilteredTaskQueue.submit_task`](task_queue.py).

```mermaid
flowchart TD
    Start[submit_task called] --> Lock[async with _cv]
    Lock --> Ensure[_ensure_processor_started_locked]
    Ensure --> CheckActive{"incoming level < _highest_active_level_locked()?"}
    CheckActive -->|Yes| Reject["Return False - Behavior 4"]
    CheckActive -->|No| EvictLoop{Back task level < incoming level?}
    EvictLoop -->|Yes| PopBack[pop from back - Behavior 3]
    PopBack --> EvictLoop
    EvictLoop -->|No| Append[append to _queue]
    Append --> Gen[_activity_generation += 1]
    Gen --> CancelIdle[_cancel_idle_task_locked]
    CancelIdle --> PreemptCheck{Queue front outranks running AND can_interrupt_running?}
    PreemptCheck -->|Yes| Cancel[cancel _current_execution - Behavior 2]
    PreemptCheck -->|No| Notify
    Cancel --> Notify[_cv.notify]
    Notify --> Accept[Return True]
    Reject --> Unlock[Release _cv]
    Accept --> Unlock
```

**Priority rules on enqueue:**

- **Reject (Behavior 4):** `_highest_active_level_locked()` returns the max level among the running task and all queued tasks. Incoming tasks strictly below that level are rejected (covers both higher-queued and higher-running cases).
- **Evict (Behavior 3):** Remove all strictly lower-level tasks from the back before appending.
- **Preempt (Behavior 2):** After append, if queue front (`_queue[0]`) outranks `_running_task` and has `can_interrupt_running=True`, cancel `_current_execution` immediately — preemption is triggered at submit time, not via a polling loop.
- **Wake processor:** `_cv.notify()` wakes the long-lived processor if it is blocked on `_cv.wait()`.

Rejected and evicted tasks are logged at `WARNING` and `DEBUG` respectively via the module `LOGGER`.

## Workflow 2: Queue Processing (`_run_processor` / `_process_queue`)

`_run_processor` wraps `_process_queue` in a restart loop for crash recovery. `asyncio.CancelledError` is re-raised so the processor can shut down cleanly; any other exception is logged and the loop restarts, notifying the condition if work remains queued.

`_process_queue` is an infinite loop that waits on `_cv` when idle and dequeues work when notified.

```mermaid
flowchart TD
    Start[_run_processor started on first submit] --> TryLoop[try await _process_queue]
    TryLoop -->|CancelledError| Shutdown[re-raise for clean shutdown]
    TryLoop -->|other exception| LogCrash[log exception + sleep 0]
    LogCrash --> NotifyIfWork[notify if _queue non-empty]
    NotifyIfWork --> TryLoop
    TryLoop --> Inner[_process_queue infinite loop]
    Inner --> WaitEmpty{queue empty?}
    WaitEmpty -->|Yes| Block["async with _cv: clear running state, _is_queue_processing = False, _cv.wait()"]
    Block --> WaitEmpty
    WaitEmpty -->|No| LockPop["async with _cv: popleft, set _running_task, create execution"]
    LockPop --> Wait["await wait_for(shield, timeout) outside _cv"]
    Wait --> Join[await _await_execution_finished]
    Join --> Outcome{result}
    Outcome -->|success| LogDone[log Finished]
    Outcome -->|TimeoutError| LogTimeout[cancel + log Timeout - Behavior 5]
    Outcome -->|CancelledError| LogAbort[log Aborted - Behavior 2]
    Outcome -->|Exception| LogError[log Failed]
    LogDone --> ClearIfEmpty[clear running state if _queue empty]
    LogTimeout --> ClearIfEmpty
    LogAbort --> ClearIfEmpty
    LogError --> ClearIfEmpty
    ClearIfEmpty --> IdleCheck{callable on_queue_idle?}
    IdleCheck -->|Yes| InvokeIdle[_invoke_idle_if_still_idle]
    IdleCheck -->|No| Inner
    InvokeIdle --> Inner
```

**Behavior mapping:**

- **Behavior 1 (one at a time):** `_process_queue` awaits each coroutine (plus `_await_execution_finished`) before dequeuing the next; only one `_processor` task is ever started.
- **Behavior 2 (preemption):** Cancel on submit raises `CancelledError` in the running `wait_for`; processor joins the cancelled execution, then continues to next queued task.
- **Behavior 2 inverse:** Lower-level tasks never preempt — preemption requires `_queue[0].level > running_task.level` and `can_interrupt_running=True`.
- **Behavior 5 (timeout):** `asyncio.wait_for(asyncio.shield(...), timeout)` cancels overlong tasks; loop continues to next item after join.
- **Idle hook:** When the queue empties after a task finishes, optional `on_queue_idle` fires once per drain cycle (not while tasks remain queued).

## Workflow 3: Task Execution

```mermaid
sequenceDiagram
    participant Process as _process_queue
    participant AsyncTask as asyncio.Task
    participant Coroutine

    Process->>Process: _running_task = task, execution = create_task(coroutine)
    Process->>AsyncTask: await wait_for(shield, timeout)

    alt completes in time
        Coroutine-->>AsyncTask: return
        AsyncTask-->>Process: log Finished
    else exceeds timeout
        AsyncTask-->>Process: TimeoutError - Behavior 5
        Process->>AsyncTask: cancel
    else preempted on submit
        AsyncTask-->>Process: CancelledError - Behavior 2
    else coroutine error
        AsyncTask-->>Process: Exception log
    end

    Process->>AsyncTask: await _await_execution_finished
    Note over Process,AsyncTask: ensures cancelled/timed-out coroutine fully exits before next dequeue

    Process->>Process: continue loop or invoke idle hook
    opt queue empty and on_queue_idle set
        Process->>Process: _invoke_idle_if_still_idle
        Process->>App: idle_callback()
    end
```

**Execution detail:** Task name and level are captured in local variables at dequeue time for logging (no unsynchronized reads of `_running_task`). `asyncio.shield` prevents the inner task from being immediately destroyed on timeout/cancel at the `wait_for` boundary; `_await_execution_finished` then joins the execution task before the next item is dequeued, suppressing `CancelledError` from propagating to the processor.

## Workflow 4: Queue Idle Hook (`on_queue_idle`)

Application code registers an optional sync or async callback at queue construction. Arguments are bound with `functools.partial` before passing the callable in — the queue always invokes it with `idle_callback()` and no extra parameters.

```mermaid
sequenceDiagram
    participant Process as _process_queue
    participant CV as asyncio.Condition
    participant IdleTask as _idle_task
    participant Hook as on_queue_idle
    participant Submit as submit_task

    Process->>Process: last task finished, _queue empty
    Process->>CV: acquire — check _queue, _is_queue_processing, capture idle_generation
    Process->>CV: release
    Process->>Process: asyncio.sleep(0)
    Process->>CV: acquire — still_idle check with generation
    Process->>IdleTask: create_task(_execute_idle_callback)
    Process->>CV: release
    Process->>IdleTask: await

    alt new work arrives during async idle
        Submit->>CV: acquire
        Submit->>Submit: _activity_generation += 1
        Submit->>IdleTask: cancel via _cancel_idle_task_locked
        Submit->>CV: notify
        IdleTask-->>Process: CancelledError
    else idle completes normally
        IdleTask->>Hook: idle_callback() / await result
    end
```

| Concern | Approach |
|---------|----------|
| Turn-taking / mic control | App logic inside `on_queue_idle`; queue stays domain-agnostic |
| Passing context (mode, controllers) | `functools.partial(handler, arg1, arg2, ...)` at init |
| Concurrent submit before idle fires | `_activity_generation` double-check + `sleep(0)` skips stale idle |
| Submit during async idle callback | `_cancel_idle_task_locked()` cancels `_idle_task` |
| Callback submits a new task | Safe — hook runs outside `_cv` |

## Workflow 5: Concurrency Model

```mermaid
flowchart LR
    subgraph asyncSafe [Asyncio-cooperative concurrency]
        Submit[submit_task]
        Process[_process_queue]
        CV[asyncio.Condition _cv]
        Submit -->|async with| CV
        Process -->|popleft + state under _cv| Deque
        Process -->|await coroutine + join| OutsideCV[outside _cv]
        Accessor[is_queue_processing]
        Accessor -->|async with| CV
    end

    subgraph guards [Concurrency guards]
        ProcTask[_processor - single long-lived processor]
        Gen[_activity_generation - stale idle detection]
        IdleCancel[_idle_task cancel on submit]
        Join[_await_execution_finished - no coroutine overlap]
        Crash[_run_processor restart on exception]
    end
```

| Concern | Mechanism |
|---------|-----------|
| Concurrent `submit_task` calls | `_cv` wraps reject/evict/append/preempt decision |
| Multiple processor loops | `_ensure_processor_started_locked` only under `_cv`; one `_processor` task |
| Processor blocked on empty queue | `_cv.wait()` until `notify()` from `submit_task` |
| Preemption race | Preempt check runs inside `submit_task` under `_cv` before releasing |
| Coroutine overlap after cancel | `_await_execution_finished` joins execution before next dequeue |
| Stale idle notification | `_activity_generation` double-check + cancel in-flight `_idle_task` |
| Unsynchronized state reads | Private `_queue` / `_running_task`; only `is_queue_processing()` is public |
| Processor crash | `_run_processor` logs, restarts, and `notify()`s if work is queued |
| Processor shutdown | `CancelledError` propagates out of `_run_processor` without restart |
| Cross-thread submit | **Not supported** — single event loop, coroutine-only use |

## End-to-End Example (Preemption + Eviction)

```mermaid
sequenceDiagram
    participant App
    participant Queue as LevelFilteredTaskQueue
    participant Deque as _queue

    App->>Queue: await submit_task LOW
    Queue->>Deque: enqueue LOW
    Queue->>Queue: _run_processor starts, LOW dequeued

    App->>Queue: await submit_task HIGH can_interrupt=True
    Queue->>Deque: enqueue HIGH
    Queue->>Queue: cancel LOW _current_execution
    Note over Queue: LOW CancelledError, join LOW, HIGH dequeued next

    App->>Queue: await submit_task CRITICAL
    Note over Deque: evicts HIGH from back
    Queue->>Deque: enqueue CRITICAL

    Note over Queue: after LOW abort path, CRITICAL runs
```

## State Machine (Queue)

```mermaid
stateDiagram-v2
    [*] --> Waiting: queue created
    Waiting --> Processing: first submit starts _processor + notify
    Processing --> Running: popleft, set _running_task
    Running --> Running: await coroutine + join
    Running --> Running: preempted, join, next task dequeued
    Running --> Waiting: _queue empty, _is_queue_processing = False, _cv.wait()
    Waiting --> IdleHook: _invoke_idle_if_still_idle
    IdleHook --> Waiting: idle done or cancelled
    Waiting --> Processing: submit_task notify
```

## Required Behaviors and Tests

[README.md](README.md) documents behaviors 1–5. [test_task_queue.py](test_task_queue.py) additionally verifies `on_queue_idle` (12 tests total).

| Behavior | Description | Test |
|----------|-------------|------|
| 1 | Only one task runs at a time | `test_only_one_task_runs_at_a_time` |
| 2a | No preempt by default | `test_higher_level_not_interrupt_running_task_with_can_interrupt_running_false` |
| 2b | Preempt when `can_interrupt_running=True` | `test_higher_level_interrupt_running_task_with_can_interrupt_running_true` |
| 3 | Higher-level incoming evicts lower queued | `test_higher_level_incoming_tasks_evicts_lower_queued` |
| 4a | Reject when higher-level queued | `test_do_not_enqueue_incoming_task_when_higher_queued` |
| 4b | Reject when higher-level running | `test_do_not_enqueue_incoming_task_when_higher_running` |
| 5 | Timeout stops running task | `test_task_times_out_when_exceeding_limit` |
| 6 | `on_queue_idle` not called between back-to-back tasks | `test_on_queue_idle_not_called_between_back_to_back_tasks` |
| 7 | `on_queue_idle` runs once when queue drains | `test_on_queue_idle_runs_once_when_queue_drains` |
| 8 | `functools.partial` binds args (sync callback) | `test_on_queue_idle_partial_binds_args_before_level_filtered_task_queue` |
| 9 | Async `on_queue_idle` is awaited | `test_on_queue_idle_awaits_async_callback` |
| 10 | `functools.partial` binds args (async callback) | `test_on_queue_idle_partial_binds_args_for_async_callback` |

## Key Files

- Implementation: [task_queue.py](task_queue.py) — `TaskLevel`, `QueuedTask`, `LevelFilteredTaskQueue`
- Behavior specs: [README.md](README.md) — behaviors 1–5
- Verification: [test_task_queue.py](test_task_queue.py) — 12 tests (behaviors 1–5 plus `on_queue_idle`)

## Known Design Notes

- `submit_task` is **async** — uses `asyncio.Condition` and must be called from the event loop; from sync code, schedule via `asyncio.create_task(...)`.
- Preemption is **submit-driven** (cancel inside `submit_task`), not poll-driven.
- A **single long-lived processor** (`_run_processor` → `_process_queue`) waits on `_cv` when idle; it is started lazily on the first accepted submit under `_cv`.
- `_process_queue` pops and sets `_running_task` / `_current_execution` under `_cv`, but awaits each coroutine outside `_cv`; `_await_execution_finished` joins the execution before the next dequeue.
- `_is_queue_processing` is `True` while dequeuing or executing, `False` while blocked on `_cv.wait()` — query via `await is_queue_processing()`.
- Queue state (`_queue`, `_running_task`) is private; `is_queue_processing()` is the only public state accessor.
- `on_queue_idle` is optional, sync or async, invoked outside `_cv` with generation-based stale-idle guards; in-flight async idle callbacks are cancelled when new work is accepted.
- `_run_processor` restarts automatically on unexpected exceptions; `CancelledError` propagates for clean shutdown.
- Default per-task `timeout` is **120 seconds** (`QueuedTask.timeout`).
- **Not thread-safe** across OS threads — single event loop, coroutine-only use.
