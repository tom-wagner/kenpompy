from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Callable, Deque, Generic, Iterable, TypeVar


TaskT = TypeVar("TaskT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class QueueFailure(Generic[TaskT]):
    task: TaskT
    error: str


@dataclass(frozen=True)
class QueueRunResult(Generic[TaskT, ResultT]):
    successes: list[tuple[TaskT, ResultT]]
    dlq: list[QueueFailure[TaskT]]


@dataclass(frozen=True)
class QueueProgress:
    event: str
    total: int
    completed: int
    active: int
    pending: int
    succeeded: int
    failed: int
    max_parallel: int


class ParallelXAIQueue(Generic[TaskT, ResultT]):
    def __init__(self, max_parallel: int = 15):
        if max_parallel < 1:
            raise ValueError("max_parallel must be at least 1")
        self.max_parallel = max_parallel

    def run(
        self,
        tasks: Iterable[TaskT],
        worker: Callable[[TaskT], ResultT],
        progress_callback: Callable[[QueueProgress], None] | None = None,
    ) -> QueueRunResult[TaskT, ResultT]:
        pending: Deque[TaskT] = deque(tasks)
        successes: list[tuple[TaskT, ResultT]] = []
        dlq: list[QueueFailure[TaskT]] = []
        total = len(pending)

        if not pending:
            return QueueRunResult(successes=successes, dlq=dlq)

        def report_progress(event: str, *, active_count: int) -> None:
            if progress_callback is None:
                return
            progress_callback(
                QueueProgress(
                    event=event,
                    total=total,
                    completed=len(successes) + len(dlq),
                    active=active_count,
                    pending=len(pending),
                    succeeded=len(successes),
                    failed=len(dlq),
                    max_parallel=self.max_parallel,
                )
            )

        with ThreadPoolExecutor(max_workers=self.max_parallel) as executor:
            active: dict[Future[ResultT], TaskT] = {}

            def submit_until_full() -> None:
                while pending and len(active) < self.max_parallel:
                    task = pending.popleft()
                    active[executor.submit(worker, task)] = task

            submit_until_full()
            report_progress("started", active_count=len(active))

            while active:
                done, _ = wait(active.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    task = active.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:  # pragma: no cover - depends on runtime failures
                        dlq.append(QueueFailure(task=task, error=str(exc)))
                    else:
                        successes.append((task, result))
                submit_until_full()
                report_progress("progress", active_count=len(active))

        report_progress("finished", active_count=0)

        return QueueRunResult(successes=successes, dlq=dlq)
