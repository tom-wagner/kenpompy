from kenpompy.xai_queue import ParallelXAIQueue


def test_parallel_xai_queue_collects_successes_and_dlq():
    queue = ParallelXAIQueue(max_parallel=2)

    def worker(value: int) -> int:
        if value == 2:
            raise RuntimeError("boom")
        return value * 10

    result = queue.run([1, 2, 3], worker)

    assert sorted(result.successes) == [(1, 10), (3, 30)]
    assert len(result.dlq) == 1
    assert result.dlq[0].task == 2
    assert result.dlq[0].error == "boom"


def test_parallel_xai_queue_reports_progress():
    queue = ParallelXAIQueue(max_parallel=2)
    snapshots = []

    def worker(value: int) -> int:
        return value * 10

    result = queue.run([1, 2, 3], worker, progress_callback=snapshots.append)

    assert sorted(result.successes) == [(1, 10), (2, 20), (3, 30)]
    assert [snapshot.event for snapshot in snapshots] == ["started", "progress", "progress", "finished"]

    assert snapshots[0].total == 3
    assert snapshots[0].active == 2
    assert snapshots[0].pending == 1
    assert snapshots[0].completed == 0

    assert snapshots[-1].completed == 3
    assert snapshots[-1].active == 0
    assert snapshots[-1].pending == 0
    assert snapshots[-1].succeeded == 3
    assert snapshots[-1].failed == 0
