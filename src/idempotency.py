from __future__ import annotations

from threading import Lock


class InMemoryIdempotencyLedger:
    """Atomic first-writer-wins ledger for logical job results."""

    def __init__(self) -> None:
        self._completed: set[str] = set()
        self._lock = Lock()

    def commit_once(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._completed:
                return False
            self._completed.add(job_id)
            return True

    def contains(self, job_id: str) -> bool:
        return job_id in self._completed

    def __len__(self) -> int:
        return len(self._completed)

