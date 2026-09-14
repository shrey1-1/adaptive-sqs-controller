from __future__ import annotations

from .base import TimeoutDecision, VisibilityPolicy


class ClassBasedPolicy(VisibilityPolicy):
    """Static, tuned-per-class baseline and the key adaptive comparator."""

    name = "fixed_per_class"

    def __init__(self, timeouts: dict[str, float], default_timeout: float) -> None:
        self.timeouts = {k: float(v) for k, v in timeouts.items()}
        self.default_timeout = float(default_timeout)

    def decide(self, job_type: str, now: float) -> TimeoutDecision:
        fallback = job_type not in self.timeouts
        timeout = self.timeouts.get(job_type, self.default_timeout)
        return TimeoutDecision(timeout, timeout, fallback)

