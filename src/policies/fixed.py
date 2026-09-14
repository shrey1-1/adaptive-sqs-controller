from __future__ import annotations

from .base import TimeoutDecision, VisibilityPolicy


class FixedPolicy(VisibilityPolicy):
    def __init__(self, timeout: float, name: str | None = None) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.timeout = float(timeout)
        self.name = name or f"fixed_{timeout:g}s"

    def decide(self, job_type: str, now: float) -> TimeoutDecision:
        return TimeoutDecision(self.timeout, self.timeout)


class MeanFixedPolicy(FixedPolicy):
    def __init__(self, mean_duration: float, safety_margin: float = 0.0) -> None:
        super().__init__(mean_duration + safety_margin, "fixed_overall_mean")

