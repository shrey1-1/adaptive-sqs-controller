from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class TimeoutDecision:
    timeout: float
    prediction: float
    used_fallback: bool = False


class VisibilityPolicy(ABC):
    name = "base"
    # Static policies report their timeout as the "prediction"; prediction-error
    # and adaptation metrics are only meaningful for policies that estimate duration.
    predicts_duration = False
    heartbeat_interval: float | None = None
    max_extensions: int = 0
    max_total_visibility: float = 0.0

    @abstractmethod
    def decide(self, job_type: str, now: float) -> TimeoutDecision:
        raise NotImplementedError

    def observe(self, job_type: str, duration: float, now: float) -> None:
        pass

    def observe_attempt_end(self, job_type: str, failed: bool, now: float) -> None:
        """Called when an attempt completes (failed=False) or its worker crashes or
        is restarted after a hang (failed=True); lets a policy learn the failure rate."""

    def extension_timeout(self, job_type: str, now: float) -> float:
        return self.decide(job_type, now).timeout

    def heartbeat_interval_for(self, job_type: str, now: float) -> float | None:
        """Heartbeat period for a newly received message; None disables heartbeats."""
        return self.heartbeat_interval

    def max_total_visibility_for(self, job_type: str, now: float) -> float:
        """Visibility budget measured from receive: no extension may pass it."""
        return self.max_total_visibility

    def clone(self) -> "VisibilityPolicy":
        import copy

        return copy.deepcopy(self)

