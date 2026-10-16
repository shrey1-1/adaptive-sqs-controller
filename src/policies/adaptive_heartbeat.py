from __future__ import annotations

import numpy as np

from .adaptive_percentile import AdaptivePercentilePolicy
from .base import TimeoutDecision


class AdaptiveHeartbeatPolicy(AdaptivePercentilePolicy):
    """Short adaptive lease, heartbeat renewal, and a per-class visibility budget.

    Three per-class quantities are learned from the same rolling history:

    - **Lease** = min(lease_cap, P90 + margin). Short classes usually finish inside
      their first lease and never heartbeat; long classes are capped at
      `lease_cap`, so a crashed worker is noticed within one lease whatever the
      job length.
    - **Heartbeat interval** = heartbeat_fraction x lease, renewing the lease
      by one lease length each time.
    - **Visibility budget** = clamp(P99 x budget_factor + budget_margin,
      lease, max_total_visibility), measured from receive. A healthy job
      longer than the budget is redelivered (a duplicate), while a hung worker
      that keeps heartbeating is reclaimed after roughly its class's worst normal
      duration rather than after one global cap.

    Before `min_history` observations a class uses `default_lease` and the
    global `max_total_visibility`. With `adaptive_budget=False` the budget is
    always the global cap, isolating the effect of the adaptive lease. With
    `dynamic_budget=True` the budget is re-read at every heartbeat instead of
    being fixed when the message is received.
    """

    name = "adaptive_heartbeat"

    def __init__(
        self,
        percentile: float = 90.0,
        safety_margin: float = 2.0,
        history_window: int = 40,
        min_history: int = 5,
        lease_cap: float = 5.0,
        default_lease: float = 5.0,
        heartbeat_fraction: float = 0.7,
        budget_percentile: float = 99.0,
        budget_factor: float = 1.25,
        budget_margin: float = 2.0,
        max_total_visibility: float = 120.0,
        max_extensions: int = 1000,
        adaptive_budget: bool = True,
        dynamic_budget: bool = False,
        name: str | None = None,
        **kwargs,
    ) -> None:
        if not 0 < heartbeat_fraction < 1:
            raise ValueError("heartbeat_fraction must be in (0, 1) so renewal precedes expiry")
        if lease_cap <= 0 or default_lease <= 0:
            raise ValueError("leases must be positive")
        if not 0 < budget_percentile <= 100 or budget_factor < 1 or budget_margin < 0:
            raise ValueError("budget must be a valid percentile scaled by >= 1 with a non-negative margin")
        super().__init__(
            percentile=percentile,
            safety_margin=safety_margin,
            history_window=history_window,
            min_history=min_history,
            default_timeout=min(default_lease, lease_cap),
            max_total_visibility=max_total_visibility,
            name=name,
            **kwargs,
        )
        self.lease_cap = float(lease_cap)
        self.heartbeat_fraction = float(heartbeat_fraction)
        self.budget_percentile = float(budget_percentile)
        self.budget_factor = float(budget_factor)
        self.budget_margin = float(budget_margin)
        self.adaptive_budget = bool(adaptive_budget)
        # False: the budget is fixed at receive time. True: the simulator re-reads
        # it at each heartbeat, so a detected change reaches in-flight jobs.
        self.dynamic_budget = bool(dynamic_budget)
        self.max_extensions = int(max_extensions)
        # Truthy marker so generic code treats this as a heartbeat policy; the
        # simulator uses the per-class value from heartbeat_interval_for().
        self.heartbeat_interval = self.heartbeat_fraction * self.default_timeout

    def decide(self, job_type: str, now: float) -> TimeoutDecision:
        decision = super().decide(job_type, now)
        return TimeoutDecision(min(self.lease_cap, decision.timeout), decision.prediction, decision.used_fallback)

    def heartbeat_interval_for(self, job_type: str, now: float) -> float:
        return self.heartbeat_fraction * self.decide(job_type, now).timeout

    def max_total_visibility_for(self, job_type: str, now: float) -> float:
        values = self.history[job_type]
        if not self.adaptive_budget or len(values) < self.min_history:
            return self.max_total_visibility
        tail = float(np.percentile(np.asarray(values), self.budget_percentile))
        budget = tail * self.budget_factor + self.budget_margin
        return min(self.max_total_visibility, max(self.decide(job_type, now).timeout, budget))
