from __future__ import annotations

from collections import defaultdict, deque

import numpy as np

from .base import TimeoutDecision, VisibilityPolicy


class AdaptivePercentilePolicy(VisibilityPolicy):
    name = "adaptive_p90"
    predicts_duration = True

    def __init__(
        self,
        percentile: float = 90.0,
        safety_margin: float = 2.0,
        min_timeout: float = 1.0,
        max_timeout: float = 120.0,
        default_timeout: float = 15.0,
        history_window: int = 50,
        min_history: int = 5,
        heartbeat_interval: float | None = None,
        max_extensions: int = 2,
        max_total_visibility: float = 180.0,
    ) -> None:
        if not 0 < percentile <= 100:
            raise ValueError("percentile must be in (0, 100]")
        if history_window < min_history or min_history < 1:
            raise ValueError("history_window must be >= min_history >= 1")
        self.percentile = float(percentile)
        self.safety_margin = float(safety_margin)
        self.min_timeout = float(min_timeout)
        self.max_timeout = float(max_timeout)
        self.default_timeout = float(default_timeout)
        self.history_window = int(history_window)
        self.min_history = int(min_history)
        self.heartbeat_interval = heartbeat_interval
        self.max_extensions = int(max_extensions if heartbeat_interval else 0)
        self.max_total_visibility = float(max_total_visibility)
        self.history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.history_window)
        )

    def decide(self, job_type: str, now: float) -> TimeoutDecision:
        values = self.history[job_type]
        if len(values) < self.min_history:
            return TimeoutDecision(self.default_timeout, self.default_timeout, True)
        prediction = float(np.percentile(np.asarray(values), self.percentile))
        timeout = min(
            self.max_timeout,
            max(self.min_timeout, prediction + self.safety_margin),
        )
        return TimeoutDecision(timeout, prediction, False)

    def observe(self, job_type: str, duration: float, now: float) -> None:
        self.history[job_type].append(float(duration))

