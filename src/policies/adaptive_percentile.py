from __future__ import annotations

import math
from collections import defaultdict, deque

import numpy as np

from .base import TimeoutDecision, VisibilityPolicy


class AdaptivePercentilePolicy(VisibilityPolicy):
    """Rolling per-class quantile of successful durations plus a safety margin.

    Options, each off by default unless noted, so variants differ by one setting:

    - ``cold_start`` (default ``"partial"``): with 1 <= n < min_history samples,
      use ``partial_factor`` x the largest observed duration + margin instead of
      ignoring that history; ``"default"`` restores the fixed fallback.
    - ``quantile_method="conformal"``: use the ceil((n+1)q)-th order statistic,
      which covers the next duration with probability >= q whenever recent
      durations are exchangeable. When n is too small for that rank to exist,
      the cold-start rule applies.
    - ``cost_ratio``: choose the quantile from the costs instead of fixing it.
      With crash probability p, overrun cost c_dup per second and recovery cost
      c_rec per second, expected cost is minimised where
      P(D > T) = p c_rec / ((1 - p) c_dup), i.e. q* = 1 - p r / (1 - p) with
      r = c_rec / c_dup. p is learned online from attempt outcomes with a
      Beta prior; q* is clamped to [q_min, q_max].
    - ``change_detection``: a two-sided Page-Hinkley test per class on log
      durations. When the cumulative deviation from the running mean exceeds
      ``ph_lambda`` (after subtracting ``ph_delta`` per observation), the class
      history is cut to its last ``keep_after_change`` observations, so the
      window refills with post-change durations instead of waiting for 40 new
      completions to push the old regime out.
    """

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
        relative_margin: float = 0.0,
        mad_multiplier: float = 0.0,
        cold_start: str = "partial",
        partial_factor: float = 1.5,
        quantile_method: str = "interpolate",
        cost_ratio: float | None = None,
        failure_prior: tuple[float, float] = (1.0, 19.0),
        quantile_bounds: tuple[float, float] = (0.5, 0.995),
        change_detection: bool = False,
        ph_delta: float = 0.05,
        ph_lambda: float = 2.0,
        keep_after_change: int = 3,
        name: str | None = None,
    ) -> None:
        if not 0 < percentile <= 100:
            raise ValueError("percentile must be in (0, 100]")
        if history_window < min_history or min_history < 1:
            raise ValueError("history_window must be >= min_history >= 1")
        if safety_margin < 0 or relative_margin < 0 or mad_multiplier < 0:
            raise ValueError("margins must be non-negative")
        if cold_start not in {"default", "partial"} or partial_factor < 1:
            raise ValueError("cold_start must be 'default' or 'partial' with partial_factor >= 1")
        if quantile_method not in {"interpolate", "conformal"}:
            raise ValueError("quantile_method must be 'interpolate' or 'conformal'")
        if cost_ratio is not None and cost_ratio <= 0:
            raise ValueError("cost_ratio must be positive")
        if not 0 < quantile_bounds[0] < quantile_bounds[1] < 1 or min(failure_prior) <= 0:
            raise ValueError("quantile_bounds must satisfy 0 < low < high < 1 and the prior must be positive")
        if ph_delta < 0 or ph_lambda <= 0 or keep_after_change < 1:
            raise ValueError("Page-Hinkley needs ph_delta >= 0, ph_lambda > 0 and keep_after_change >= 1")
        if name:
            self.name = name
        self.percentile = float(percentile)
        self.safety_margin = float(safety_margin)
        # Margin components add together. An absolute margin alone over-protects
        # short classes and under-protects long ones; the relative and MAD terms
        # scale with each class's typical duration and spread respectively.
        self.relative_margin = float(relative_margin)
        self.mad_multiplier = float(mad_multiplier)
        self.min_timeout = float(min_timeout)
        self.max_timeout = float(max_timeout)
        self.default_timeout = float(default_timeout)
        self.history_window = int(history_window)
        self.min_history = int(min_history)
        self.heartbeat_interval = heartbeat_interval
        self.max_extensions = int(max_extensions if heartbeat_interval else 0)
        self.max_total_visibility = float(max_total_visibility)
        self.cold_start = cold_start
        self.partial_factor = float(partial_factor)
        self.quantile_method = quantile_method
        self.cost_ratio = None if cost_ratio is None else float(cost_ratio)
        self.failure_prior = (float(failure_prior[0]), float(failure_prior[1]))
        self.quantile_bounds = (float(quantile_bounds[0]), float(quantile_bounds[1]))
        self.attempts_seen = 0
        self.failures_seen = 0
        self.change_detection = bool(change_detection)
        self.ph_delta = float(ph_delta)
        self.ph_lambda = float(ph_lambda)
        self.keep_after_change = int(keep_after_change)
        self.change_points = 0
        # Per-class Page-Hinkley state: [n, mean, up, min_up, down, max_down].
        self._ph: dict[str, list[float]] = {}
        self.history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.history_window)
        )

    def decide(self, job_type: str, now: float) -> TimeoutDecision:
        values = self.history[job_type]
        prediction = self._quantile(values) if len(values) >= self.min_history else None
        if prediction is None:
            return self._cold_start_decision(values)
        samples = np.asarray(values)
        timeout = self._clamp(prediction + self.margin(samples, prediction))
        return TimeoutDecision(timeout, prediction, False)

    def _clamp(self, timeout: float) -> float:
        return min(self.max_timeout, max(self.min_timeout, timeout))

    def _cold_start_decision(self, values) -> TimeoutDecision:
        if self.cold_start == "partial" and len(values):
            # With n samples, the next duration exceeds their maximum with
            # probability 1/(n+1); the factor widens that bound for the tail.
            prediction = self.partial_factor * max(values)
            return TimeoutDecision(self._clamp(prediction + self.safety_margin), prediction, True)
        return TimeoutDecision(self.default_timeout, self.default_timeout, True)

    def target_quantile(self) -> float:
        """Quantile level in (0, 1): fixed, or chosen from costs and the failure rate."""
        if self.cost_ratio is None:
            return self.percentile / 100.0
        p = self.failure_rate()
        q = 1.0 - p * self.cost_ratio / (1.0 - p)
        low, high = self.quantile_bounds
        return min(high, max(low, q))

    def failure_rate(self) -> float:
        a, b = self.failure_prior
        return (self.failures_seen + a) / (self.attempts_seen + a + b)

    def _quantile(self, values) -> float | None:
        q = self.target_quantile()
        samples = np.sort(np.asarray(values))
        if self.quantile_method == "conformal":
            rank = math.ceil((len(samples) + 1) * q)
            return float(samples[rank - 1]) if rank <= len(samples) else None
        return float(np.percentile(samples, 100.0 * q))

    def margin(self, samples: np.ndarray, prediction: float) -> float:
        margin = self.safety_margin + self.relative_margin * prediction
        if self.mad_multiplier:
            mad = float(np.median(np.abs(samples - np.median(samples))))
            margin += self.mad_multiplier * mad
        return margin

    def observe(self, job_type: str, duration: float, now: float) -> None:
        self.history[job_type].append(float(duration))
        if self.change_detection and self._page_hinkley(job_type, math.log(max(float(duration), 1e-9))):
            recent = list(self.history[job_type])[-self.keep_after_change:]
            self.history[job_type].clear()
            self.history[job_type].extend(recent)
            self.change_points += 1

    def _page_hinkley(self, job_type: str, x: float) -> bool:
        """Two-sided Page-Hinkley update; True (and a reset) when a shift is detected."""
        state = self._ph.setdefault(job_type, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        n, mean, up, min_up, down, max_down = state
        n += 1
        mean += (x - mean) / n
        up += x - mean - self.ph_delta      # grows when durations rise
        down += x - mean + self.ph_delta    # falls when durations drop
        min_up, max_down = min(min_up, up), max(max_down, down)
        if up - min_up > self.ph_lambda or max_down - down > self.ph_lambda:
            self._ph[job_type] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            return True
        self._ph[job_type] = [n, mean, up, min_up, down, max_down]
        return False

    def observe_attempt_end(self, job_type: str, failed: bool, now: float) -> None:
        self.attempts_seen += 1
        self.failures_seen += int(failed)
