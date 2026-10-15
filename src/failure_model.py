from __future__ import annotations

import zlib
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class FailureDecision:
    fails: bool
    fraction: float
    hangs: bool = False


class FailureModel:
    """Per-attempt worker faults.

    A crash stops the worker and its heartbeats. A hang stops progress but leaves
    the process (and its heartbeat thread) alive, so the lease keeps being
    extended until the policy's visibility budget runs out; the worker is then
    assumed to be restarted when its lease expires. Crash and hang are mutually
    exclusive for one attempt.
    """

    LEVELS = {"none": 0.0, "low": 0.02, "medium": 0.08, "high": 0.20}

    def __init__(self, probability: float = 0.0, hang_probability: float = 0.0) -> None:
        if not 0 <= probability < 1 or not 0 <= hang_probability < 1:
            raise ValueError("failure probabilities must be in [0, 1)")
        self.probability = float(probability)
        self.hang_probability = float(hang_probability)

    @classmethod
    def from_level(cls, level: str, hang_level: str = "none") -> "FailureModel":
        for name in (level, hang_level):
            if name not in cls.LEVELS:
                raise ValueError(f"unknown failure level: {name}")
        return cls(cls.LEVELS[level], cls.LEVELS[hang_level])

    def sample_for(self, seed: int, job_id: str, attempt_index: int) -> FailureDecision:
        """Common random numbers: the k-th delivery of a job fails identically under
        every policy, so policy comparisons are paired rather than confounded by
        which random draw each retry path happened to consume."""
        key = [int(seed), zlib.crc32(job_id.encode("utf-8")), int(attempt_index)]
        return self.sample(np.random.default_rng(key))

    def sample(self, rng: np.random.Generator) -> FailureDecision:
        fails = bool(rng.random() < self.probability)
        # Avoid failures at exactly start/end; this also makes wasted work measurable.
        fraction = float(rng.uniform(0.15, 0.85))
        # Drawn last so adding hangs leaves every existing crash draw unchanged.
        hangs = bool(rng.random() < self.hang_probability) and not fails
        return FailureDecision(fails, fraction, hangs)

