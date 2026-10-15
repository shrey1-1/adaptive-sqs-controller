from __future__ import annotations

import zlib
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class FailureDecision:
    fails: bool
    fraction: float


class FailureModel:
    LEVELS = {"none": 0.0, "low": 0.02, "medium": 0.08, "high": 0.20}

    def __init__(self, probability: float = 0.0) -> None:
        if not 0 <= probability < 1:
            raise ValueError("failure probability must be in [0, 1)")
        self.probability = float(probability)

    @classmethod
    def from_level(cls, level: str) -> "FailureModel":
        if level not in cls.LEVELS:
            raise ValueError(f"unknown failure level: {level}")
        return cls(cls.LEVELS[level])

    def sample_for(self, seed: int, job_id: str, attempt_index: int) -> FailureDecision:
        """Common random numbers: the k-th delivery of a job fails identically under
        every policy, so policy comparisons are paired rather than confounded by
        which random draw each retry path happened to consume."""
        key = [int(seed), zlib.crc32(job_id.encode("utf-8")), int(attempt_index)]
        return self.sample(np.random.default_rng(key))

    def sample(self, rng: np.random.Generator) -> FailureDecision:
        fails = bool(rng.random() < self.probability)
        # Avoid failures at exactly start/end; this also makes wasted work measurable.
        return FailureDecision(fails, float(rng.uniform(0.15, 0.85)))

