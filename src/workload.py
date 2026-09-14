from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .message import Message


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    n_messages: int = 120
    arrival_rate: float = 1.2
    burst_probability: float = 0.0
    burst_size: int = 6
    shift_fraction: float = 0.5


def _duration(kind: str, rng: np.random.Generator, shifted: bool = False) -> float:
    scale = 2.0 if shifted else 1.0
    if kind == "short":
        value = rng.lognormal(np.log(1.2 * scale), 0.22)
    elif kind == "medium":
        value = rng.lognormal(np.log(5.0 * scale), 0.28)
    elif kind == "long":
        value = rng.lognormal(np.log(14.0 * scale), 0.32)
    elif kind == "heavy":
        value = (rng.pareto(2.2) + 1.0) * 3.0 * scale
    else:
        raise ValueError(f"unknown job type: {kind}")
    return float(np.clip(value, 0.15, 90.0))


def generate_workload(
    spec: WorkloadSpec,
    seed: int,
    experiment_id: str,
) -> tuple[list[Message], float | None]:
    """Generate deterministic jobs and return jobs plus optional shift time."""
    rng = np.random.default_rng(seed)
    now = 0.0
    messages: list[Message] = []
    shift_index = int(spec.n_messages * spec.shift_fraction)
    shift_time: float | None = None

    for index in range(spec.n_messages):
        if index and rng.random() < spec.burst_probability:
            gap = 0.0
        else:
            gap = float(rng.exponential(1.0 / spec.arrival_rate))
        now += gap

        if spec.name == "mostly_short":
            kind = str(rng.choice(["short", "medium"], p=[0.9, 0.1]))
        elif spec.name == "mixed":
            kind = str(rng.choice(["short", "medium", "long"], p=[0.5, 0.3, 0.2]))
        elif spec.name == "heavy_tailed":
            kind = "heavy"
        elif spec.name == "bursty":
            kind = str(rng.choice(["short", "medium", "long"], p=[0.5, 0.35, 0.15]))
            if index % max(spec.burst_size, 1) != 0:
                now = messages[-1].creation_time if messages else now
        elif spec.name == "gradual_shift":
            progress = index / max(spec.n_messages - 1, 1)
            kind = str(rng.choice(["short", "medium", "long"], p=[0.7 - 0.5 * progress, 0.2, 0.1 + 0.5 * progress]))
        elif spec.name == "sudden_shift":
            kind = "medium"
        else:
            raise ValueError(f"unknown workload: {spec.name}")

        shifted = spec.name == "sudden_shift" and index >= shift_index
        if shifted and shift_time is None:
            shift_time = now
        messages.append(
            Message(
                job_id=f"job-{index:05d}",
                job_type=kind,
                processing_duration=_duration(kind, rng, shifted),
                creation_time=now,
                experiment_id=experiment_id,
            )
        )
    return messages, shift_time


def oracle_class_timeouts(messages: list[Message], margin: float = 2.0) -> dict[str, float]:
    """Static baseline fitted once to the generated workload (an optimistic oracle)."""
    grouped: dict[str, list[float]] = {}
    for message in messages:
        grouped.setdefault(message.job_type, []).append(message.processing_duration)
    return {key: float(np.percentile(values, 90) + margin) for key, values in grouped.items()}

