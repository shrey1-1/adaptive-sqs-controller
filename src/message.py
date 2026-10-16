from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Message:
    job_id: str
    job_type: str
    processing_duration: float
    creation_time: float
    experiment_id: str
    # A poison message fails on every attempt (e.g. malformed payload); only a
    # dead-letter queue removes it.
    poison: bool = False


@dataclass
class DeliveryAttempt:
    attempt_id: str
    receipt_handle: str
    message: Message
    worker_id: str
    receive_time: float
    start_time: float
    predicted_duration: float
    visibility_timeout: float
    visibility_deadline: float
    receive_count: int
    extensions: int = 0
    failed: bool = False
    completed: bool = False
    duplicate_delivery: bool = False
    duplicate_processing: bool = False
    finish_time: Optional[float] = None
    failure_time: Optional[float] = None
    last_extension_time: Optional[float] = None
    heartbeat_interval: Optional[float] = None
    visibility_budget: float = 0.0
    hung: bool = False
    used_fallback: bool = False
    # Effective processing time of this attempt: the job's base duration scaled
    # by retry jitter and worker speed (equal to the base duration by default).
    duration: float = 0.0

