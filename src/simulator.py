from __future__ import annotations

import heapq
import itertools
import uuid
from dataclasses import asdict
from typing import Any

import numpy as np

from .failure_model import FailureModel
from .idempotency import InMemoryIdempotencyLedger
from .message import DeliveryAttempt, Message
from .policies.base import VisibilityPolicy


class SQSSimulator:
    """Discrete-event approximation of one Standard SQS queue.

    Simplifications are explicit: receive/delete/change-visibility are modeled per
    message (not batches), network latency is zero, and receipt handles are unique
    per delivery. For finite experiments, any successful active attempt can delete.
    Standard queues may also create rare duplicates
    even before a visibility timeout; that independent replication effect is not
    simulated.
    """

    def __init__(
        self,
        messages: list[Message],
        policy: VisibilityPolicy,
        failure_model: FailureModel,
        workers: int = 8,
        seed: int = 0,
    ) -> None:
        self.messages = messages
        self.policy = policy
        self.failure_model = failure_model
        self.worker_ids = [f"worker-{i:02d}" for i in range(workers)]
        self.rng = np.random.default_rng(seed)
        self.ledger = InMemoryIdempotencyLedger()
        self.events: list[tuple[float, int, str, Any]] = []
        self.counter = itertools.count()
        self.now = 0.0
        self.visible: list[Message] = []
        self.busy: dict[str, str] = {}
        self.attempts: dict[str, DeliveryAttempt] = {}
        self.current_receipt: dict[str, str] = {}
        self.receive_counts: dict[str, int] = {}
        self.deleted: set[str] = set()
        self.active_by_job: dict[str, set[str]] = {}
        self.last_failure: dict[str, float] = {}
        self.failure_recoveries: list[float] = []
        self.rows: list[dict[str, Any]] = []
        self.api_operations = {"send": 0, "receive": 0, "delete": 0, "change_visibility": 0}

    def _push(self, when: float, event: str, payload: Any) -> None:
        heapq.heappush(self.events, (when, next(self.counter), event, payload))

    def _dispatch(self) -> None:
        idle = [w for w in self.worker_ids if w not in self.busy]
        self.visible.sort(key=lambda m: (m.creation_time, m.job_id))
        while idle and self.visible:
            worker_id = idle.pop(0)
            message = self.visible.pop(0)
            if message.job_id in self.deleted:
                continue
            receive_count = self.receive_counts.get(message.job_id, 0) + 1
            self.receive_counts[message.job_id] = receive_count
            self.api_operations["receive"] += 1
            decision = self.policy.decide(message.job_type, self.now)
            attempt_id = str(uuid.uuid4())
            receipt = str(uuid.uuid4())
            active = self.active_by_job.setdefault(message.job_id, set())
            attempt = DeliveryAttempt(
                attempt_id=attempt_id,
                receipt_handle=receipt,
                message=message,
                worker_id=worker_id,
                receive_time=self.now,
                start_time=self.now,
                predicted_duration=decision.prediction,
                visibility_timeout=decision.timeout,
                visibility_deadline=self.now + decision.timeout,
                receive_count=receive_count,
                duplicate_delivery=receive_count > 1,
                duplicate_processing=bool(active),
            )
            self.attempts[attempt_id] = attempt
            active.add(attempt_id)
            self.current_receipt[message.job_id] = receipt
            self.busy[worker_id] = attempt_id
            if message.job_id in self.last_failure:
                self.failure_recoveries.append(self.now - self.last_failure.pop(message.job_id))
            self._push(attempt.visibility_deadline, "visibility_expire", attempt_id)
            failure = self.failure_model.sample(self.rng)
            if failure.fails:
                self._push(self.now + message.processing_duration * failure.fraction, "fail", attempt_id)
            else:
                self._push(self.now + message.processing_duration, "complete", attempt_id)
                if self.policy.heartbeat_interval:
                    self._push(self.now + self.policy.heartbeat_interval, "heartbeat", attempt_id)

    def _record(self, attempt: DeliveryAttempt, success: bool, committed: bool) -> None:
        message = attempt.message
        completion = attempt.finish_time if success else None
        row = {
            "experiment_id": message.experiment_id,
            "policy_name": self.policy.name,
            "job_id": message.job_id,
            "job_type": message.job_type,
            "worker_id": attempt.worker_id,
            "send_time": message.creation_time,
            "receive_time": attempt.receive_time,
            "processing_start_time": attempt.start_time,
            "completion_time": completion,
            "actual_processing_duration": (
                message.processing_duration if success else (attempt.failure_time - attempt.start_time)
            ),
            "predicted_processing_duration": attempt.predicted_duration,
            "assigned_visibility_timeout": attempt.visibility_timeout,
            "number_of_timeout_extensions": attempt.extensions,
            "receive_count": attempt.receive_count,
            "worker_failure": attempt.failed,
            "duplicate_delivery": attempt.duplicate_delivery,
            "duplicate_processing": attempt.duplicate_processing,
            "successful_completion": success,
            "committed_result": committed,
            "duplicate_committed_result": success and not committed,
        }
        self.rows.append(row)

    def _on_failure(self, attempt: DeliveryAttempt) -> None:
        if attempt.completed or attempt.failed:
            return
        attempt.failed = True
        attempt.failure_time = self.now
        self.busy.pop(attempt.worker_id, None)
        self.active_by_job.get(attempt.message.job_id, set()).discard(attempt.attempt_id)
        self.last_failure[attempt.message.job_id] = self.now
        self._record(attempt, False, False)

    def _on_complete(self, attempt: DeliveryAttempt) -> None:
        if attempt.failed or attempt.completed:
            return
        attempt.completed = True
        attempt.finish_time = self.now
        self.busy.pop(attempt.worker_id, None)
        self.active_by_job.get(attempt.message.job_id, set()).discard(attempt.attempt_id)
        committed = self.ledger.commit_once(attempt.message.job_id)
        if attempt.message.job_id not in self.deleted:
            self.deleted.add(attempt.message.job_id)
            self.api_operations["delete"] += 1
        if committed:
            self.policy.observe(attempt.message.job_type, attempt.message.processing_duration, self.now)
        self._record(attempt, True, committed)

    def _on_visibility_expire(self, attempt: DeliveryAttempt) -> None:
        job_id = attempt.message.job_id
        if job_id in self.deleted or attempt.failed and self.current_receipt.get(job_id) != attempt.receipt_handle:
            return
        if abs(attempt.visibility_deadline - self.now) > 1e-9:
            return  # stale expiry event after ChangeMessageVisibility
        if self.current_receipt.get(job_id) == attempt.receipt_handle:
            self.visible.append(attempt.message)

    def _on_heartbeat(self, attempt: DeliveryAttempt) -> None:
        if attempt.failed or attempt.completed or attempt.message.job_id in self.deleted:
            return
        if attempt.extensions >= self.policy.max_extensions:
            return
        proposed = self.now + self.policy.extension_timeout(attempt.message.job_type, self.now)
        cap = attempt.start_time + self.policy.max_total_visibility
        new_deadline = min(proposed, cap)
        if new_deadline > attempt.visibility_deadline:
            attempt.extensions += 1
            attempt.last_extension_time = self.now
            attempt.visibility_deadline = new_deadline
            self.api_operations["change_visibility"] += 1
            self._push(new_deadline, "visibility_expire", attempt.attempt_id)
        if attempt.extensions < self.policy.max_extensions:
            self._push(self.now + float(self.policy.heartbeat_interval), "heartbeat", attempt.attempt_id)

    def run(self) -> dict[str, Any]:
        for message in self.messages:
            self._push(message.creation_time, "send", message)
        safety_events = max(10000, len(self.messages) * 100)
        processed = 0
        while self.events and processed < safety_events:
            when, _, event, payload = heapq.heappop(self.events)
            self.now = when
            processed += 1
            if event == "send":
                self.api_operations["send"] += 1
                self.visible.append(payload)
            else:
                attempt = self.attempts[payload]
                if event == "fail":
                    self._on_failure(attempt)
                elif event == "complete":
                    self._on_complete(attempt)
                elif event == "visibility_expire":
                    self._on_visibility_expire(attempt)
                elif event == "heartbeat":
                    self._on_heartbeat(attempt)
            self._dispatch()
            if len(self.deleted) == len(self.messages) and not self.busy:
                break
        return {
            "attempt_rows": self.rows,
            "api_operations": dict(self.api_operations),
            "failure_recoveries": list(self.failure_recoveries),
            "simulation_end": self.now,
            "completed_jobs": len(self.ledger),
            "deleted_jobs": len(self.deleted),
            "event_limit_reached": processed >= safety_events,
        }
