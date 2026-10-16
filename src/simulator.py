from __future__ import annotations

import heapq
import itertools
import math
import uuid
import zlib
from dataclasses import asdict
from typing import Any

import numpy as np

from .failure_model import FailureDecision, FailureModel
from .idempotency import InMemoryIdempotencyLedger
from .message import DeliveryAttempt, Message
from .policies.base import VisibilityPolicy


class SQSSimulator:
    """Discrete-event approximation of one Standard SQS queue.

    Simplifications are explicit: receipt handles are unique per delivery, worker
    replacement is instantaneous, and for finite experiments any successful active
    attempt can delete. Standard queues may also create rare duplicates even
    before a visibility timeout; that independent replication effect is not
    simulated.

    Phase 5 realism features, all off by default so earlier results reproduce:

    - ``max_receive_count``: a message received more than this many times is
      moved to a dead-letter queue instead of being delivered (SQS redrive).
    - ``retry_jitter``: each attempt's duration is the job's base duration times
      a mean-one log-normal factor with this sigma, drawn per (job, attempt).
    - ``worker_speed_spread``: each worker has a fixed mean-one log-normal factor
      with this sigma that multiplies the durations it processes.
    - ``api_latency``: seconds before a receive starts processing, a delete takes
      effect, or a ChangeMessageVisibility is applied.
    - ``receive_batch_size``: messages per ReceiveMessage call (API accounting).
    - ``outages``: (time, worker indices) pairs; every attempt running on those
      workers crashes at that time (correlated failures).
    """

    def __init__(
        self,
        messages: list[Message],
        policy: VisibilityPolicy,
        failure_model: FailureModel,
        workers: int = 8,
        seed: int = 0,
        hang_detection_timeout: float | None = None,
        max_receive_count: int | None = None,
        retry_jitter: float = 0.0,
        worker_speed_spread: float = 0.0,
        api_latency: float = 0.0,
        receive_batch_size: int = 1,
        outages: list[tuple[float, list[int]]] | None = None,
    ) -> None:
        if max_receive_count is not None and max_receive_count < 1:
            raise ValueError("max_receive_count must be >= 1")
        if retry_jitter < 0 or worker_speed_spread < 0 or api_latency < 0 or receive_batch_size < 1:
            raise ValueError("realism parameters must be non-negative (batch size >= 1)")
        self.messages = messages
        self.policy = policy
        self.failure_model = failure_model
        self.worker_ids = [f"worker-{i:02d}" for i in range(workers)]
        self.seed = int(seed)
        # Optional liveness probe: a hung worker is killed this long after it
        # stops progressing, independently of its lease. None = no probe.
        self.hang_detection_timeout = hang_detection_timeout
        self.max_receive_count = max_receive_count
        self.retry_jitter = float(retry_jitter)
        self.api_latency = float(api_latency)
        self.receive_batch_size = int(receive_batch_size)
        self.outages = list(outages or [])
        self.worker_speed = {w: 1.0 for w in self.worker_ids}
        if worker_speed_spread > 0:
            # Same speeds for every policy run with this seed (common random numbers).
            factors = np.random.default_rng([self.seed, 0x5EED]).lognormal(
                -worker_speed_spread**2 / 2, worker_speed_spread, size=len(self.worker_ids)
            )
            self.worker_speed = {w: float(f) for w, f in zip(self.worker_ids, factors)}
        self.dead_lettered: dict[str, float] = {}
        if max_receive_count is None and any(m.poison for m in messages):
            raise ValueError("poison messages need max_receive_count, or they are redelivered forever")
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
        self.last_failure_kind: dict[str, str] = {}
        self.failure_recovery_kinds: list[str] = []
        self.worker_hangs = 0
        self.failure_recoveries: list[float] = []
        self.failure_recovery_job_ids: list[str] = []
        self.rows: list[dict[str, Any]] = []
        self.api_operations = {"send": 0, "receive": 0, "delete": 0, "change_visibility": 0}

    def _push(self, when: float, event: str, payload: Any) -> None:
        heapq.heappush(self.events, (when, next(self.counter), event, payload))

    def _dispatch(self) -> None:
        idle = [w for w in self.worker_ids if w not in self.busy]
        self.visible.sort(key=lambda m: (m.creation_time, m.job_id))
        received = 0
        while idle and self.visible:
            message = self.visible.pop(0)
            if message.job_id in self.deleted or message.job_id in self.dead_lettered:
                continue  # stale copy; keep the worker for the next visible message
            receive_count = self.receive_counts.get(message.job_id, 0) + 1
            if self.max_receive_count is not None and receive_count > self.max_receive_count:
                # SQS redrive: moved to the dead-letter queue instead of delivered.
                self.dead_lettered[message.job_id] = self.now
                self.last_failure.pop(message.job_id, None)
                self.last_failure_kind.pop(message.job_id, None)
                continue
            worker_id = idle.pop(0)
            self.receive_counts[message.job_id] = receive_count
            received += 1
            decision = self.policy.decide(message.job_type, self.now)
            attempt_id = str(uuid.uuid4())
            receipt = str(uuid.uuid4())
            active = self.active_by_job.setdefault(message.job_id, set())
            start = self.now + self.api_latency
            attempt = DeliveryAttempt(
                attempt_id=attempt_id,
                receipt_handle=receipt,
                message=message,
                worker_id=worker_id,
                receive_time=self.now,
                start_time=start,
                predicted_duration=decision.prediction,
                visibility_timeout=decision.timeout,
                visibility_deadline=self.now + decision.timeout,
                receive_count=receive_count,
                duplicate_delivery=receive_count > 1,
                duplicate_processing=bool(active),
                heartbeat_interval=self.policy.heartbeat_interval_for(message.job_type, self.now),
                visibility_budget=self.policy.max_total_visibility_for(message.job_type, self.now),
                used_fallback=decision.used_fallback,
                duration=self._attempt_duration(message, receive_count, worker_id),
            )
            self.attempts[attempt_id] = attempt
            active.add(attempt_id)
            self.current_receipt[message.job_id] = receipt
            self.busy[worker_id] = attempt_id
            if message.job_id in self.last_failure:
                self.failure_recoveries.append(self.now - self.last_failure.pop(message.job_id))
                self.failure_recovery_job_ids.append(message.job_id)
                self.failure_recovery_kinds.append(self.last_failure_kind.pop(message.job_id, "crash"))
            self._push(attempt.visibility_deadline, "visibility_expire", attempt_id)
            failure = self.failure_model.sample_for(self.seed, message.job_id, receive_count)
            if message.poison:
                failure = FailureDecision(True, failure.fraction)
            if failure.fails:
                self._push(start + attempt.duration * failure.fraction, "fail", attempt_id)
            elif failure.hangs:
                self._push(start + attempt.duration * failure.fraction, "hang", attempt_id)
            else:
                self._push(start + attempt.duration, "complete", attempt_id)
            # Every attempt heartbeats, including ones that will later crash: a real
            # worker keeps extending its lease right up to the moment it dies.
            if attempt.heartbeat_interval:
                self._push(self.now + attempt.heartbeat_interval, "heartbeat", attempt_id)
        if received:
            self.api_operations["receive"] += math.ceil(received / self.receive_batch_size)

    def _attempt_duration(self, message: Message, receive_count: int, worker_id: str) -> float:
        duration = message.processing_duration
        if self.retry_jitter > 0:
            key = [self.seed, zlib.crc32(message.job_id.encode("utf-8")), receive_count, 0x717]
            sigma = self.retry_jitter
            duration *= float(np.random.default_rng(key).lognormal(-sigma**2 / 2, sigma))
        speed = self.worker_speed[worker_id]
        return duration if speed == 1.0 else duration * speed

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
                attempt.duration if success else (attempt.failure_time - attempt.start_time)
            ),
            "predicted_processing_duration": attempt.predicted_duration,
            "assigned_visibility_timeout": attempt.visibility_timeout,
            "number_of_timeout_extensions": attempt.extensions,
            "receive_count": attempt.receive_count,
            "worker_failure": attempt.failed,
            "worker_hang": attempt.hung,
            "used_fallback_timeout": attempt.used_fallback,
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
        self.last_failure_kind[attempt.message.job_id] = "crash"
        self.policy.observe_attempt_end(attempt.message.job_type, True, self.now)
        self._record(attempt, False, False)

    def _on_hang(self, attempt: DeliveryAttempt) -> None:
        """Progress stops but the worker stays busy and keeps heartbeating."""
        if attempt.completed or attempt.failed:
            return
        attempt.hung = True
        attempt.failure_time = self.now
        self.worker_hangs += 1
        self.last_failure[attempt.message.job_id] = self.now
        self.last_failure_kind[attempt.message.job_id] = "hang"
        if self.hang_detection_timeout is not None:
            self._push(self.now + self.hang_detection_timeout, "hang_detected", attempt.attempt_id)

    def _release_hung(self, attempt: DeliveryAttempt) -> None:
        """The hung worker is restarted once its lease finally expires."""
        attempt.failed = True
        self.busy.pop(attempt.worker_id, None)
        self.active_by_job.get(attempt.message.job_id, set()).discard(attempt.attempt_id)
        self.policy.observe_attempt_end(attempt.message.job_type, True, self.now)
        self._record(attempt, False, False)

    def _on_hang_detected(self, attempt: DeliveryAttempt) -> None:
        """Liveness probe kills the hung worker: heartbeats stop, and the message
        becomes visible when its current lease runs out."""
        if attempt.hung and not attempt.failed:
            self._release_hung(attempt)

    def _on_complete(self, attempt: DeliveryAttempt) -> None:
        if attempt.failed or attempt.completed:
            return
        attempt.completed = True
        attempt.finish_time = self.now
        self.busy.pop(attempt.worker_id, None)
        self.active_by_job.get(attempt.message.job_id, set()).discard(attempt.attempt_id)
        committed = self.ledger.commit_once(attempt.message.job_id)
        self.policy.observe_attempt_end(attempt.message.job_type, False, self.now)
        if self.api_latency > 0:
            self._push(self.now + self.api_latency, "delete", attempt.attempt_id)
        else:
            self._delete(attempt)
        if committed:
            self.policy.observe(attempt.message.job_type, attempt.duration, self.now)
        self._record(attempt, True, committed)

    def _delete(self, attempt: DeliveryAttempt) -> None:
        job_id = attempt.message.job_id
        if job_id in self.dead_lettered:
            return  # the message now sits in the DLQ; the old handle cannot delete it
        if job_id not in self.deleted:
            self.deleted.add(job_id)
            self.api_operations["delete"] += 1

    def _on_outage(self, worker_indices: list[int]) -> None:
        """Correlated failure: every attempt running on these workers crashes."""
        for index in worker_indices:
            attempt_id = self.busy.get(self.worker_ids[index % len(self.worker_ids)])
            if attempt_id is not None:
                attempt = self.attempts[attempt_id]
                if attempt.hung:
                    self._release_hung(attempt)
                else:
                    self._on_failure(attempt)

    def _on_visibility_expire(self, attempt: DeliveryAttempt) -> None:
        job_id = attempt.message.job_id
        if abs(attempt.visibility_deadline - self.now) > 1e-9:
            return  # stale expiry event after ChangeMessageVisibility
        if attempt.hung and not attempt.failed:
            self._release_hung(attempt)
        if job_id in self.deleted or job_id in self.dead_lettered:
            return
        if self.current_receipt.get(job_id) == attempt.receipt_handle:
            self.visible.append(attempt.message)

    def _on_heartbeat(self, attempt: DeliveryAttempt) -> None:
        if self.api_latency > 0:
            # The worker sends ChangeMessageVisibility now; SQS applies it later.
            job_id = attempt.message.job_id
            if not (attempt.failed or attempt.completed or job_id in self.deleted or job_id in self.dead_lettered):
                self._push(self.now + self.api_latency, "extend", attempt.attempt_id)
            return
        self._apply_extension(attempt, next_heartbeat_delay=0.0)

    def _apply_extension(self, attempt: DeliveryAttempt, next_heartbeat_delay: float) -> None:
        job_id = attempt.message.job_id
        if attempt.failed or attempt.completed or job_id in self.deleted or job_id in self.dead_lettered:
            return
        if self.current_receipt.get(job_id) != attempt.receipt_handle:
            return  # lease already lost to a newer receive; SQS rejects the stale handle
        if attempt.extensions >= self.policy.max_extensions:
            return
        proposed = self.now + self.policy.extension_timeout(attempt.message.job_type, self.now)
        if getattr(self.policy, "dynamic_budget", False):
            # Re-read the budget at every heartbeat, so in-flight jobs pick up a
            # larger budget as soon as the policy learns durations have grown.
            attempt.visibility_budget = self.policy.max_total_visibility_for(attempt.message.job_type, self.now)
        cap = attempt.start_time + attempt.visibility_budget
        new_deadline = min(proposed, cap)
        if new_deadline > attempt.visibility_deadline:
            attempt.extensions += 1
            attempt.last_extension_time = self.now
            attempt.visibility_deadline = new_deadline
            self.api_operations["change_visibility"] += 1
            self._push(new_deadline, "visibility_expire", attempt.attempt_id)
        # A static budget that has been reached can never grow, so heartbeats stop;
        # a dynamic budget may grow later, so the worker keeps heartbeating.
        reached_cap = attempt.visibility_deadline >= cap - 1e-9 and not getattr(self.policy, "dynamic_budget", False)
        if attempt.extensions < self.policy.max_extensions and not reached_cap:
            delay = max(0.0, float(attempt.heartbeat_interval) - next_heartbeat_delay)
            self._push(self.now + delay, "heartbeat", attempt.attempt_id)

    def run(self) -> dict[str, Any]:
        for message in self.messages:
            self._push(message.creation_time, "send", message)
        for when, worker_indices in self.outages:
            self._push(float(when), "outage", list(worker_indices))
        safety_events = max(10000, len(self.messages) * 100)
        processed = 0
        while self.events and processed < safety_events:
            when, _, event, payload = heapq.heappop(self.events)
            self.now = when
            processed += 1
            if event == "send":
                self.api_operations["send"] += 1
                self.visible.append(payload)
            elif event == "outage":
                self._on_outage(payload)
            else:
                attempt = self.attempts[payload]
                if event == "fail":
                    self._on_failure(attempt)
                elif event == "hang":
                    self._on_hang(attempt)
                elif event == "hang_detected":
                    self._on_hang_detected(attempt)
                elif event == "complete":
                    self._on_complete(attempt)
                elif event == "visibility_expire":
                    self._on_visibility_expire(attempt)
                elif event == "heartbeat":
                    self._on_heartbeat(attempt)
                elif event == "extend":
                    self._apply_extension(attempt, next_heartbeat_delay=self.api_latency)
                elif event == "delete":
                    self._delete(attempt)
            self._dispatch()
            # Deleted and dead-lettered jobs are disjoint: dispatch never dead-letters
            # a deleted job and _delete never deletes a dead-lettered one.
            finished = len(self.deleted) + len(self.dead_lettered)
            if finished == len(self.messages) and not self.busy:
                break
        return {
            "attempt_rows": self.rows,
            "api_operations": dict(self.api_operations),
            "failure_recoveries": list(self.failure_recoveries),
            "failure_recovery_job_ids": list(self.failure_recovery_job_ids),
            "failure_recovery_kinds": list(self.failure_recovery_kinds),
            "worker_hangs": self.worker_hangs,
            "job_ids_in_send_order": [m.job_id for m in sorted(self.messages, key=lambda m: (m.creation_time, m.job_id))],
            "simulation_end": self.now,
            "completed_jobs": len(self.ledger),
            "deleted_jobs": len(self.deleted),
            "dead_lettered": dict(self.dead_lettered),
            "poison_job_ids": [m.job_id for m in self.messages if m.poison],
            "job_send_times": {m.job_id: m.creation_time for m in self.messages},
            "event_limit_reached": processed >= safety_events,
            "policy_predicts_duration": self.policy.predicts_duration,
            "policy_change_points": int(getattr(self.policy, "change_points", 0)),
        }
