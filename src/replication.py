"""Small real-SQS replication of the simulator (Phase 6.1).

Worker threads process a seeded workload through a real (or fake) SQS queue
using the same policy objects as the simulator. Job work is `time.sleep`;
crashes come from the same `FailureModel.sample_for(seed, job, receive_count)`
draws the simulator uses, so the real run and its simulated twin face the same
failures. The output compares predicted and observed duplicates, recovery and
API calls: a test of whether the simulator predicts reality, not a
significance test (one run per policy).

Safety: the harness never creates or deletes queues, refuses to start unless the
queue reports zero messages, only processes messages tagged with its run id
(others are released immediately), and deletes its own leftovers at the end.
"""
from __future__ import annotations

import argparse
import json
import math
import threading
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd

from .failure_model import FailureModel
from .message import Message
from .metrics import summarize_run
from .policies import (
    AdaptiveHeartbeatPolicy,
    AdaptivePercentilePolicy,
    FixedHeartbeatPolicy,
    FixedPolicy,
)
from .policies.base import TimeoutDecision, VisibilityPolicy
from .simulator import SQSSimulator
from .workload import WorkloadSpec, generate_workload

MAX_VISIBILITY = 43200  # SQS limit, seconds


class InvalidReceiptHandle(Exception):
    """Raised by the fake client where SQS rejects a stale receipt handle."""


class FakeSQSClient:
    """In-memory stand-in for the boto3 SQS client, following SQS visibility rules.

    Implements the five calls the harness uses. ChangeMessageVisibility requires
    the most recent receipt handle; DeleteMessage accepts any handle of the
    message, matching the simulator's assumption. Time is the wall clock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._messages: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        self.calls: dict[str, int] = {"send": 0, "receive": 0, "delete": 0, "change_visibility": 0}

    def get_queue_attributes(self, QueueUrl: str, AttributeNames: list[str]) -> dict:
        with self._lock:
            return {"Attributes": {"ApproximateNumberOfMessages": str(len(self._messages))}}

    def send_message(self, QueueUrl: str, MessageBody: str) -> dict:
        with self._lock:
            self.calls["send"] += 1
            message_id = str(uuid.uuid4())
            self._messages[message_id] = {"body": MessageBody, "visible_at": 0.0, "count": 0, "receipts": [], "deleted": False}
            self._order.append(message_id)
            return {"MessageId": message_id}

    def receive_message(self, QueueUrl: str, MaxNumberOfMessages: int = 1, WaitTimeSeconds: int = 0,
                        VisibilityTimeout: float = 30, AttributeNames: list[str] | None = None) -> dict:
        deadline = time.monotonic() + WaitTimeSeconds
        with self._lock:
            self.calls["receive"] += 1
        while True:
            with self._lock:
                now = time.monotonic()
                for message_id in self._order:
                    m = self._messages.get(message_id)
                    if m and not m["deleted"] and m["visible_at"] <= now:
                        m["visible_at"] = now + float(VisibilityTimeout)
                        m["count"] += 1
                        receipt = str(uuid.uuid4())
                        m["receipts"].append(receipt)
                        return {"Messages": [{"MessageId": message_id, "ReceiptHandle": receipt, "Body": m["body"],
                                              "Attributes": {"ApproximateReceiveCount": str(m["count"])}}]}
            if time.monotonic() >= deadline:
                return {}
            time.sleep(0.005)

    def change_message_visibility(self, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: float) -> dict:
        with self._lock:
            self.calls["change_visibility"] += 1
            for m in self._messages.values():
                if m["receipts"] and m["receipts"][-1] == ReceiptHandle and not m["deleted"]:
                    m["visible_at"] = time.monotonic() + float(VisibilityTimeout)
                    return {}
            raise InvalidReceiptHandle(ReceiptHandle)

    def delete_message(self, QueueUrl: str, ReceiptHandle: str) -> dict:
        with self._lock:
            self.calls["delete"] += 1
            for message_id, m in list(self._messages.items()):
                if ReceiptHandle in m["receipts"]:
                    del self._messages[message_id]
                    return {}
            return {}


class IntegerTimeouts(VisibilityPolicy):
    """Wraps a policy so every visibility timeout is a whole number of seconds,
    as SQS requires (1..43200); used for both the real run and its simulated twin."""

    def __init__(self, inner: VisibilityPolicy) -> None:
        self.inner = inner
        self.name = inner.name
        self.predicts_duration = inner.predicts_duration
        self.heartbeat_interval = inner.heartbeat_interval
        self.max_extensions = inner.max_extensions
        self.max_total_visibility = inner.max_total_visibility
        self.dynamic_budget = getattr(inner, "dynamic_budget", False)

    @staticmethod
    def _seconds(value: float) -> int:
        return int(min(MAX_VISIBILITY, max(1, math.ceil(value - 1e-9))))

    def decide(self, job_type: str, now: float) -> TimeoutDecision:
        d = self.inner.decide(job_type, now)
        return TimeoutDecision(float(self._seconds(d.timeout)), d.prediction, d.used_fallback)

    def extension_timeout(self, job_type: str, now: float) -> float:
        return float(self._seconds(self.inner.extension_timeout(job_type, now)))

    def heartbeat_interval_for(self, job_type: str, now: float):
        return self.inner.heartbeat_interval_for(job_type, now)

    def max_total_visibility_for(self, job_type: str, now: float) -> float:
        return self.inner.max_total_visibility_for(job_type, now)

    def observe(self, job_type: str, duration: float, now: float) -> None:
        self.inner.observe(job_type, duration, now)

    def observe_attempt_end(self, job_type: str, failed: bool, now: float, kind: str = "crash") -> None:
        self.inner.observe_attempt_end(job_type, failed, now, kind)


def replication_policies(time_scale: float) -> dict[str, VisibilityPolicy]:
    """The compared policies, with fixed values scaled like the job durations."""
    s = time_scale
    return {
        "fixed_short": FixedPolicy(3.0 * s, "fixed_short"),
        "fixed_heartbeat": FixedHeartbeatPolicy(5.0 * s, heartbeat_interval=3.5 * s, max_extensions=4, max_total_visibility=40.0 * s),
        "adaptive_p90": AdaptivePercentilePolicy(safety_margin=2.0 * s, history_window=40, default_timeout=12.0 * s, min_history=5),
        "adaptive_heartbeat_dynamic": AdaptiveHeartbeatPolicy(
            safety_margin=2.0 * s, history_window=40, min_history=5, lease_cap=5.0 * s, default_lease=5.0 * s,
            max_total_visibility=40.0 * s, dynamic_budget=True, name="adaptive_heartbeat_dynamic",
        ),
    }


@dataclass
class ReplicationConfig:
    n_jobs: int = 40
    workers: int = 4
    seed: int = 11
    failure_probability: float = 0.08
    workload: str = "mixed"
    time_scale: float = 1.0
    integer_timeouts: bool = True
    poll_wait_seconds: int = 1
    max_seconds_per_policy: float = 600.0


class ReplicationRun:
    """One policy's real run: worker threads, heartbeats, injected crashes, records."""

    def __init__(self, client, queue_url: str, policy: VisibilityPolicy, messages: list[Message], config: ReplicationConfig) -> None:
        self.client, self.queue_url, self.policy, self.config = client, queue_url, policy, config
        self.messages = {m.job_id: m for m in messages}
        self.failure_model = FailureModel(config.failure_probability)
        self.run_id = str(uuid.uuid4())
        self.lock = threading.Lock()
        self.committed: set[str] = set()
        self.rows: list[dict[str, Any]] = []
        self.calls = {"send": 0, "receive": 0, "delete": 0, "change_visibility": 0}
        self.crash_times: dict[str, float] = {}
        self.recoveries: list[float] = []
        self.recovery_jobs: list[str] = []
        self.active: dict[str, int] = {}
        self.stop = threading.Event()
        self.t0 = 0.0

    def _now(self) -> float:
        return time.monotonic() - self.t0

    def _call(self, name: str, fn, **kwargs):
        with self.lock:
            self.calls[name] += 1
        return fn(QueueUrl=self.queue_url, **kwargs)

    def run(self) -> dict[str, Any]:
        self.t0 = time.monotonic()
        for job_id in sorted(self.messages):
            self._call("send", self.client.send_message, MessageBody=json.dumps({"run_id": self.run_id, "job_id": job_id}))
        threads = [threading.Thread(target=self._worker, args=(f"worker-{i:02d}",), daemon=True) for i in range(self.config.workers)]
        for t in threads:
            t.start()
        deadline = time.monotonic() + self.config.max_seconds_per_policy
        while time.monotonic() < deadline:
            with self.lock:
                if len(self.committed) == len(self.messages) and not any(self.active.values()):
                    break
            time.sleep(0.05)
        timed_out = time.monotonic() >= deadline
        self.stop.set()
        for t in threads:
            t.join(timeout=10)
        end = self._now()
        self._drain()
        return {
            "attempt_rows": self.rows, "api_operations": dict(self.calls),
            "failure_recoveries": list(self.recoveries), "failure_recovery_job_ids": list(self.recovery_jobs),
            "failure_recovery_kinds": ["crash"] * len(self.recoveries),
            "job_ids_in_send_order": sorted(self.messages), "simulation_end": end,
            "completed_jobs": len(self.committed), "deleted_jobs": len(self.committed),
            "event_limit_reached": timed_out, "policy_predicts_duration": self.policy.predicts_duration,
            "job_send_times": {j: 0.0 for j in self.messages},
        }

    def _worker(self, worker_id: str) -> None:
        while not self.stop.is_set():
            # The job type is unknown until the body is read, so receive with a
            # short initial lease and set the policy's lease immediately after.
            response = self._call("receive", self.client.receive_message, MaxNumberOfMessages=1,
                                  WaitTimeSeconds=self.config.poll_wait_seconds,
                                  VisibilityTimeout=self._timeout_value(2.0 * self.config.time_scale),
                                  AttributeNames=["ApproximateReceiveCount"])
            messages = response.get("Messages", [])
            for raw in messages:
                self._process(worker_id, raw)
            if not messages and self.config.poll_wait_seconds == 0:
                time.sleep(0.01)  # avoid spinning when short polling

    def _timeout_value(self, seconds: float):
        return int(min(MAX_VISIBILITY, max(1, math.ceil(seconds - 1e-9)))) if self.config.integer_timeouts else float(seconds)

    def _process(self, worker_id: str, raw: dict) -> None:
        body = json.loads(raw["Body"])
        receipt = raw["ReceiptHandle"]
        if body.get("run_id") != self.run_id:
            self._safe_change(receipt, 0)  # someone else's message: release it untouched
            return
        job_id = body["job_id"]
        message = self.messages[job_id]
        count = int(raw.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
        receive_time = self._now()
        with self.lock:
            decision = self.policy.decide(message.job_type, receive_time)
            interval = self.policy.heartbeat_interval_for(message.job_type, receive_time)
            budget = self.policy.max_total_visibility_for(message.job_type, receive_time)
            duplicate = self.active.get(job_id, 0) > 0
            self.active[job_id] = self.active.get(job_id, 0) + 1
            if job_id in self.crash_times:
                self.recoveries.append(receive_time - self.crash_times.pop(job_id))
                self.recovery_jobs.append(job_id)
        self._safe_change(receipt, self._timeout_value(decision.timeout))
        deadline = receive_time + decision.timeout
        failure = self.failure_model.sample_for(self.config.seed, job_id, count)
        work = message.processing_duration * (failure.fraction if failure.fails else 1.0)
        extensions, start = 0, receive_time
        next_beat = receive_time + interval if interval else math.inf
        while self._now() - start < work:
            time.sleep(min(0.02, max(0.0, work - (self._now() - start))))
            now = self._now()
            if now >= next_beat and extensions < self.policy.max_extensions:
                with self.lock:
                    ext = self.policy.extension_timeout(message.job_type, now)
                    if getattr(self.policy, "dynamic_budget", False):
                        budget = self.policy.max_total_visibility_for(message.job_type, now)
                new_deadline = min(now + ext, start + budget)
                if new_deadline > deadline and self._safe_change(receipt, self._timeout_value(new_deadline - now)):
                    deadline, extensions = new_deadline, extensions + 1
                next_beat = now + interval
        finish = self._now()
        with self.lock:
            self.active[job_id] -= 1
            committed = False
            if failure.fails:
                self.crash_times[job_id] = finish
                self.policy.observe_attempt_end(message.job_type, True, finish)
            else:
                committed = job_id not in self.committed
                self.committed.add(job_id)
                self.policy.observe_attempt_end(message.job_type, False, finish)
                if committed:
                    self.policy.observe(message.job_type, message.processing_duration, finish)
        if not failure.fails:
            try:
                self._call("delete", self.client.delete_message, ReceiptHandle=receipt)
            except Exception:  # noqa: BLE001 - a stale handle may be rejected; the job is already committed
                pass
        with self.lock:
            self.rows.append({
                "policy_name": self.policy.name, "job_id": job_id, "job_type": message.job_type, "worker_id": worker_id,
                "send_time": 0.0, "receive_time": receive_time, "processing_start_time": receive_time,
                "completion_time": finish if not failure.fails else None,
                "actual_processing_duration": finish - receive_time,
                "predicted_processing_duration": decision.prediction, "assigned_visibility_timeout": decision.timeout,
                "number_of_timeout_extensions": extensions, "receive_count": count, "worker_failure": failure.fails,
                "worker_hang": False, "used_fallback_timeout": decision.used_fallback, "duplicate_delivery": count > 1,
                "duplicate_processing": duplicate, "successful_completion": not failure.fails,
                "committed_result": committed, "duplicate_committed_result": (not failure.fails) and not committed,
            })

    def _safe_change(self, receipt: str, timeout) -> bool:
        try:
            self._call("change_visibility", self.client.change_message_visibility, ReceiptHandle=receipt, VisibilityTimeout=timeout)
            return True
        except Exception:  # noqa: BLE001 - lease already lost to a newer receive
            return False

    def _drain(self) -> None:
        """Delete this run's leftover copies (e.g. duplicates still in flight)."""
        idle_polls = 0
        while idle_polls < 3:
            response = self.client.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                                   VisibilityTimeout=self._timeout_value(5), AttributeNames=["ApproximateReceiveCount"])
            messages = response.get("Messages", [])
            idle_polls = idle_polls + 1 if not messages else 0
            for raw in messages:
                if json.loads(raw["Body"]).get("run_id") == self.run_id:
                    self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=raw["ReceiptHandle"])
                else:
                    self._safe_change(raw["ReceiptHandle"], 0)


def scaled_workload(config: ReplicationConfig) -> list[Message]:
    """Seeded jobs, all sent at time 0, durations multiplied by time_scale."""
    messages, _ = generate_workload(WorkloadSpec(config.workload, n_messages=config.n_jobs), config.seed, "replication")
    return [replace(m, creation_time=0.0, processing_duration=m.processing_duration * config.time_scale) for m in messages]


def simulate_twin(policy: VisibilityPolicy, messages: list[Message], config: ReplicationConfig) -> dict[str, Any]:
    return SQSSimulator(messages, policy, FailureModel(config.failure_probability), workers=config.workers, seed=config.seed).run()


def compare(real: dict[str, Any], simulated: dict[str, Any]) -> dict[str, float]:
    keys = ["duplicate_processing_rate", "mean_failure_recovery_time_s", "mean_end_to_end_latency_s", "attempts", "completed_jobs"]
    r, s = summarize_run(real), summarize_run(simulated)
    out = {}
    for key in keys:
        out[f"real_{key}"], out[f"simulated_{key}"] = r[key], s[key]
    # Heartbeat extensions, not raw ChangeMessageVisibility calls: the real worker
    # also spends one call per receive setting the policy's lease.
    out["real_extensions"] = float(sum(row["number_of_timeout_extensions"] for row in real["attempt_rows"]))
    out["simulated_extensions"] = float(sum(row["number_of_timeout_extensions"] for row in simulated["attempt_rows"]))
    out["real_makespan_s"], out["simulated_makespan_s"] = real["simulation_end"], simulated["simulation_end"]
    out["real_timed_out"] = float(real["event_limit_reached"])
    return out


def run_replication(client, queue_url: str, config: ReplicationConfig, policy_names: list[str], output_dir: str | Path) -> pd.DataFrame:
    attributes = client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"])
    if int(attributes["Attributes"]["ApproximateNumberOfMessages"]) != 0:
        raise RuntimeError("The queue is not empty; use a dedicated, empty test queue.")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    messages = scaled_workload(config)
    rows, attempts = [], []
    for name in policy_names:
        def make():
            policy = replication_policies(config.time_scale)[name]
            return IntegerTimeouts(policy) if config.integer_timeouts else policy
        real = ReplicationRun(client, queue_url, make(), messages, config).run()
        twin = simulate_twin(make(), messages, config)
        rows.append({"policy_name": name, **compare(real, twin)})
        attempts.append(pd.DataFrame(real["attempt_rows"]))
    table = pd.DataFrame(rows)
    table.to_csv(output_dir / "replication_summary.csv", index=False)
    pd.concat(attempts, ignore_index=True).to_csv(output_dir / "replication_attempts.csv", index=False)
    mode = "fake" if isinstance(client, FakeSQSClient) else "aws"
    (output_dir / "replication_config.json").write_text(json.dumps({**config.__dict__, "policies": policy_names, "mode": mode}, indent=2), encoding="utf-8")
    return table


def main() -> None:
    parser = argparse.ArgumentParser(description="Small real-SQS replication of the simulator.")
    parser.add_argument("--queue-url", help="URL of a dedicated, empty Standard test queue (never created by this tool)")
    parser.add_argument("--region", default=None)
    parser.add_argument("--fake", action="store_true", help="dry run against an in-memory fake queue; no AWS calls")
    parser.add_argument("--policies", nargs="+", default=list(replication_policies(1.0)))
    parser.add_argument("--jobs", type=int, default=40)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--time-scale", type=float, default=1.0)
    parser.add_argument("--output", default="results/aws_replication")
    parser.add_argument("--no-open", action="store_true", help="do not open the HTML report in a browser")
    args = parser.parse_args()
    config = ReplicationConfig(n_jobs=args.jobs, workers=args.workers, time_scale=args.time_scale, integer_timeouts=not args.fake)
    if args.fake:
        client, queue_url = FakeSQSClient(), "fake://queue"
    else:
        if not args.queue_url:
            parser.error("--queue-url is required unless --fake is given")
        import boto3

        client, queue_url = boto3.client("sqs", region_name=args.region), args.queue_url
    table = run_replication(client, queue_url, config, args.policies, args.output)
    from .replication_report import write_replication_report

    report = write_replication_report(args.output)
    short = table[["policy_name", "real_duplicate_processing_rate", "simulated_duplicate_processing_rate",
                   "real_mean_failure_recovery_time_s", "simulated_mean_failure_recovery_time_s"]].copy()
    short.columns = ["policy", "dup real", "dup sim", "recovery real (s)", "recovery sim (s)"]
    print(short.round(3).to_string(index=False))
    print()
    print(f"Full report with figures: {report.resolve()}")
    if not args.no_open:
        import webbrowser

        webbrowser.open(report.resolve().as_uri())


if __name__ == "__main__":
    main()
