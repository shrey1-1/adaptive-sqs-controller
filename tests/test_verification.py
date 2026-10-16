"""Simulator verification (fix D): outputs checked against independently derived
results — closed-form counts for simple cases and the Erlang C queueing formula.

These pin the simulator's behaviour before Phase 5 adds realism features, which
are off by default; every test here must keep passing unchanged afterwards.
"""
import math
import unittest

import numpy as np

from src.failure_model import FailureDecision, FailureModel
from src.message import Message
from src.metrics import summarize_run
from src.policies import FixedHeartbeatPolicy, FixedPolicy
from src.simulator import SQSSimulator


class ScriptedFailures:
    def __init__(self, *decisions):
        self.decisions = decisions

    def sample_for(self, seed, job_id, attempt_index):
        if attempt_index <= len(self.decisions):
            return self.decisions[attempt_index - 1]
        return FailureDecision(False, 0.5)


def isolated_jobs(durations, gap=1000.0):
    """Jobs far enough apart that they never overlap or queue."""
    return [Message(f"job-{i:03d}", "a", float(d), i * gap, "v") for i, d in enumerate(durations)]


class ClosedFormTests(unittest.TestCase):
    def test_receives_per_job_equal_ceil_duration_over_timeout(self):
        # With timeout T and ample workers, a job of duration D is received at
        # 0, T, 2T, ... while the first attempt is still running: ceil(D/T) times.
        timeout = 3.0
        durations = [1.0, 2.9, 3.1, 7.0, 10.0, 0.5]
        result = SQSSimulator(isolated_jobs(durations), FixedPolicy(timeout), FailureModel(0), workers=8, seed=1).run()
        expected = [math.ceil(d / timeout) for d in durations]
        receives = {}
        for row in result["attempt_rows"]:
            receives[row["job_id"]] = receives.get(row["job_id"], 0) + 1
        self.assertEqual([receives[f"job-{i:03d}"] for i in range(len(durations))], expected)
        self.assertEqual(result["api_operations"], {"send": 6, "receive": sum(expected), "delete": 6, "change_visibility": 0})
        # Every extra receive overlaps the still-running first attempt and later
        # finishes as a rejected duplicate result.
        rows = result["attempt_rows"]
        self.assertEqual(sum(r["duplicate_processing"] for r in rows), sum(expected) - len(durations))
        self.assertEqual(sum(r["duplicate_committed_result"] for r in rows), sum(expected) - len(durations))

    def test_duplicate_share_matches_tail_probability(self):
        # Without contention, the share of jobs redelivered is exactly P(D > T).
        rng = np.random.default_rng(3)
        durations = rng.lognormal(np.log(5.0), 0.4, size=400)
        timeout = 6.0
        result = SQSSimulator(isolated_jobs(durations), FixedPolicy(timeout), FailureModel(0), workers=4, seed=1).run()
        redelivered = {r["job_id"] for r in result["attempt_rows"] if r["receive_count"] > 1}
        self.assertEqual(len(redelivered), int((durations > timeout).sum()))

    def test_latency_and_throughput_without_contention(self):
        durations = [2.0, 4.0, 6.0]
        result = SQSSimulator(isolated_jobs(durations, gap=10.0), FixedPolicy(30), FailureModel(0), workers=2, seed=1).run()
        summary = summarize_run(result)
        self.assertAlmostEqual(summary["mean_end_to_end_latency_s"], 4.0)
        self.assertAlmostEqual(result["simulation_end"], 26.0)  # last job sent at 20, runs 6 s
        self.assertAlmostEqual(summary["throughput_jobs_per_s"], 3 / 26.0)
        self.assertEqual(summary["duplicate_processing_rate"], 0.0)

    def test_crash_recovery_equals_timeout_minus_progress(self):
        # Crash at fraction f of D under timeout T: redelivered at T, so the job
        # waits T - f * D after the crash.
        failures = ScriptedFailures(FailureDecision(True, 0.25))
        result = SQSSimulator(isolated_jobs([8.0]), FixedPolicy(10), failures, workers=1, seed=1).run()
        self.assertAlmostEqual(result["failure_recoveries"][0], 10 - 0.25 * 8.0)
        self.assertEqual(result["completed_jobs"], 1)

    def test_heartbeat_extension_count(self):
        # Lease 2 s renewed every 1 s: a 7.5 s job heartbeats at t = 1..7.
        policy = FixedHeartbeatPolicy(2, heartbeat_interval=1, max_extensions=100, max_total_visibility=1000)
        result = SQSSimulator(isolated_jobs([7.5]), policy, FailureModel(0), workers=1, seed=1).run()
        self.assertEqual(result["api_operations"]["change_visibility"], 7)
        self.assertEqual(result["api_operations"]["receive"], 1)

    def test_heartbeat_extensions_stop_at_cap(self):
        # max_extensions = 3: deadline reaches 3 + 2 = 5, so a 7.5 s job is
        # redelivered at t = 5.
        policy = FixedHeartbeatPolicy(2, heartbeat_interval=1, max_extensions=3, max_total_visibility=1000)
        result = SQSSimulator(isolated_jobs([7.5]), policy, FailureModel(0), workers=2, seed=1).run()
        second = [r for r in result["attempt_rows"] if r["receive_count"] == 2]
        self.assertEqual(len(second), 1)
        self.assertAlmostEqual(second[0]["receive_time"], 5.0)


def erlang_c_wait(arrival_rate, service_rate, servers):
    """Mean queueing delay of an M/M/c queue (Erlang C)."""
    a = arrival_rate / service_rate
    rho = a / servers
    terms = sum(a**k / math.factorial(k) for k in range(servers))
    tail = a**servers / math.factorial(servers) / (1 - rho)
    p_wait = tail / (terms + tail)
    return p_wait / (servers * service_rate - arrival_rate)


class QueueingTheoryTests(unittest.TestCase):
    def test_mean_wait_matches_erlang_c(self):
        # Poisson arrivals, exponential service, c FCFS workers, no failures and a
        # timeout long enough never to expire: the simulator is an M/M/c queue.
        rng = np.random.default_rng(11)
        n, servers, arrival_rate, service_rate = 30000, 4, 3.0, 1.0
        arrivals = np.cumsum(rng.exponential(1 / arrival_rate, size=n))
        durations = rng.exponential(1 / service_rate, size=n)
        messages = [Message(f"job-{i:05d}", "a", float(d), float(t), "v") for i, (t, d) in enumerate(zip(arrivals, durations))]
        result = SQSSimulator(messages, FixedPolicy(1e6), FailureModel(0), workers=servers, seed=1).run()
        waits = sorted((r["send_time"], r["receive_time"] - r["send_time"]) for r in result["attempt_rows"])
        steady = np.asarray([w for _, w in waits[n // 10:]])  # drop warm-up
        expected = erlang_c_wait(arrival_rate, service_rate, servers)
        self.assertAlmostEqual(steady.mean(), expected, delta=0.1 * expected)
        self.assertFalse(result["event_limit_reached"])


if __name__ == "__main__":
    unittest.main()
