import unittest
from dataclasses import replace

import numpy as np

from src.experiment import realism_options
from src.failure_model import FailureDecision, FailureModel
from src.message import Message
from src.metrics import summarize_run
from src.policies import AdaptivePercentilePolicy, FixedHeartbeatPolicy, FixedPolicy
from src.simulator import SQSSimulator
from src.workload import WorkloadSpec, generate_workload


class ScriptedFailures:
    def __init__(self, *decisions):
        self.decisions = decisions

    def sample_for(self, seed, job_id, attempt_index):
        if attempt_index <= len(self.decisions):
            return self.decisions[attempt_index - 1]
        return FailureDecision(False, 0.5)


def job(duration, at=0.0, name="job-1", poison=False):
    return Message(name, "a", float(duration), float(at), "t", poison=poison)


class DeadLetterTests(unittest.TestCase):
    def test_slow_healthy_job_is_dead_lettered_after_max_receives(self):
        # Timeout 2, duration 10: receives at 0, 2, 4; the 4th receive (t=6)
        # exceeds maxReceiveCount=3, so the message moves to the DLQ even though
        # the first attempt finishes the work at t=10.
        result = SQSSimulator([job(10)], FixedPolicy(2), FailureModel(0), workers=5, seed=1, max_receive_count=3).run()
        self.assertEqual(result["dead_lettered"], {"job-1": 6.0})
        self.assertEqual(result["api_operations"]["delete"], 0)
        summary = summarize_run(result)
        self.assertEqual(summary["false_dlq_rate"], 1.0)
        self.assertEqual(summary["dlq_but_completed"], 1.0)
        self.assertEqual(summary["dead_lettered_jobs"], 1.0)
        self.assertFalse(result["event_limit_reached"])

    def test_poison_message_quarantined_and_healthy_jobs_complete(self):
        messages = [job(4, name="job-p", poison=True), job(1, at=0.5, name="job-h")]
        result = SQSSimulator(messages, FixedPolicy(5), FailureModel(0), workers=2, seed=1, max_receive_count=2).run()
        self.assertIn("job-p", result["dead_lettered"])
        self.assertNotIn("job-h", result["dead_lettered"])
        summary = summarize_run(result)
        self.assertEqual(summary["false_dlq_rate"], 0.0)
        self.assertEqual(summary["healthy_completion_rate"], 1.0)
        self.assertAlmostEqual(summary["poison_quarantine_time_s"], 10.0)  # two 5 s leases

    def test_poison_without_dlq_rejected(self):
        with self.assertRaises(ValueError):
            SQSSimulator([job(1, poison=True)], FixedPolicy(5), FailureModel(0))

    def test_poison_marking_leaves_workload_unchanged(self):
        plain, _ = generate_workload(WorkloadSpec("mixed", n_messages=500), 7, "t")
        marked, _ = generate_workload(WorkloadSpec("mixed", n_messages=500, poison_fraction=0.05), 7, "t")
        self.assertEqual([replace(m, poison=False) for m in marked], plain)
        self.assertTrue(10 <= sum(m.poison for m in marked) <= 45)


class ServiceVariationTests(unittest.TestCase):
    def test_retry_jitter_varies_by_attempt_and_is_shared_across_policies(self):
        def durations(policy):
            result = SQSSimulator([job(10)], policy, FailureModel(0), workers=10, seed=4, retry_jitter=0.3).run()
            return sorted(round(r["actual_processing_duration"], 9) for r in result["attempt_rows"] if r["successful_completion"])

        short = durations(FixedPolicy(2))
        self.assertGreater(len(set(short)), 1)
        self.assertNotIn(10.0, short)
        # The first attempt of the job has the same duration under any policy.
        first = lambda p: SQSSimulator([job(10)], p, FailureModel(0), workers=10, seed=4, retry_jitter=0.3).run()["attempt_rows"]
        a = [r for r in first(FixedPolicy(2)) if r["receive_count"] == 1][0]["actual_processing_duration"]
        b = [r for r in first(FixedPolicy(30)) if r["receive_count"] == 1][0]["actual_processing_duration"]
        self.assertAlmostEqual(a, b)

    def test_jitter_has_mean_one(self):
        messages = [job(1.0, at=i * 10.0, name=f"job-{i}") for i in range(2000)]
        result = SQSSimulator(messages, FixedPolicy(100), FailureModel(0), workers=2, seed=2, retry_jitter=0.3).run()
        self.assertAlmostEqual(np.mean([r["actual_processing_duration"] for r in result["attempt_rows"]]), 1.0, delta=0.03)

    def test_worker_speeds_fixed_per_seed(self):
        a = SQSSimulator([job(1)], FixedPolicy(5), FailureModel(0), workers=6, seed=9, worker_speed_spread=0.3)
        b = SQSSimulator([job(1)], FixedPolicy(50), FailureModel(0), workers=6, seed=9, worker_speed_spread=0.3)
        self.assertEqual(a.worker_speed, b.worker_speed)
        self.assertGreater(len(set(a.worker_speed.values())), 1)

    def test_observed_duration_is_the_attempt_duration(self):
        policy = AdaptivePercentilePolicy(min_history=1, history_window=5)
        SQSSimulator([job(10)], policy, FailureModel(0), workers=2, seed=4, retry_jitter=0.3).run()
        self.assertNotEqual(list(policy.history["a"]), [10.0])


class LatencyTests(unittest.TestCase):
    def test_delete_latency_can_cause_a_duplicate(self):
        # Timeout 5, duration 4.85: without latency the job finishes in time; with
        # 0.1 s latency it starts at 0.1, finishes at 4.95, and the delete lands
        # at 5.05 — after the lease expired at 5.
        without = SQSSimulator([job(4.85)], FixedPolicy(5), FailureModel(0), workers=2, seed=1).run()
        with_latency = SQSSimulator([job(4.85)], FixedPolicy(5), FailureModel(0), workers=2, seed=1, api_latency=0.1).run()
        self.assertEqual(without["api_operations"]["receive"], 1)
        self.assertEqual(with_latency["api_operations"]["receive"], 2)

    def test_late_heartbeat_loses_the_lease(self):
        # Lease 2, heartbeat at 1.5: a 0.6 s latency applies the extension at 2.1,
        # after the lease expired and the message was re-received.
        def receives(latency):
            policy = FixedHeartbeatPolicy(2, heartbeat_interval=1.5, max_extensions=10, max_total_visibility=60)
            return SQSSimulator([job(5)], policy, FailureModel(0), workers=2, seed=1, api_latency=latency).run()["api_operations"]["receive"]

        self.assertEqual(receives(0.1), 1)
        self.assertGreater(receives(0.6), 1)

    def test_batched_receives_count_one_call_per_dispatch_round(self):
        # Batching only merges messages delivered in the same dispatch round.
        # Here an outage frees all 5 workers in one event, and the 5 queued jobs
        # are received together: one call instead of five.
        messages = [job(10, name=f"job-a{i}") for i in range(5)] + [job(1, at=1.0, name=f"job-b{i}") for i in range(5)]

        def calls(batch):
            return SQSSimulator(
                messages, FixedPolicy(30), FailureModel(0), workers=5, seed=1,
                receive_batch_size=batch, outages=[(3.0, [0, 1, 2, 3, 4])],
            ).run()["api_operations"]["receive"]

        self.assertEqual(calls(1) - calls(10), 4)


class OutageTests(unittest.TestCase):
    def test_outage_crashes_running_attempts_on_its_workers(self):
        messages = [job(5, name="job-a"), job(5, name="job-b")]
        result = SQSSimulator(messages, FixedPolicy(8), FailureModel(0), workers=2, seed=1, outages=[(1.0, [0, 1])]).run()
        self.assertEqual(sum(r["worker_failure"] for r in result["attempt_rows"]), 2)
        self.assertEqual(sorted(result["failure_recoveries"]), [7.0, 7.0])  # lease ends at 8
        self.assertEqual(result["completed_jobs"], 2)

    def test_outages_are_seeded_and_policy_independent(self):
        messages, _ = generate_workload(WorkloadSpec("mixed", n_messages=100), 3, "t")
        scenario = {"outage_rate": 0.02, "outage_size": 3, "retry_jitter": 0.2}
        a = realism_options(scenario, messages, 3, 10)
        self.assertEqual(a, realism_options(scenario, messages, 3, 10))
        self.assertNotEqual(a["outages"], realism_options(scenario, messages, 4, 10)["outages"])
        self.assertTrue(all(len(set(ws)) == 3 for _, ws in a["outages"]))
        self.assertEqual(a["retry_jitter"], 0.2)
        self.assertEqual(realism_options({}, messages, 3, 10), {})


if __name__ == "__main__":
    unittest.main()
