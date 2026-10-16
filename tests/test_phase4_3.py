import unittest

import numpy as np

from src.experiment import build_policies
from src.failure_model import FailureDecision, FailureModel
from src.message import Message
from src.metrics import summarize_run
from src.policies import AdaptiveHeartbeatPolicy, AdaptivePercentilePolicy
from src.simulator import SQSSimulator
from src.workload import WorkloadSpec, generate_workload


class ScriptedFailures:
    def __init__(self, *decisions):
        self.decisions = decisions

    def sample_for(self, seed, job_id, attempt_index):
        if attempt_index <= len(self.decisions):
            return self.decisions[attempt_index - 1]
        return FailureDecision(False, 0.5)


class PageHinkleyTests(unittest.TestCase):
    def policy(self, **kwargs):
        return AdaptivePercentilePolicy(history_window=40, change_detection=True, ph_lambda=3.0, **kwargs)

    def test_doubling_is_detected_and_history_trimmed(self):
        policy = self.policy()
        rng = np.random.default_rng(1)
        for value in rng.lognormal(np.log(5.0), 0.1, size=30):
            policy.observe("a", float(value), 0)
        self.assertEqual(policy.change_points, 0)
        for value in rng.lognormal(np.log(10.0), 0.1, size=10):
            policy.observe("a", float(value), 0)
            if policy.change_points:
                break
        self.assertEqual(policy.change_points, 1)
        self.assertLessEqual(len(policy.history["a"]), 3)
        self.assertGreater(min(policy.history["a"]), 7.0)  # only post-shift durations kept

    def test_decrease_is_detected(self):
        policy = self.policy()
        for _ in range(30):
            policy.observe("a", 10.0, 0)
        for _ in range(10):
            policy.observe("a", 4.0, 0)
        self.assertGreaterEqual(policy.change_points, 1)

    def test_stationary_noise_rarely_alarms(self):
        policy = self.policy()
        rng = np.random.default_rng(2)
        for value in rng.lognormal(np.log(5.0), 0.28, size=500):
            policy.observe("a", float(value), 0)
        self.assertLessEqual(policy.change_points, 2)

    def test_detection_off_by_default(self):
        policy = AdaptivePercentilePolicy(history_window=40)
        for value in [5.0] * 30 + [50.0] * 10:
            policy.observe("a", value, 0)
        self.assertEqual(policy.change_points, 0)
        self.assertEqual(len(policy.history["a"]), 40)

    def test_invalid_parameters_rejected(self):
        for kwargs in [{"ph_lambda": 0}, {"ph_delta": -1}, {"keep_after_change": 0}]:
            with self.assertRaises(ValueError):
                AdaptivePercentilePolicy(**kwargs)

    def test_change_points_reported_in_metrics(self):
        messages, shift = generate_workload(WorkloadSpec("sudden_shift", n_messages=200), 5, "t")
        policy = self.policy(min_history=5, default_timeout=12.0)
        result = SQSSimulator(messages, policy, FailureModel(0), workers=10, seed=1).run()
        self.assertGreaterEqual(summarize_run(result, shift)["change_points_detected"], 1)


class DynamicBudgetTests(unittest.TestCase):
    def test_in_flight_job_picks_up_larger_budget(self):
        # job-0 (30 s) is received with a 12 s budget. Five 8 s jobs complete at
        # about t=8, after which the learned budget is 60 s. Its heartbeat at
        # t=10.5 then extends past 12 s only if the budget is re-read.
        messages = [Message("job-0", "long", 30.0, 0.0, "t")] + [
            Message(f"job-{i}", "long", 8.0, 0.1 * i, "t") for i in range(1, 6)
        ]

        def receives(dynamic):
            policy = AdaptiveHeartbeatPolicy(
                min_history=2, history_window=10, lease_cap=5.0, default_lease=5.0, dynamic_budget=dynamic,
            )
            policy.max_total_visibility_for = (  # stand-in for a budget learned from history
                lambda job_type, now, p=policy: 12.0 if len(p.history[job_type]) < 2 else 60.0
            )
            result = SQSSimulator(messages, policy, FailureModel(0), workers=6, seed=1).run()
            return sum(1 for r in result["attempt_rows"] if r["job_id"] == "job-0")

        self.assertEqual(receives(dynamic=True), 1)
        self.assertGreater(receives(dynamic=False), 1)

    def test_static_budget_is_default(self):
        self.assertFalse(AdaptiveHeartbeatPolicy().dynamic_budget)


class VariantTests(unittest.TestCase):
    def test_phase4_3_variants_built(self):
        messages, _ = generate_workload(WorkloadSpec("mixed", n_messages=20), 1, "t")
        from src.experiment import DEFAULT_ADAPTIVE_VARIANTS

        names = {p.name: p for p in build_policies(messages, adaptive_variants=DEFAULT_ADAPTIVE_VARIANTS)}
        self.assertTrue(names["adaptive_p90_cpd"].change_detection)
        self.assertTrue(names["adaptive_heartbeat_dynamic"].dynamic_budget)
        both = names["adaptive_heartbeat_cpd_dynamic"]
        self.assertTrue(both.change_detection and both.dynamic_budget)
        self.assertIsInstance(both, AdaptiveHeartbeatPolicy)


if __name__ == "__main__":
    unittest.main()
