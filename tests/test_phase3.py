import math
import tempfile
import unittest

from src.experiment import run_experiments
from src.failure_model import FailureDecision, FailureModel
from src.message import Message
from src.metrics import summarize_run
from src.policies import AdaptiveHeartbeatPolicy, FixedHeartbeatPolicy, FixedPolicy
from src.simulator import SQSSimulator


class ScriptedFailures:
    """Deterministic stand-in for FailureModel: one decision per delivery number."""

    def __init__(self, *decisions):
        self.decisions = decisions

    def sample_for(self, seed, job_id, attempt_index):
        if attempt_index <= len(self.decisions):
            return self.decisions[attempt_index - 1]
        return FailureDecision(False, 0.5)


def one_message(duration):
    return [Message("job-1", "long", duration, 0.0, "t")]


class HeartbeatBeforeCrashTests(unittest.TestCase):
    def test_crashing_worker_heartbeats_until_it_dies(self):
        # Lease 2 s, heartbeat every 1 s; crash at t=5. Heartbeats at 1..4 push the
        # deadline to 6, so recovery is 1 s. Before the fix the doomed attempt
        # never heartbeated and its lease expired at t=2 while still running.
        policy = FixedHeartbeatPolicy(2, heartbeat_interval=1, max_extensions=100, max_total_visibility=60)
        failures = ScriptedFailures(FailureDecision(True, 0.5))
        result = SQSSimulator(one_message(10), policy, failures, workers=2, seed=1).run()
        self.assertEqual(result["failure_recoveries"], [1.0])
        crashed = next(r for r in result["attempt_rows"] if r["worker_failure"])
        self.assertEqual(crashed["number_of_timeout_extensions"], 4)
        self.assertFalse(any(r["duplicate_processing"] for r in result["attempt_rows"]))


class HangTests(unittest.TestCase):
    def test_hung_worker_heartbeats_until_budget_then_is_reclaimed(self):
        # Hang at t=3; heartbeats keep extending until the 12 s budget, the lease
        # expires at 12, the worker is restarted and the job redelivered.
        policy = FixedHeartbeatPolicy(2, heartbeat_interval=1, max_extensions=100, max_total_visibility=12)
        failures = ScriptedFailures(FailureDecision(False, 0.3, hangs=True))
        result = SQSSimulator(one_message(10), policy, failures, workers=1, seed=1).run()
        self.assertEqual(result["worker_hangs"], 1)
        self.assertEqual(result["failure_recovery_kinds"], ["hang"])
        self.assertAlmostEqual(result["failure_recoveries"][0], 9.0)
        self.assertEqual(result["completed_jobs"], 1)
        hung = [r for r in result["attempt_rows"] if r["worker_hang"]]
        self.assertEqual(len(hung), 1)
        self.assertTrue(hung[0]["worker_failure"])
        summary = summarize_run(result)
        self.assertAlmostEqual(summary["mean_hang_recovery_time_s"], 9.0)
        self.assertEqual(summary["worker_hangs"], 1.0)

    def test_hang_without_heartbeat_ends_at_fixed_timeout(self):
        failures = ScriptedFailures(FailureDecision(False, 0.3, hangs=True))
        result = SQSSimulator(one_message(10), FixedPolicy(4), failures, workers=1, seed=1).run()
        self.assertAlmostEqual(result["failure_recoveries"][0], 1.0)
        self.assertEqual(result["completed_jobs"], 1)

    def test_hung_worker_released_even_if_job_deleted_elsewhere(self):
        # Two workers: attempt 1 hangs; its lease (timeout 4) expires at 4 and
        # attempt 2 completes the job. The hung worker must still be freed.
        failures = ScriptedFailures(FailureDecision(False, 0.2, hangs=True))
        messages = one_message(3) + [Message("job-2", "long", 1.0, 5.0, "t")]
        result = SQSSimulator(messages, FixedPolicy(4), failures, workers=1, seed=1).run()
        self.assertEqual(result["completed_jobs"], 2)
        self.assertFalse(result["event_limit_reached"])

    def test_hang_draw_does_not_change_crash_draws(self):
        plain, with_hangs = FailureModel(0.3), FailureModel(0.3, hang_probability=0.5)
        for k in range(1, 200):
            a, b = plain.sample_for(7, f"job-{k}", 1), with_hangs.sample_for(7, f"job-{k}", 1)
            self.assertEqual((a.fails, a.fraction), (b.fails, b.fraction))
            self.assertFalse(b.fails and b.hangs)
        rate = sum(with_hangs.sample_for(7, f"job-{k}", 1).hangs for k in range(4000)) / 4000
        self.assertAlmostEqual(rate, 0.7 * 0.5, delta=0.03)


class AdaptiveHeartbeatPolicyTests(unittest.TestCase):
    def policy(self, **kwargs):
        defaults = dict(min_history=3, history_window=50, lease_cap=5.0, default_lease=5.0, max_total_visibility=40.0)
        return AdaptiveHeartbeatPolicy(**{**defaults, **kwargs})

    def test_cold_start_uses_default_lease_and_global_budget(self):
        policy = self.policy()
        self.assertEqual(policy.decide("short", 0).timeout, 5.0)
        self.assertAlmostEqual(policy.heartbeat_interval_for("short", 0), 3.5)
        self.assertEqual(policy.max_total_visibility_for("short", 0), 40.0)

    def test_short_class_gets_covering_lease_and_small_budget(self):
        policy = self.policy()
        for value in [1.0, 1.0, 1.0, 1.0]:
            policy.observe("short", value, 0)
        self.assertAlmostEqual(policy.decide("short", 0).timeout, 3.0)  # P90 1 + margin 2
        self.assertAlmostEqual(policy.heartbeat_interval_for("short", 0), 2.1)
        self.assertAlmostEqual(policy.max_total_visibility_for("short", 0), 3.25)  # P99 1 x 1.25 + 2

    def test_long_class_lease_capped_and_budget_bounded(self):
        policy = self.policy()
        for value in [30.0, 32.0, 34.0, 40.0]:
            policy.observe("long", value, 0)
        self.assertEqual(policy.decide("long", 0).timeout, 5.0)
        self.assertEqual(policy.max_total_visibility_for("long", 0), 40.0)  # 1.25 x ~30 + 2 > cap

    def test_fixed_budget_ablation_ignores_history(self):
        policy = self.policy(adaptive_budget=False)
        for value in [1.0] * 5:
            policy.observe("short", value, 0)
        self.assertEqual(policy.max_total_visibility_for("short", 0), 40.0)
        self.assertAlmostEqual(policy.decide("short", 0).timeout, 3.0)

    def test_invalid_parameters_rejected(self):
        for kwargs in [{"heartbeat_fraction": 1.0}, {"lease_cap": 0}, {"budget_factor": 0.5}]:
            with self.assertRaises(ValueError):
                self.policy(**kwargs)

    def test_long_job_survives_on_heartbeats_alone(self):
        # Lease capped at 5 s; a 30 s job completes on renewals with no duplicate.
        policy = self.policy()
        result = SQSSimulator(one_message(30), policy, FailureModel(0), workers=2, seed=1).run()
        self.assertEqual(result["api_operations"]["receive"], 1)
        self.assertGreater(result["api_operations"]["change_visibility"], 5)


class Phase3IntegrationTests(unittest.TestCase):
    def test_hang_scenario_runs_with_heartbeat_policies(self):
        config = {
            "n_messages": 40,
            "workers": 4,
            "seeds": [1, 2],
            "scenarios": {"hangs": {"workload": "mixed", "failure": "low", "hang": "medium"}},
        }
        with tempfile.TemporaryDirectory() as tmp:
            _, runs, _ = run_experiments(config, tmp)
        self.assertTrue({"adaptive_heartbeat", "adaptive_heartbeat_fixed_budget", "fixed_heartbeat"} <= set(runs.policy_name))
        self.assertGreater(runs["worker_hangs"].sum(), 0)
        self.assertFalse(runs["event_limit_reached"].any())
        self.assertTrue((runs["successful_completion_rate"] == 1.0).all())


if __name__ == "__main__":
    unittest.main()
