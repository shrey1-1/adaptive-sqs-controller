import math
import tempfile
import unittest

import numpy as np
import pandas as pd

from src.experiment import run_experiments
from src.failure_model import FailureDecision, FailureModel
from src.message import Message
from src.metrics import summarize_run
from src.policies import AdaptivePercentilePolicy, FixedHeartbeatPolicy
from src.simulator import SQSSimulator
from src.stats import primary_tests
from src.workload import WorkloadSpec, generate_workload


class ScriptedFailures:
    def __init__(self, *decisions):
        self.decisions = decisions

    def sample_for(self, seed, job_id, attempt_index):
        if attempt_index <= len(self.decisions):
            return self.decisions[attempt_index - 1]
        return FailureDecision(False, 0.5)


def fed(values, **kwargs):
    policy = AdaptivePercentilePolicy(history_window=50, default_timeout=12.0, safety_margin=2.0, **kwargs)
    for value in values:
        policy.observe("a", value, 0)
    return policy


class ColdStartTests(unittest.TestCase):
    def test_no_history_uses_default(self):
        decision = fed([]).decide("a", 0)
        self.assertEqual(decision.timeout, 12.0)
        self.assertTrue(decision.used_fallback)

    def test_partial_history_scales_largest_sample(self):
        decision = fed([4.0, 6.0]).decide("a", 0)
        self.assertAlmostEqual(decision.timeout, 1.5 * 6.0 + 2.0)
        self.assertTrue(decision.used_fallback)

    def test_default_rule_ignores_partial_history(self):
        self.assertEqual(fed([4.0, 6.0], cold_start="default").decide("a", 0).timeout, 12.0)

    def test_full_history_unchanged(self):
        decision = fed([1.0, 2.0, 3.0, 4.0, 5.0]).decide("a", 0)
        self.assertFalse(decision.used_fallback)
        self.assertAlmostEqual(decision.prediction, float(np.percentile([1, 2, 3, 4, 5], 90)))


class ConformalTests(unittest.TestCase):
    def test_rank_is_ceil_n_plus_one_q(self):
        values = list(range(1, 20))  # n = 19 -> rank ceil(20 * 0.9) = 18
        policy = fed(values, quantile_method="conformal")
        self.assertEqual(policy.decide("a", 0).prediction, 18.0)

    def test_too_few_samples_falls_back_to_cold_start(self):
        # n = 8 -> rank 9 does not exist, so the partial cold-start rule applies.
        policy = fed([1.0] * 7 + [3.0], quantile_method="conformal")
        decision = policy.decide("a", 0)
        self.assertTrue(decision.used_fallback)
        self.assertAlmostEqual(decision.timeout, 1.5 * 3.0 + 2.0)
        self.assertFalse(fed([1.0] * 8 + [3.0], quantile_method="conformal").decide("a", 0).used_fallback)

    def test_coverage_guarantee_on_exchangeable_data(self):
        rng = np.random.default_rng(0)
        covered = []
        for _ in range(4000):
            draws = rng.lognormal(1.0, 0.5, size=11)
            policy = fed(draws[:10], quantile_method="conformal", min_history=5)
            covered.append(draws[10] <= policy.decide("a", 0).prediction)
        self.assertGreaterEqual(np.mean(covered), 0.89)  # guarantee: >= 10/11 * ... >= 0.9 in expectation


class CostAwareTests(unittest.TestCase):
    def test_prior_gives_initial_quantile(self):
        policy = AdaptivePercentilePolicy(cost_ratio=1.0)
        self.assertAlmostEqual(policy.target_quantile(), 1 - 0.05 / 0.95)

    def test_quantile_tracks_failure_rate(self):
        policy = AdaptivePercentilePolicy(cost_ratio=1.0, failure_prior=(1e-9, 1e-9))
        for i in range(1000):
            policy.observe_attempt_end("a", i % 25 < 2, 0)  # 8% failures
        self.assertAlmostEqual(policy.target_quantile(), 1 - 0.08 / 0.92, places=3)

    def test_quantile_clamped(self):
        safe = AdaptivePercentilePolicy(cost_ratio=1.0)
        for _ in range(5000):
            safe.observe_attempt_end("a", False, 0)
        self.assertEqual(safe.target_quantile(), 0.995)
        risky = AdaptivePercentilePolicy(cost_ratio=4.0)
        for _ in range(100):
            risky.observe_attempt_end("a", True, 0)
        self.assertEqual(risky.target_quantile(), 0.5)

    def test_fixed_percentile_unaffected_by_failures(self):
        policy = AdaptivePercentilePolicy()
        policy.observe_attempt_end("a", True, 0)
        self.assertEqual(policy.target_quantile(), 0.9)

    def test_simulator_reports_attempt_outcomes(self):
        messages, _ = generate_workload(WorkloadSpec("mixed", n_messages=60), 3, "t")
        policy = AdaptivePercentilePolicy(cost_ratio=1.0)
        SQSSimulator(messages, policy, FailureModel(0.3), workers=4, seed=1).run()
        self.assertGreater(policy.failures_seen, 0)
        self.assertGreater(policy.attempts_seen, policy.failures_seen)

    def test_invalid_options_rejected(self):
        for kwargs in [{"cost_ratio": 0}, {"quantile_method": "median"}, {"cold_start": "zero"}, {"partial_factor": 0.5}]:
            with self.assertRaises(ValueError):
                AdaptivePercentilePolicy(**kwargs)


class LivenessProbeTests(unittest.TestCase):
    def test_probe_kills_hung_worker_before_budget(self):
        # Hang at t=3, probe fires at 5.5; the last heartbeat (t=5) set the
        # deadline to 7, so the job is redelivered at 7: recovery 4 s.
        policy = FixedHeartbeatPolicy(2, heartbeat_interval=1, max_extensions=100, max_total_visibility=60)
        failures = ScriptedFailures(FailureDecision(False, 0.3, hangs=True))
        result = SQSSimulator(
            [Message("job-1", "long", 10.0, 0.0, "t")], policy, failures, workers=1, seed=1, hang_detection_timeout=2.5
        ).run()
        self.assertAlmostEqual(result["failure_recoveries"][0], 4.0)
        self.assertEqual(result["completed_jobs"], 1)


class MetricTests(unittest.TestCase):
    def test_coverage_and_tradeoff_cost(self):
        messages, _ = generate_workload(WorkloadSpec("mixed", n_messages=80), 4, "t")
        result = SQSSimulator(messages, AdaptivePercentilePolicy(history_window=20), FailureModel(0.1), workers=4, seed=1).run()
        summary = summarize_run(result)
        self.assertTrue(0.5 < summary["prediction_coverage"] <= 1.0)
        self.assertGreater(summary["tradeoff_cost_s_per_job"], 0)


class PrimaryTestTests(unittest.TestCase):
    def test_supported_requires_direction_and_holm(self):
        seeds = list(range(1, 13))
        runs = pd.DataFrame(
            [{"scenario": "s", "policy_name": "a", "seed": s, "m": 1.0 + 0.01 * s} for s in seeds]
            + [{"scenario": "s", "policy_name": "b", "seed": s, "m": 2.0 + 0.02 * s} for s in seeds]
        )
        hypotheses = [
            {"id": "H1", "scenario": "s", "reference": "a", "comparator": "b", "metric": "m", "expected": "lower"},
            {"id": "H2", "scenario": "s", "reference": "a", "comparator": "b", "metric": "m", "expected": "higher"},
        ]
        table = primary_tests(runs, hypotheses).set_index("hypothesis")
        self.assertTrue(table.loc["H1", "supported"])
        self.assertFalse(table.loc["H2", "supported"])
        self.assertEqual(table.loc["H1", "seeds_in_expected_direction"], 12)
        self.assertAlmostEqual(table.loc["H1", "p_holm"], 2 * table.loc["H1", "p_value"])


class Phase4IntegrationTests(unittest.TestCase):
    def test_run_includes_phase4_policies_and_primary_table(self):
        config = {
            "n_messages": 40,
            "workers": 4,
            "seeds": [1, 2, 3],
            "scenarios": {"mixed": {"workload": "mixed", "failure": "medium"}},
            "primary_hypotheses": [
                {"id": "H1", "scenario": "mixed", "reference": "adaptive_conformal", "comparator": "adaptive_p90", "metric": "duplicate_processing_rate"}
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            _, runs, _ = run_experiments(config, tmp)
            primary = pd.read_csv(f"{tmp}/primary_hypotheses.csv")
        expected = {"adaptive_cost_aware", "adaptive_conformal", "adaptive_cost_conformal", "adaptive_p90_fixed_fallback"}
        self.assertTrue(expected <= set(runs.policy_name))
        self.assertEqual(primary.loc[0, "n_pairs"], 3)
        self.assertIn("prediction_coverage", runs)


if __name__ == "__main__":
    unittest.main()
