import math
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from src.experiment import run_experiments
from src.failure_model import FailureModel
from src.message import Message
from src.metrics import summarize_run
from src.policies import AdaptivePercentilePolicy, ClassBasedPolicy, FixedPolicy
from src.simulator import SQSSimulator
from src.stats import holm_adjust, paired_comparisons, pareto_optimal, wilcoxon_signed_rank
from src.workload import WorkloadSpec, generate_workload, oracle_class_timeouts, pretuned_class_timeouts


class MarginTests(unittest.TestCase):
    def fed(self, **kwargs):
        policy = AdaptivePercentilePolicy(percentile=50, min_history=1, history_window=10, **kwargs)
        for value in [2.0, 4.0, 6.0, 8.0, 10.0]:
            policy.observe("a", value, 0)
        return policy

    def test_relative_margin_scales_with_prediction(self):
        decision = self.fed(safety_margin=0.0, relative_margin=0.25).decide("a", 0)
        self.assertAlmostEqual(decision.prediction, 6.0)
        self.assertAlmostEqual(decision.timeout, 7.5)

    def test_mad_margin_uses_median_absolute_deviation(self):
        # median 6, absolute deviations [4, 2, 0, 2, 4] -> MAD 2
        decision = self.fed(safety_margin=1.0, mad_multiplier=1.5).decide("a", 0)
        self.assertAlmostEqual(decision.timeout, 6.0 + 1.0 + 3.0)

    def test_negative_margin_rejected(self):
        with self.assertRaises(ValueError):
            AdaptivePercentilePolicy(relative_margin=-0.1)

    def test_policies_accept_custom_names(self):
        self.assertEqual(AdaptivePercentilePolicy(name="adaptive_x").name, "adaptive_x")
        self.assertEqual(AdaptivePercentilePolicy().name, "adaptive_p90")
        self.assertEqual(ClassBasedPolicy({}, 5.0, name="pretuned_per_class").name, "pretuned_per_class")


class PretunedBaselineTests(unittest.TestCase):
    def test_pretuned_ignores_evaluated_workload(self):
        spec = WorkloadSpec("mixed", n_messages=200)
        evaluated, _ = generate_workload(spec, 11, "eval")
        pretuned = pretuned_class_timeouts(spec, 11, 2.0)
        self.assertEqual(pretuned, pretuned_class_timeouts(spec, 11, 2.0))
        self.assertNotEqual(pretuned, oracle_class_timeouts(evaluated, 2.0))

    def test_pretuned_goes_stale_after_shift(self):
        # Calibration sees only pre-shift jobs; the oracle sees the doubled
        # post-shift durations too, so its timeout must be larger.
        spec = WorkloadSpec("sudden_shift", n_messages=200)
        evaluated, _ = generate_workload(spec, 11, "eval")
        self.assertLess(pretuned_class_timeouts(spec, 11)["medium"], oracle_class_timeouts(evaluated)["medium"])


class SteadyStateTests(unittest.TestCase):
    def test_zero_warmup_matches_full_metrics(self):
        messages, _ = generate_workload(WorkloadSpec("mixed", n_messages=60), 3, "s")
        result = SQSSimulator(messages, FixedPolicy(4.0), FailureModel(0.1), workers=4, seed=1).run()
        summary = summarize_run(result, warmup_fraction=0.0)
        self.assertAlmostEqual(summary["steady_duplicate_processing_rate"], summary["duplicate_processing_rate"])
        self.assertAlmostEqual(summary["steady_mean_end_to_end_latency_s"], summary["mean_end_to_end_latency_s"])
        self.assertAlmostEqual(summary["steady_mean_failure_recovery_time_s"], summary["mean_failure_recovery_time_s"])

    def test_warmup_excludes_early_jobs(self):
        # The first job duplicates (timeout 2 < 5); the second does not.
        messages = [
            Message("job-a", "short", 5.0, 0.0, "t"),
            Message("job-b", "short", 1.0, 10.0, "t"),
        ]
        result = SQSSimulator(messages, FixedPolicy(2), FailureModel(0), workers=2, seed=1).run()
        summary = summarize_run(result, warmup_fraction=0.5)
        self.assertGreater(summary["duplicate_processing_rate"], 0)
        self.assertEqual(summary["steady_duplicate_processing_rate"], 0)


class StatsTests(unittest.TestCase):
    def test_wilcoxon_exact_all_positive(self):
        _, p5, n5 = wilcoxon_signed_rank([1, 2, 3, 4, 5])
        self.assertEqual(n5, 5)
        self.assertAlmostEqual(p5, 2 / 32)
        _, p10, _ = wilcoxon_signed_rank(np.arange(1, 11))
        self.assertAlmostEqual(p10, 2 / 1024)

    def test_wilcoxon_symmetric_and_drops_zeros(self):
        w, p, n = wilcoxon_signed_rank([0.0, 1.0, -2.0, 3.0, -4.0, 0.0])
        self.assertEqual(n, 4)
        self.assertGreater(p, 0.5)
        self.assertTrue(math.isnan(wilcoxon_signed_rank([0.0, 0.0])[1]))

    def test_wilcoxon_ties_use_normal_approximation(self):
        _, p, n = wilcoxon_signed_rank([1.0] * 12)
        self.assertEqual(n, 12)
        self.assertLess(p, 0.01)

    def test_holm_adjustment(self):
        adjusted = holm_adjust([0.01, 0.04, 0.03, math.nan])
        for got, expected in zip(adjusted[:3], [0.03, 0.06, 0.06]):
            self.assertAlmostEqual(got, expected)
        self.assertTrue(math.isnan(adjusted[3]))

    def test_pareto_optimal(self):
        points = pd.DataFrame({"x": [1, 2, 3, 2, math.nan], "y": [3, 2, 1, 3, 0]})
        self.assertEqual(pareto_optimal(points, "x", "y").tolist(), [True, True, True, False, False])

    def test_paired_comparison_sign_and_pairing(self):
        runs = pd.DataFrame(
            {
                "scenario": "s",
                "seed": [1, 2, 3, 1, 2, 3],
                "policy_name": ["ref"] * 3 + ["other"] * 3,
                "duplicate_processing_rate": [0.1, 0.2, 0.3, 0.2, 0.4, 0.6],
            }
        )
        table = paired_comparisons(runs, reference="ref", metrics=["duplicate_processing_rate"])
        row = table.iloc[0]
        self.assertEqual(row["n_pairs"], 3)
        self.assertAlmostEqual(row["mean_difference"], -0.2)
        self.assertEqual(row["reference_better_seeds"], 3)


class ExperimentIntegrationTests(unittest.TestCase):
    def test_run_writes_new_policies_and_paired_table(self):
        config = {
            "n_messages": 30,
            "workers": 4,
            "seeds": [1, 2, 3],
            "scenarios": {
                "mixed": {"workload": "mixed", "failure": "medium"},
                "sweep": {
                    "workload": "mixed",
                    "failure": "low",
                    "adaptive_variants": {"adaptive_p75": {"percentile": 75}},
                    "policies": ["adaptive_p90", "adaptive_p75"],
                },
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            _, runs, aggregates = run_experiments(config, tmp)
            paired = pd.read_csv(Path(tmp) / "paired_comparisons.csv")
        mixed = set(runs[runs.scenario == "mixed"].policy_name)
        self.assertTrue({"pretuned_per_class", "adaptive_p90_relative", "fixed_per_class"} <= mixed)
        self.assertEqual(set(runs[runs.scenario == "sweep"].policy_name), {"adaptive_p90", "adaptive_p75"})
        self.assertIn("steady_duplicate_processing_rate", runs)
        self.assertIn("pareto_optimal_dup_vs_recovery", aggregates)
        self.assertEqual(set(paired["reference"]), {"adaptive_p90", "adaptive_heartbeat"})
        # The sweep scenario filters out adaptive_heartbeat, so only adaptive_p90 references it.
        sweep = paired[paired.scenario == "sweep"]
        self.assertEqual(set(sweep.reference), {"adaptive_p90"})
        self.assertEqual(set(sweep.comparator), {"adaptive_p75"})


if __name__ == "__main__":
    unittest.main()
