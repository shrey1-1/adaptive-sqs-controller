import math
import unittest

import pandas as pd

from src.failure_model import FailureModel
from src.message import Message
from src.metrics import aggregate_runs, summarize_run, t_critical
from src.policies import AdaptivePercentilePolicy, FixedPolicy
from src.simulator import SQSSimulator
from src.workload import WorkloadSpec, generate_workload


class DispatchTests(unittest.TestCase):
    def test_deleted_message_does_not_strand_idle_worker(self):
        # job-a times out at t=2 and is re-queued ahead of job-b while the only
        # worker is busy; when job-a completes at t=5 the freed worker must
        # skip the stale copy and still pick up job-b.
        messages = [
            Message("job-a", "short", 5.0, 0.0, "test"),
            Message("job-b", "short", 1.0, 1.0, "test"),
        ]
        result = SQSSimulator(messages, FixedPolicy(2), FailureModel(0), workers=1, seed=1).run()
        self.assertEqual(result["completed_jobs"], 2)
        job_b = next(r for r in result["attempt_rows"] if r["job_id"] == "job-b")
        self.assertAlmostEqual(job_b["receive_time"], 5.0)


class CommonRandomNumberTests(unittest.TestCase):
    def test_policies_see_identical_failures_for_same_attempt(self):
        messages, _ = generate_workload(WorkloadSpec("mixed", n_messages=60), 7, "crn")

        def first_attempt_failures(policy):
            result = SQSSimulator(messages, policy, FailureModel(0.3), workers=4, seed=99).run()
            return {
                r["job_id"]: (r["worker_failure"], round(r["actual_processing_duration"], 9))
                for r in result["attempt_rows"]
                if r["receive_count"] == 1
            }

        short = first_attempt_failures(FixedPolicy(3.0))
        long = first_attempt_failures(FixedPolicy(30.0))
        self.assertEqual(short, long)
        self.assertTrue(any(failed for failed, _ in short.values()))

    def test_failure_draw_is_deterministic_per_attempt(self):
        model = FailureModel(0.5)
        self.assertEqual(model.sample_for(3, "job-1", 1), model.sample_for(3, "job-1", 1))
        draws = {model.sample_for(3, "job-1", k) for k in range(1, 30)}
        self.assertGreater(len(draws), 1)


class ConfidenceIntervalTests(unittest.TestCase):
    def test_t_critical_matches_reference_values(self):
        for df, expected in [(1, 12.7062), (4, 2.7764), (29, 2.0452), (1000, 1.9623)]:
            self.assertAlmostEqual(t_critical(0.95, df), expected, places=3)
        self.assertAlmostEqual(t_critical(0.99, 4), 4.6041, places=3)

    def test_aggregate_uses_t_interval_and_adds_bootstrap(self):
        runs = pd.DataFrame(
            {"scenario": "s", "policy_name": "p", "seed": [1, 2], "metric": [1.0, 3.0]}
        )
        row = aggregate_runs(runs).iloc[0]
        half = 12.7062 * math.sqrt(2.0) / math.sqrt(2)
        self.assertAlmostEqual(row["metric_ci95_low"], 2.0 - half, places=3)
        self.assertAlmostEqual(row["metric_ci95_high"], 2.0 + half, places=3)
        self.assertLessEqual(row["metric_boot_ci95_low"], row["metric_mean"])
        self.assertGreaterEqual(row["metric_boot_ci95_high"], row["metric_mean"])


class PredictionMetricTests(unittest.TestCase):
    def setUp(self):
        self.messages, self.shift = generate_workload(WorkloadSpec("sudden_shift", n_messages=60), 5, "pm")

    def run_policy(self, policy):
        result = SQSSimulator(self.messages, policy, FailureModel(0), workers=4, seed=1).run()
        return summarize_run(result, self.shift)

    def test_static_policy_has_no_prediction_metrics(self):
        summary = self.run_policy(FixedPolicy(30.0))
        self.assertTrue(math.isnan(summary["mean_absolute_prediction_error_s"]))
        self.assertTrue(math.isnan(summary["adaptation_time_s"]))

    def test_adaptive_policy_keeps_prediction_metrics(self):
        summary = self.run_policy(AdaptivePercentilePolicy(history_window=10))
        self.assertFalse(math.isnan(summary["mean_absolute_prediction_error_s"]))


if __name__ == "__main__":
    unittest.main()
