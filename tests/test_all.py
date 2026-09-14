import math
import unittest

from src.failure_model import FailureModel
from src.idempotency import InMemoryIdempotencyLedger
from src.message import Message
from src.policies import AdaptivePercentilePolicy, ClassBasedPolicy, FixedHeartbeatPolicy, FixedPolicy
from src.simulator import SQSSimulator
from src.workload import WorkloadSpec, generate_workload


def one_message(duration=5.0):
    return [Message("job-1", "short", duration, 0.0, "test")]


class ProjectTests(unittest.TestCase):
    def test_fixed_policy_calculation(self):
        self.assertEqual(FixedPolicy(7).decide("x", 0).timeout, 7)

    def test_class_based_policy_calculation(self):
        policy = ClassBasedPolicy({"short": 3}, default_timeout=9)
        self.assertEqual(policy.decide("short", 0).timeout, 3)
        self.assertTrue(policy.decide("unknown", 0).used_fallback)

    def test_adaptive_percentile_calculation(self):
        policy = AdaptivePercentilePolicy(percentile=90, safety_margin=2, history_window=10, min_history=5)
        for value in [1, 2, 3, 4, 10]:
            policy.observe("x", value, 0)
        decision = policy.decide("x", 0)
        self.assertTrue(math.isclose(decision.prediction, 7.6))
        self.assertTrue(math.isclose(decision.timeout, 9.6))

    def test_insufficient_history_fallback(self):
        policy = AdaptivePercentilePolicy(default_timeout=13, min_history=3)
        policy.observe("x", 2, 0)
        self.assertTrue(policy.decide("x", 0).used_fallback)

    def test_idempotent_result_handling(self):
        ledger = InMemoryIdempotencyLedger()
        self.assertTrue(ledger.commit_once("job-1"))
        self.assertFalse(ledger.commit_once("job-1"))

    def test_visibility_expiration_and_redelivery(self):
        result = SQSSimulator(one_message(5), FixedPolicy(2), FailureModel(0), workers=2, seed=1).run()
        self.assertGreaterEqual(result["api_operations"]["receive"], 2)
        self.assertTrue(any(r["duplicate_delivery"] for r in result["attempt_rows"]))
        self.assertTrue(any(r["duplicate_processing"] for r in result["attempt_rows"]))

    def test_successful_deletion(self):
        result = SQSSimulator(one_message(1), FixedPolicy(3), FailureModel(0), workers=2, seed=1).run()
        self.assertEqual(result["api_operations"]["receive"], 1)
        self.assertEqual(result["api_operations"]["delete"], 1)

    def test_worker_failure(self):
        result = SQSSimulator(one_message(1), FixedPolicy(2), FailureModel(0.5), workers=1, seed=2).run()
        self.assertTrue(any(r["worker_failure"] for r in result["attempt_rows"]))
        self.assertTrue(result["failure_recoveries"])

    def test_timeout_extension_and_maximum_limit(self):
        policy = FixedHeartbeatPolicy(2, heartbeat_interval=1, max_extensions=2, max_total_visibility=6)
        result = SQSSimulator(one_message(5), policy, FailureModel(0), workers=2, seed=1).run()
        self.assertEqual(result["api_operations"]["change_visibility"], 2)
        self.assertLessEqual(max(r["number_of_timeout_extensions"] for r in result["attempt_rows"]), 2)

    def test_duplicate_detection_and_single_commit(self):
        result = SQSSimulator(one_message(5), FixedPolicy(2), FailureModel(0), workers=3, seed=1).run()
        self.assertEqual(sum(r["committed_result"] for r in result["attempt_rows"]), 1)

    def test_reproducibility_with_fixed_seed(self):
        spec = WorkloadSpec("mixed", n_messages=25)
        first, _ = generate_workload(spec, 42, "a")
        second, _ = generate_workload(spec, 42, "a")
        self.assertEqual(first, second)

