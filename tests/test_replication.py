import json
import tempfile
import time
import unittest

from src.policies import FixedPolicy
from src.replication import (
    FakeSQSClient,
    IntegerTimeouts,
    InvalidReceiptHandle,
    ReplicationConfig,
    ReplicationRun,
    run_replication,
    scaled_workload,
)


class FakeClientTests(unittest.TestCase):
    def test_visibility_redelivery_and_receipt_rules(self):
        client = FakeSQSClient()
        client.send_message(QueueUrl="q", MessageBody="x")
        first = client.receive_message(QueueUrl="q", VisibilityTimeout=0.05)["Messages"][0]
        self.assertEqual(client.receive_message(QueueUrl="q", VisibilityTimeout=0.05), {})  # hidden
        time.sleep(0.08)
        second = client.receive_message(QueueUrl="q", VisibilityTimeout=5)["Messages"][0]
        self.assertEqual(second["Attributes"]["ApproximateReceiveCount"], "2")
        with self.assertRaises(InvalidReceiptHandle):
            client.change_message_visibility(QueueUrl="q", ReceiptHandle=first["ReceiptHandle"], VisibilityTimeout=10)
        client.delete_message(QueueUrl="q", ReceiptHandle=first["ReceiptHandle"])  # old handle still deletes
        self.assertEqual(client.get_queue_attributes(QueueUrl="q", AttributeNames=[])["Attributes"]["ApproximateNumberOfMessages"], "0")


class IntegerTimeoutTests(unittest.TestCase):
    def test_rounds_up_to_whole_seconds(self):
        self.assertEqual(IntegerTimeouts(FixedPolicy(2.1)).decide("a", 0).timeout, 3.0)
        self.assertEqual(IntegerTimeouts(FixedPolicy(0.2)).decide("a", 0).timeout, 1.0)
        self.assertEqual(IntegerTimeouts(FixedPolicy(4.0)).decide("a", 0).timeout, 4.0)


class SafetyTests(unittest.TestCase):
    def test_refuses_non_empty_queue(self):
        client = FakeSQSClient()
        client.send_message(QueueUrl="q", MessageBody="someone else's message")
        with self.assertRaises(RuntimeError):
            run_replication(client, "q", ReplicationConfig(n_jobs=2), ["fixed_short"], tempfile.mkdtemp())

    def test_foreign_message_is_released_untouched(self):
        client = FakeSQSClient()
        client.send_message(QueueUrl="q", MessageBody=json.dumps({"run_id": "other", "job_id": "x"}))
        config = ReplicationConfig(n_jobs=2, integer_timeouts=False, time_scale=0.05)
        run = ReplicationRun(client, "q", FixedPolicy(1.0), scaled_workload(config), config)
        raw = client.receive_message(QueueUrl="q", VisibilityTimeout=30)["Messages"][0]
        run._process("worker-00", raw)
        self.assertIn("Messages", client.receive_message(QueueUrl="q", VisibilityTimeout=30))  # visible again
        self.assertEqual(run.rows, [])


class EndToEndTests(unittest.TestCase):
    def test_fake_run_completes_and_compares_with_simulator(self):
        config = ReplicationConfig(n_jobs=12, workers=3, time_scale=0.05, integer_timeouts=False,
                                   poll_wait_seconds=0, failure_probability=0.1, max_seconds_per_policy=30)
        client = FakeSQSClient()
        with tempfile.TemporaryDirectory() as tmp:
            table = run_replication(client, "q", config, ["fixed_short", "adaptive_heartbeat_dynamic"], tmp)
        self.assertEqual(list(table.policy_name), ["fixed_short", "adaptive_heartbeat_dynamic"])
        self.assertTrue((table.real_completed_jobs == 12).all())
        self.assertTrue((table.simulated_completed_jobs == 12).all())
        self.assertFalse(table.real_timed_out.any())
        # A 0.15 s fixed timeout against 0.06-0.7 s jobs must produce duplicates in both worlds.
        short = table.set_index("policy_name").loc["fixed_short"]
        self.assertGreater(short.real_duplicate_processing_rate, 0)
        self.assertGreater(short.simulated_duplicate_processing_rate, 0)
        self.assertEqual(client.get_queue_attributes(QueueUrl="q", AttributeNames=[])["Attributes"]["ApproximateNumberOfMessages"], "0")



class ReportTests(unittest.TestCase):
    def test_report_page_with_figures(self):
        from src.replication_report import write_replication_report

        config = ReplicationConfig(n_jobs=8, workers=2, time_scale=0.05, integer_timeouts=False,
                                   poll_wait_seconds=0, max_seconds_per_policy=30)
        with tempfile.TemporaryDirectory() as tmp:
            run_replication(FakeSQSClient(), "q", config, ["fixed_short", "adaptive_heartbeat_dynamic"], tmp)
            report = write_replication_report(tmp)
            page = report.read_text(encoding="utf-8")
            import pandas as pd
            attempts = pd.read_csv(f"{tmp}/replication_attempts.csv")
            from pathlib import Path
            figures = sorted(p.name for p in Path(tmp, "plots").glob("*.png"))
        self.assertIn("Dry run against an in-memory fake queue", page)
        self.assertIn("07_attempt_timeline.png", page)
        self.assertEqual(len(figures), 7)
        self.assertEqual(set(attempts.policy_name), {"fixed_short", "adaptive_heartbeat_dynamic"})


if __name__ == "__main__":
    unittest.main()
