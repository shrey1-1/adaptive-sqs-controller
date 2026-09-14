from src.failure_model import FailureModel
from src.message import Message
from src.policies import FixedHeartbeatPolicy, FixedPolicy
from src.simulator import SQSSimulator


def one_message(duration=5.0):
    return [Message("job-1", "short", duration, 0.0, "test")]


def test_visibility_expiration_and_redelivery_create_duplicate():
    result = SQSSimulator(one_message(5), FixedPolicy(2), FailureModel(0), workers=2, seed=1).run()
    assert result["api_operations"]["receive"] >= 2
    assert any(row["duplicate_delivery"] for row in result["attempt_rows"])
    assert any(row["duplicate_processing"] for row in result["attempt_rows"])


def test_successful_deletion_prevents_redelivery():
    result = SQSSimulator(one_message(1), FixedPolicy(3), FailureModel(0), workers=2, seed=1).run()
    assert result["api_operations"]["receive"] == 1
    assert result["api_operations"]["delete"] == 1
    assert result["deleted_jobs"] == 1


def test_worker_failure_causes_later_recovery():
    result = SQSSimulator(one_message(1), FixedPolicy(2), FailureModel(0.5), workers=1, seed=2).run()
    assert any(row["worker_failure"] for row in result["attempt_rows"])
    assert result["completed_jobs"] == 1
    assert result["failure_recoveries"]


def test_timeout_extension_and_maximum_limit():
    policy = FixedHeartbeatPolicy(2, heartbeat_interval=1, max_extensions=2, max_total_visibility=6)
    result = SQSSimulator(one_message(5), policy, FailureModel(0), workers=2, seed=1).run()
    assert result["api_operations"]["change_visibility"] == 2
    assert max(row["number_of_timeout_extensions"] for row in result["attempt_rows"]) <= 2


def test_duplicate_result_is_not_committed_twice():
    result = SQSSimulator(one_message(5), FixedPolicy(2), FailureModel(0), workers=3, seed=1).run()
    committed = [row for row in result["attempt_rows"] if row["committed_result"]]
    assert len(committed) == 1
    assert result["completed_jobs"] == 1

