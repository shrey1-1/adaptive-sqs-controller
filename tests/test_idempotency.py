from src.idempotency import InMemoryIdempotencyLedger


def test_idempotent_result_handling():
    ledger = InMemoryIdempotencyLedger()
    assert ledger.commit_once("job-1") is True
    assert ledger.commit_once("job-1") is False
    assert len(ledger) == 1

