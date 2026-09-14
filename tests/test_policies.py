import math

from src.policies import AdaptivePercentilePolicy, ClassBasedPolicy, FixedPolicy


def test_fixed_policy_calculation():
    assert FixedPolicy(7).decide("anything", 0).timeout == 7


def test_class_based_policy_calculation_and_fallback():
    policy = ClassBasedPolicy({"short": 3, "long": 20}, default_timeout=9)
    assert policy.decide("long", 0).timeout == 20
    decision = policy.decide("unknown", 0)
    assert decision.timeout == 9 and decision.used_fallback


def test_adaptive_percentile_calculation():
    policy = AdaptivePercentilePolicy(
        percentile=90,
        safety_margin=2,
        min_timeout=1,
        max_timeout=100,
        default_timeout=12,
        history_window=10,
        min_history=5,
    )
    for value in [1, 2, 3, 4, 10]:
        policy.observe("x", value, 0)
    decision = policy.decide("x", 0)
    assert math.isclose(decision.prediction, 7.6)
    assert math.isclose(decision.timeout, 9.6)
    assert not decision.used_fallback


def test_insufficient_history_fallback():
    policy = AdaptivePercentilePolicy(default_timeout=13, min_history=3)
    policy.observe("x", 2, 0)
    decision = policy.decide("x", 0)
    assert decision.timeout == 13
    assert decision.used_fallback
