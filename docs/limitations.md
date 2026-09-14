# Limitations and threats to validity

## Construct validity

Duplicate processing is defined as temporal overlap at attempt start. Another definition—any repeated work regardless of overlap—would produce different rates. Recovery begins at worker failure, not initial receive. API calls are logical per-message calls and do not represent batch pricing.

## Internal validity

Event ordering is deterministic, but policy-dependent retry paths consume different failure random draws. The class baseline is an oracle fitted to the full generated dataset. This favors the baseline and should be disclosed, not hidden.

## External validity

No network latency, autoscaling delay, heterogeneous workers, correlated failures, in-flight quota, retention expiry, or SQS replicated-copy anomaly is modeled. Results demonstrate mechanisms and comparative behavior under controlled assumptions; they are not AWS production benchmarks.

## Statistical conclusion validity

Five seeds offer a student-scale estimate, not high-powered inference. Normal-approximation CIs can be weak for bounded or heavy-tailed metrics. Inspect per-seed values and distributions; use bootstrap intervals in a larger follow-up.

## Adaptive-policy failure modes

Cold-start fallback can be poor; rare classes learn slowly; small windows are noisy; large windows react slowly; the observed P90 can lag sudden changes; capped timeouts can under-cover extreme tails; large margins delay failure recovery; and a stable, well-tuned static class policy can be simpler and better.

