# Limitations and threats to validity

## Construct validity

Duplicate processing is defined as temporal overlap at attempt start. Another definition—any repeated work regardless of overlap—would produce different rates. Recovery begins at worker failure, not initial receive. API calls are logical per-message calls and do not represent batch pricing.

## Internal validity

Event ordering is deterministic. Failures use common random numbers keyed by `(seed, job_id, receive_count)`, so the k-th delivery of a job fails identically under every policy; policies still differ in how many deliveries they cause, so total failure counts can differ. Because the key omits the scenario, scenarios run with the same seed share failure draws for the same job IDs. The class baseline is an oracle fitted to the full generated dataset. This favors the baseline and should be disclosed, not hidden.

## External validity

No network latency, autoscaling delay, heterogeneous workers, correlated failures, in-flight quota, retention expiry, or SQS replicated-copy anomaly is modeled. Results demonstrate mechanisms and comparative behavior under controlled assumptions; they are not AWS production benchmarks.

## Statistical conclusion validity

Five seeds offer a student-scale estimate, not high-powered inference. Aggregates report Student-t intervals (`*_ci95_*`) and percentile-bootstrap intervals (`*_boot_ci95_*`); the t interval assumes approximate normality of seed means, and the bootstrap interval is crude with very few seeds. Inspect per-seed values and distributions. Prediction-error and adaptation-time metrics are reported only for policies that estimate duration.

## Adaptive-policy failure modes

Cold-start fallback can be poor; rare classes learn slowly; small windows are noisy; large windows react slowly; the observed P90 can lag sudden changes; capped timeouts can under-cover extreme tails; large margins delay failure recovery; and a stable, well-tuned static class policy can be simpler and better.

