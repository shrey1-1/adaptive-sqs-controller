# Limitations and threats to validity

## Construct validity

Duplicate processing is defined as temporal overlap at attempt start. Another definition—any repeated work regardless of overlap—would produce different rates. Recovery begins at worker failure, not initial receive. API calls are logical per-message calls and do not represent batch pricing.

## Internal validity

Event ordering is deterministic. Failures use common random numbers keyed by `(seed, job_id, receive_count)`, so the k-th delivery of a job fails identically under every policy; policies still differ in how many deliveries they cause, so total failure counts can differ. Because the key omits the scenario, scenarios run with the same seed share failure draws for the same job IDs. `fixed_per_class` is an oracle fitted to the full generated dataset; `pretuned_per_class` is the realistic comparator, fitted to an independent pilot calibration window.

## External validity

No network latency, autoscaling delay, heterogeneous workers, correlated failures, in-flight quota, retention expiry, or SQS replicated-copy anomaly is modeled. Hung workers are modeled as alive-but-stalled processes whose heartbeats continue until the visibility budget is exhausted and which are restarted exactly when their lease expires; real supervisors may detect hangs earlier (liveness probes) or later. Crash and hang rates are independent per attempt. Results demonstrate mechanisms and comparative behavior under controlled assumptions; they are not AWS production benchmarks.

## Statistical conclusion validity

Twenty seeds (`full.json`) support paired tests at moderate power; smoke runs with two seeds are demonstrations only. Aggregates report Student-t intervals (`*_ci95_*`) and percentile-bootstrap intervals (`*_boot_ci95_*`); the t interval assumes approximate normality of seed means, and the bootstrap interval is crude with very few seeds. Inspect per-seed values and distributions. Prediction-error and adaptation-time metrics are reported only for policies that estimate duration.

## Phase 4 assumptions

The cost-aware policy learns the failure rate from worker lifecycle events (crash, hang restart, completion), which assumes the controller can observe worker health; SQS alone only exposes redeliveries, which do not distinguish a crash from a slow job. Its derivation charges only the overrun of a slow job, while `tradeoff_cost_s_per_job` charges the full duplicate attempt, so the cost ratio must be calibrated rather than read off directly. Conformal coverage holds only while recent durations are exchangeable; it fails after shifts and is slightly reduced by completion-order bias. With a 40-sample window, conformal levels above 40/41 cannot be met, so `adaptive_cost_conformal` falls back to the cold-start rule whenever failures are rare.

## Adaptive-policy failure modes

Cold-start fallback can be poor; rare classes learn slowly; small windows are noisy; large windows react slowly; the observed P90 can lag sudden changes; capped timeouts can under-cover extreme tails; large margins delay failure recovery; and a stable, well-tuned static class policy can be simpler and better.

