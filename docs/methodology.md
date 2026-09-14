# Experimental methodology

## Design

The independent variables are timeout policy, workload scenario, failure probability, safety margin, history-window size, and random seed. Dependent variables are duplicates, wasted work, end-to-end latency, throughput, failure recovery, prediction error, extension/API overhead, successful completion, and shift-adaptation time.

Run all policies on the exact same generated message list for each scenario/seed. The failure stream is seeded identically per run. Because policies change redelivery counts, later random draws can diverge; results are reproducible but failure events are not perfectly paired after policies cause different paths. A future design can preassign failure outcomes by `(job_id, receive_count)`.

## Required experiment mapping

- Mostly short/no failures, mixed/no failures, heavy-tailed/no failures
- Mixed/medium failures and heavy-tailed/medium failures
- Sudden shift/low failures
- Margin sweep: 0, 1, 2, 4 seconds
- History windows: 10, 25, 50, 100 observations

`full.json` repeats all cases over five seeds. Report aggregates, but retain attempt-level data for auditability. Do not interpret NaN recovery metrics as zero: they mean no observed recoveries. Treat unobserved adaptation time the same way.

## Analysis rules

1. Compare adaptive P90 first with fixed-per-class, then with global policies.
2. Report effect sizes in original units and uncertainty, not only rankings.
3. Explain stationary cases where the static class policy wins.
4. Separate simulator evidence from real AWS evidence.
5. Do not generalize beyond modeled assumptions.

