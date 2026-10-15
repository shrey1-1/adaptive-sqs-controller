# Experimental methodology

## Design

The independent variables are timeout policy, workload scenario, failure probability, safety margin, history-window size, adaptive percentile, cold-start threshold (`min_history`), and random seed. Dependent variables are duplicates, wasted work, end-to-end latency, throughput, failure recovery, prediction error, extension/API overhead, successful completion, and shift-adaptation time.

All policies run on the exact same generated message list for each scenario/seed. Failures use common random numbers: the outcome of the k-th delivery of a job is drawn from a generator keyed by `(seed, job_id, k)`, so it is identical under every policy. Policies still differ in how many deliveries they cause, so total failure counts can differ, but every shared attempt is paired.

## Baselines

- `fixed_per_class` is an **oracle**: class P90 + margin fitted to the evaluated workload itself. It is an upper bound on what static per-class tuning can achieve, not a realistic competitor.
- `pretuned_per_class` is the **realistic** static baseline: class P90 + margin fitted to the first `calibration_fraction` (default 20%) of an independent pilot workload drawn from the same scenario with a different seed. It never sees evaluated jobs, and under a later shift its timeouts go stale as a hand-tuned configuration would.
- `adaptive_p90_relative` uses a margin of 25% of the predicted P90 instead of a flat 2 s, so protection scales with each class's duration.

## Required experiment mapping

- Mostly short/no failures, mixed/no failures, heavy-tailed/no failures
- Mixed/medium failures and heavy-tailed/medium failures
- Sudden shift/low failures
- Margin sweep: 0, 1, 2, 4 seconds
- History windows: 10, 25, 50, 100 observations
- Percentile sweep: P75, P80, P90, P95, P99
- Cold-start sweep: `min_history` of 2, 5, 10, 20

`full.json` repeats all cases over twenty seeds. Report aggregates, but retain attempt-level data for auditability. Do not interpret NaN recovery metrics as zero: they mean no observed recoveries. Treat unobserved adaptation time the same way.

## Warm-up

Columns prefixed `steady_` repeat key metrics for jobs sent after the first `warmup_fraction` (default 20%) of jobs. The same jobs are excluded for every policy, so steady-state comparisons remain paired. The gap between the full and steady values estimates each policy's cold-start cost.

## Statistical analysis

- `aggregate_metrics.csv` gives mean, SD, Student-t 95% CI, and percentile-bootstrap 95% CI per scenario and policy, plus `pareto_optimal_dup_vs_recovery` for policies not dominated on (mean recovery, duplicate rate) within the scenario.
- `paired_comparisons.csv` compares every policy against the reference (`paired_reference`, default `adaptive_p90`) using per-seed differences: mean difference (reference − comparator) with a t interval, an exact Wilcoxon signed-rank p-value (normal approximation when ties occur), and a Holm-adjusted p-value across every test in the file.
- With twenty seeds the smallest attainable two-sided exact Wilcoxon p-value is 2/2²⁰, so Holm-adjusted significance is attainable; with two seeds it is not, which is why smoke runs are demonstrations only.

## Analysis rules

1. Compare adaptive P90 first with `pretuned_per_class`, then with the oracle and global policies.
2. Report effect sizes in original units and uncertainty, not only rankings; use paired tests, not overlapping CIs.
3. Report both full and steady-state values, and attribute differences to cold start where the gap shows it.
4. Explain stationary cases where the static class policy wins.
5. Separate simulator evidence from real AWS evidence.
6. Do not generalize beyond modeled assumptions.
