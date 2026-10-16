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
- `paired_comparisons.csv` compares every policy against each reference (`paired_reference`, default `adaptive_p90` and `adaptive_heartbeat`) using per-seed differences: mean difference (reference − comparator) with a t interval, an exact Wilcoxon signed-rank p-value (normal approximation when ties occur), and a Holm-adjusted p-value across every exploratory test in the file.
- With twenty seeds the smallest attainable two-sided exact Wilcoxon p-value is 2/2²⁰, so Holm-adjusted significance is attainable; with two seeds it is not, which is why smoke runs are demonstrations only.

## Primary hypotheses

`configs/full.json` declares eighteen hypotheses (H1–H18), each a single paired test of one metric between two policies in one scenario, with an expected direction. `primary_tests` applies Holm correction within this family only and marks a hypothesis `supported` when the mean difference has the expected sign and the adjusted p-value is below 0.05. All other comparisons are exploratory and Holm-corrected separately across `paired_comparisons.csv`.

H1–H4 re-test findings from Phases 2 and 3 on the same seeds; they are labelled post hoc and are confirmatory only in the sense of surviving the stricter family correction. H5–H8 were committed to the repository (`bfab621`) before any evaluation-seed run of the code that tests them; H9–H11 were committed with their code (`e49c2ea`), H12–H16 with theirs (`71a1a81`) and H17–H18 with theirs (`39a0510`), each before the evaluation run that tests them.

## Tuning seeds

Design choices tuned on data use seeds 1001–1010, which never overlap the evaluation seeds (11–220). The partial-history cold-start factor (1.5), the Page–Hinkley threshold (3, from 1–4), its spread reference (0.25, from 0.25–0.5), the choice to re-read the heartbeat budget, and the rejection of the cost-aware heartbeat budget were all decided this way.

## Phase 4 metrics

- `prediction_coverage`: share of jobs whose first successful attempt finished within the predicted quantile (before the margin), excluding cold-start fallbacks. First attempts only, because a job that overran is redelivered and would otherwise be counted as uncovered once per retry.
- `tradeoff_cost_s_per_job`: seconds of duplicate-processing work plus seconds of failure-recovery delay, per completed job. It weights the two equally (cost ratio 1).

## Phase 4.3 metric

- `change_points_detected`: changes the policy's Page–Hinkley detector flagged in the run. On workloads without a real shift every detection is a false alarm.

## Phase 5 metrics and verification

- `false_dlq_rate`: healthy (non-poison) jobs moved to the dead-letter queue, as a share of healthy jobs. `dlq_but_completed`: of those, how many had already committed, so a DLQ replay would duplicate them.
- `poison_quarantine_time_s`: time from sending a poison message to its move to the DLQ.
- `healthy_completion_rate`: committed healthy jobs over healthy jobs.
- `tests/test_verification.py` checks the simulator against closed-form counts and the Erlang C formula; it must pass unchanged whenever the simulator changes.

## Hang sensitivity

Hang rates of 0%, 2%, 8% and 20% per attempt are run on the mixed workload, plus an 8% scenario with a liveness probe that kills a hung worker 10 s after it stops progressing.

## Analysis rules

1. Read `primary_hypotheses.csv` first; treat everything else as exploratory.
2. Compare adaptive P90 first with `pretuned_per_class`, then with the oracle and global policies.
3. Report effect sizes in original units and uncertainty, not only rankings; use paired tests, not overlapping CIs.
4. Report both full and steady-state values, and attribute differences to cold start where the gap shows it.
5. Explain stationary cases where the static class policy wins.
6. Separate simulator evidence from real AWS evidence.
7. Do not generalize beyond modeled assumptions.
