# Adaptive SQS Visibility Timeout Controller

An academic, reproducible study of whether a class-aware rolling-percentile visibility policy can reduce duplicate processing without making worker-failure recovery unacceptably slow. AWS use is **not** the novelty: the contribution is the controller, the event model, and an empirical trade-off analysis against strong static baselines.

## Abstract

Amazon SQS hides a received message for a visibility interval. An interval shorter than processing time permits concurrent redelivery; an interval longer than necessary delays recovery after a worker crash. This project implements a seeded discrete-event simulator and evaluates fixed-short, fixed-long, overall-mean, per-class, heartbeat, and adaptive policies. The adaptive controller estimates the recent class-specific 90th percentile and adds a bounded safety margin. Experiments cover heterogeneous and changing workloads plus controlled failures, reporting duplicates, latency, throughput, recovery, prediction error, API overhead, and adaptation time. An optional, deliberately small Boto3 adapter validates assumptions against a user-provided test queue.

## Research question and hypothesis

**Question:** Can a workload-aware adaptive visibility-timeout policy reduce duplicate message processing while maintaining fast recovery from worker failures compared with fixed SQS visibility-timeout policies?

**Hypothesis:** In non-stationary or heterogeneous workloads, a rolling class-specific P90 policy will move toward a better duplicate/recovery trade-off than globally fixed policies. A well-tuned static per-class policy may match or outperform it for stationary workloads; cold-start history and abrupt shifts may temporarily harm the adaptive policy.

## Objectives

1. Reproduce visibility expiry, redelivery, deletion, failures, receipt handles, and bounded extensions without real-time waiting.
2. Implement interpretable static and adaptive timeout policies.
3. Measure the duplicate-versus-recovery trade-off under repeatable workloads and failures.
4. Preserve logical correctness using an idempotency ledger while still recording duplicate work.
5. Provide optional, low-cost real-SQS validation without storing credentials.

## Controller and novelty

For job class `c`, the controller keeps the latest `W` successful durations. Once at least `H` observations exist:

```text
prediction(c) = percentile(history[c], 90)
timeout(c) = min(T_max, max(T_min, prediction(c) + safety_margin))
```

With no samples it uses a configured default; with 1 to `H − 1` samples it uses 1.5 × the largest sample + safety margin (the partial-history cold start). Optional extensions occur only while the original worker is healthy and are bounded by both a maximum count and a maximum total visibility horizon. The novelty is not queue automation; it is the interpretable, workload-aware control policy and its reproducible comparison with an optimistic static per-class baseline.

## Architecture

```text
Seeded workload -> simulated visible queue -> workers -> attempts/results
                         |                     |          |
                         v                     v          v
                 timeout policy        failure model  metrics CSV
                         ^                                |
                         | successful durations           v
                   rolling history                  plots/summary
```

`SQSSimulator` is a priority-queue event loop. Send, completion, failure, visibility-expiry, and heartbeat events advance simulated time. When a lease expires, the message becomes visible even if its first worker is still running. A new receipt handle is created on every receive. To keep experiments finite, the first successful active attempt may delete the message even when a newer delivery exists; real SQS deletion with an old handle can succeed without guaranteeing removal.

## Policies compared

- `fixed_short`: deliberately exposes duplicate work.
- `fixed_long`: suppresses most timeouts but can delay failure recovery.
- `fixed_overall_mean`: a global workload mean.
- `fixed_per_class`: static class P90 plus margin, fitted to the evaluated workload itself; an optimistic **oracle** and upper bound for static tuning.
- `pretuned_per_class`: the **realistic** static baseline, fitted to the first 20% of an independent pilot workload, so it never sees evaluated jobs and goes stale after a shift.
- `adaptive_p90`: rolling class-specific P90 plus a 2 s margin with cold-start fallback.
- `adaptive_p90_relative`: the same controller with a margin of 25% of the predicted P90, so protection scales with class duration.
- `fixed_heartbeat`: a fixed 5 s lease renewed every 3.5 s, at most 4 extensions (40 s cap).
- `adaptive_heartbeat`: per-class lease min(5 s, P90 + margin), heartbeat at 70% of the lease, and a per-class visibility budget of P99 × 1.25 + 2 s (capped at 40 s). Short classes finish inside their first lease; long classes renew; a hung worker is reclaimed after about its class's worst normal duration.
- `adaptive_heartbeat_fixed_budget`: ablation with the adaptive lease but the global 40 s budget.
- `adaptive_cost_aware` (Phase 4): chooses the quantile from costs and the observed failure rate, q* = 1 − p·r/(1 − p), with r = recovery cost / duplicate cost = 1, clamped to [0.5, 0.995].
- `adaptive_conformal` (Phase 4): the ⌈(n+1)·0.9⌉-th order statistic, which covers the next duration with probability ≥ 0.9 when recent durations are exchangeable.
- `adaptive_cost_conformal` (Phase 4): both.
- `adaptive_p90_fixed_fallback`: ablation with the old cold-start rule (12 s until 5 samples).
- `adaptive_p90_cpd` (Phase 4.3): adaptive P90 plus a per-class Page–Hinkley change detector on log durations (threshold 3); on a detected change the class history is cut to its last 3 observations.
- `adaptive_heartbeat_dynamic` (Phase 4.3): adaptive heartbeat whose visibility budget is re-read at every heartbeat instead of being fixed at receive time.
- `adaptive_heartbeat_cpd_dynamic` (Phase 4.3): both.

All adaptive policies now use a **partial-history cold start**: with 1–4 samples of a class, the timeout is 1.5 × the largest sample + margin, instead of a fixed 12 s.

## Workloads and failures

The generator supports mostly-short, mixed short/medium/long, Pareto heavy-tailed, bursty, gradual-shift, and sudden-shift workloads. NumPy's seeded generator controls interarrival times, job types, durations, and failures. Failure levels are `none=0`, `low=0.02`, `medium=0.08`, and `high=0.20` per attempt. A failed attempt consumes work up to a sampled point, never deletes, and never heartbeats.

Every message contains `job_id`, `job_type`, `processing_duration`, `creation_time`, and `experiment_id`; receive count is queue-owned state added to each attempt record.

## Metric semantics

- **Duplicate delivery:** any receive after the first for a job.
- **Duplicate processing:** an attempt begins while another attempt for that job is still active.
- **Duplicate committed result:** a successful attempt reaches the ledger after another attempt already committed. The ledger rejects it, so the logical result is not applied twice.
- **Wasted time:** failed-attempt work plus successful attempt time marked as duplicate processing.
- **Recovery time:** worker-failure timestamp to the next receive of that job.
- **Adaptation time:** after a known shift, time until five consecutive adaptive predictions fall within 20% (minimum 0.5 s) of the observed post-shift P90. This operational definition is intentionally reported as missing if convergence is not observed.
- **Queue operations:** simulated send, receive, delete, and change-visibility calls per committed job.

Run-level output also includes mean/P95 end-to-end latency, throughput, mean/P95 failure recovery, mean absolute prediction error, extensions per completed job, and successful completion rate. Aggregates contain mean, sample standard deviation, and normal-approximation 95% confidence intervals across seeds.

## Quick start

Python 3.10+ is recommended.

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m src.experiment --config configs/smoke.json --output results/smoke --plots
```

Use `configs/full.json` for the larger repeated experiment. Configuration stays outside policy logic. CSV files and `report.html` are written to the selected output folder and PNG figures to its `plots/` child. `full.json` runs 20 seeds plus margin, window, percentile, and `min_history` sweeps (about 1,700 simulations); its output in `results/full/` is not committed.

## Google Colab

Open `Adaptive_SQS_Controller_Colab.ipynb`. It installs packages, clones a repository URL or accepts an uploaded project, exposes editable configuration, runs tests, executes a small demonstration, displays tables/plots, provides a separate full run, and optionally exports to Google Drive. The simulator uses no Docker and no real-time sleeps.

## Optional AWS validation

Simulation requires no credentials. For a small real validation, first create a dedicated Standard queue manually, then set `ADAPTIVE_SQS_TEST_QUEUE_URL` and `AWS_REGION`. Let Boto3 use its standard credential chain or map values from Colab Secrets at runtime. Never put credentials in notebook cells, source files, CSV files, or Git.

Use [docs/iam-policy.json](docs/iam-policy.json) after replacing the account, region, and queue placeholders. It grants only Send/Receive/Delete/ChangeVisibility/GetAttributes on one test queue. Delete or purge the test queue in the AWS console after validation. The adapter never creates a queue and thus cannot create a chargeable resource silently.

## Outputs

- `attempt_metrics.csv`: one record per processing attempt, including required timing and outcome fields.
- `run_metrics.csv`: one row per scenario/policy/seed.
- `aggregate_metrics.csv`: repeated-seed means, SDs, Student-t and bootstrap 95% CIs, and a Pareto-optimality flag.
- `primary_hypotheses.csv`: the declared hypotheses H1–H18, each one paired test with Holm correction within that family only, and a `supported` flag.
- `paired_comparisons.csv`: exploratory per-seed paired differences against `adaptive_p90` and `adaptive_heartbeat`, Holm-corrected across the whole file.
- `report.html`: readable summary of the tables and figures.
- `plots/*.png`: the ten original figures plus `11_pareto_front.png` and `12_cold_start_vs_steady.png`.

Columns prefixed `steady_` repeat key metrics after excluding the first 20% of jobs (warm-up), separating cold-start cost from steady-state behaviour. See [docs/methodology.md](docs/methodology.md).

Values in `results/smoke/` and `results/full/` are simulation findings. Real Amazon SQS results are kept separately in `results/aws_replication/` (Phase 6.1).

### Current headline results (final code)

The phase sections below record what was measured at each phase; later fixes (partial cold start, heartbeat-before-crash, dynamic budget) moved some numbers. These are the values from the final code (`results/full/aggregate_metrics.csv`, 20 seeds, margin 2 s, window 40):

| Scenario | Policy | Duplicates (all) | Duplicates (after warm-up) | Mean recovery (s) | Mean latency (s) |
|---|---|---:|---:|---:|---:|
| Mixed, medium failure | Adaptive P90 | 0.031 | 0.018 | 6.37 | 6.24 |
| Mixed, medium failure | Pre-tuned per class | 0.024 | 0.023 | 6.34 | 6.15 |
| Mixed, medium failure | Oracle per class | 0.014 | 0.014 | 6.46 | 6.06 |
| Mixed, medium failure | Fixed heartbeat | 0.027 | 0.027 | 3.88 | 5.98 |
| Mixed, medium failure | Adaptive heartbeat (dynamic) | 0.002 | 0.002 | 3.17 | 5.70 |
| Sudden shift, low failure | Adaptive P90 | 0.059 | 0.066 | 8.53 | 16.52 |
| Sudden shift, low failure | Pre-tuned per class | 0.206 | 0.242 | 5.90 | 25.69 |
| Sudden shift, low failure | Oracle per class | 0.027 | 0.033 | 11.60 | 13.65 |
| Sudden shift, low failure | Fixed heartbeat | 0.003 | 0.004 | 3.51 | 12.18 |
| Sudden shift, low failure | Adaptive heartbeat (dynamic) | 0.004 | 0.005 | 3.58 | 12.34 |

All 18 primary hypotheses are supported (`results/full/primary_hypotheses.csv`).

### Recorded smoke findings

The included smoke run (`configs/smoke.json`) used 50 jobs, 6 workers, 4 scenarios, 2 seeds and 19 policies (152 runs; 8,623 attempt rows), regenerated with the final code. It is a functional demonstration, not a sufficiently powered final study. Selected two-seed means for the most important comparison are:

| Scenario | Policy | Duplicate processing | Mean recovery (s) | Mean latency (s) | Throughput (jobs/s) |
|---|---|---:|---:|---:|---:|
| Mixed, no failure | Adaptive P90 | 0.038 | n/a | 6.268 | 0.865 |
| Mixed, no failure | Fixed per class | 0.000 | n/a | 4.674 | 0.910 |
| Mixed, medium failure | Adaptive P90 | 0.037 | 5.440* | 5.925 | 0.866 |
| Mixed, medium failure | Fixed per class | 0.000 | 7.289* | 4.962 | 0.910 |
| Sudden shift, low failure | Adaptive P90 | 0.098 | n/a† | 13.495 | 0.531 |
| Sudden shift, low failure | Fixed per class | 0.048 | n/a† | 11.681 | 0.552 |

\* Single seed: seed 22 sampled no recoverable worker failures. † No worker failures were sampled at the 2% level in either seed.

These results do **not** support a blanket claim that adaptation wins. The optimistic static class baseline had fewer duplicates and lower latency in this small run. Most adaptive duplicates in the stationary scenario occur during cold start, before five observations of each class exist. With 50 jobs and 2 seeds, recovery-time comparisons are not estimable; run `full.json` (or larger) before drawing academic conclusions.

### Full-run findings (20 seeds)

*Recorded in Phase 2, before later fixes; for final-code values see "Current headline results" above.*

`configs/full.json` (200 jobs, 10 workers, 20 seeds; output in `results/full/`, not committed). Differences are paired per seed; *p* values are exact Wilcoxon, Holm-adjusted across all ~2,000 tests in `paired_comparisons.csv`.

| Scenario | Policy | Duplicates (all) | Duplicates (after warm-up) | Mean recovery (s) | Mean latency (s) |
|---|---|---:|---:|---:|---:|
| Mixed, medium failure | Adaptive P90 | 0.040 | 0.021 | 6.45 | 6.45 |
| Mixed, medium failure | Pre-tuned per class | 0.024 | 0.023 | 6.34 | 6.15 |
| Mixed, medium failure | Oracle per class | 0.014 | 0.014 | 6.46 | 6.06 |
| Mixed, medium failure | Fixed heartbeat | 0.039 | 0.039 | 3.90 | 5.93 |
| Sudden shift, low failure | Adaptive P90 | 0.057 | 0.066 | 8.53 | 16.51 |
| Sudden shift, low failure | Pre-tuned per class | 0.206 | 0.242 | 5.90 | 25.69 |
| Sudden shift, low failure | Oracle per class | 0.027 | 0.033 | 11.60 | 13.65 |
| Sudden shift, low failure | Fixed heartbeat | 0.006 | 0.007 | 2.38 | 12.16 |

1. **Under a workload shift, adaptation clearly beats realistic static tuning.** Against `pretuned_per_class`, adaptive P90 cut duplicate processing from 20.6% to 5.7% and mean latency by 9.2 s, better in all 20 seeds (*p*<0.001). The stale static timeouts recovered failures 2.6 s faster only because they were too short.
2. **In stationary workloads, the adaptive gap is cold start.** Over all jobs, adaptive P90 had more duplicates than the pre-tuned baseline (4.0% vs 2.4% in mixed/medium). After the 20% warm-up the two are indistinguishable (2.1% vs 2.3%, difference not significant). Lowering `min_history` from 5 to 2 reduced overall duplicates to 3.6%; raising it to 20 increased them to 7.7%.
3. **Against the oracle, adaptation trades duplicates for recovery after a shift.** It recovered 3.1 s faster (15 of 18 seeds with failures, *p*=0.02) but had 3 percentage points more duplicates.
4. **The heartbeat baseline is the strongest competitor.** `fixed_heartbeat` is Pareto-optimal in every scenario with failures. It beats adaptive P90 on duplicates, recovery and latency for heavy-tailed and shifting workloads. The one exception: in mixed/medium after warm-up, adaptive has fewer duplicates (2.1% vs 3.9%, *p*=0.04).
5. **Heavy tails defeat any per-class P90.** Adaptive, pre-tuned and oracle policies all land near 8–9% duplicates, because the 10% of jobs above P90 are exactly the ones that overrun.
6. **Sensitivity sweeps behave as expected.** Raising the percentile (P75 to P99) or the margin (0 to 4 s) traces a smooth curve with fewer duplicates and slower recovery. After a shift, windows of 10–25 observations beat 100. The 25% relative margin was no better than the 2 s absolute margin, and was worse for mostly-short jobs (1.6% vs 0.2%).

These results motivated Phase 3 below and Phase 4 (cost-aware percentile selection and change-point detection).

### Phase 3: adaptive heartbeat and hung workers (20 seeds)

*Recorded in Phase 3; final-code values differ slightly (see `results/full/`).*

Phase 3 added **hung workers**: an attempt stops progressing but its heartbeat thread stays alive, so it keeps extending the lease until the policy's visibility budget runs out, after which the worker is restarted. Without hangs, an unlimited heartbeat budget is free in simulation; hangs are what make the budget a real trade-off. Phase 3 also fixed a bias: attempts destined to crash previously never heartbeated, which understated heartbeat policies' crash-recovery time (fixed heartbeat, sudden shift: 2.4 s before the fix, 3.5 s after).

| Scenario | Policy | Duplicates | Mean recovery (s) | Hang recovery (s) | Mean latency (s) |
|---|---|---:|---:|---:|---:|
| Mixed, medium crashes | Adaptive heartbeat | 0.002 | 3.21 | n/a | 5.75 |
| Mixed, medium crashes | Fixed heartbeat | 0.027 | 3.88 | n/a | 5.98 |
| Mixed, medium crashes | Adaptive P90 | 0.040 | 6.45 | n/a | 6.45 |
| Mixed, medium hangs | Adaptive heartbeat | 0.002 | 9.47 | 10.89 | 6.95 |
| Mixed, medium hangs | Fixed heartbeat | 0.023 | 14.13 | 16.33 | 8.93 |
| Mixed, medium hangs | Adaptive lease, fixed budget | 0.000 | 30.91 | 36.88 | 15.29 |
| Heavy-tailed, medium hangs | Adaptive heartbeat | 0.022 | 17.67 | 20.80 | 13.74 |
| Heavy-tailed, medium hangs | Fixed heartbeat | 0.021 | 14.16 | 16.46 | 12.52 |

1. **On mixed workloads, adaptive heartbeat beats fixed heartbeat on every axis.** With crashes it had 2.5 percentage points fewer duplicates, recovered 0.7 s faster and had 0.2 s lower latency (19–20 of 20 seeds, Holm *p*≤0.003 for recovery and latency). With hangs it had 2.2 points fewer duplicates, recovered 4.7 s faster and had 2.0 s lower latency, better in all 20 seeds (Holm *p*=0.002). It is the first adaptive policy to beat the heartbeat baseline.
2. **The adaptive budget is what handles hangs.** With the global 40 s budget, hung jobs took 37 s to recover. The per-class budget cut that by 18–21 s on mixed and shifting workloads (all 20 seeds, *p*=0.002), at a cost of 0.2–0.7 points more duplicates.
3. **Heavy tails remain the weak spot.** The P99 budget of a Pareto class is long, so hang recovery was 3.5 s slower than fixed heartbeat (2 of 20 seeds better; not significant after Holm). After a sudden shift, adaptive heartbeat was within noise of fixed heartbeat on recovery but 0.7 s slower on latency.
4. **Without hangs, the fixed-budget ablation is best.** It has near-zero duplicates and the same crash recovery. In simulation the budget costs nothing until workers can hang.
5. **The budget factor traces a clean trade-off.** On heavy-tailed workloads with hangs, raising it from 1.0 to 2.0 cut duplicates from 3.1% to 0.8% but raised hang recovery from 17.4 s to 29.1 s.

Heartbeat policies pay about one extra queue API call per job (≈4.1–5.2 versus ≈3.1 calls per completed job).

### Fixes before Phase 4

- **Primary hypotheses.** Eight hypotheses (H1–H8; H9–H11 were added in Phase 4.3) are declared in `configs/full.json` and tested as one family (`primary_hypotheses.csv`); everything else is exploratory. H1–H4 re-test Phase 2/3 findings and are labelled post hoc. H5–H8 were committed (`bfab621`) before any evaluation-seed run that could test them.
- **Cold start.** The partial-history rule was chosen on tuning seeds 1001–1010, which do not overlap the evaluation seeds. On the evaluation seeds it cut duplicates in mixed/no-failure from 4.3% to 3.4% (H5, 20 seeds, Holm *p* = 0.002) and was neutral on heavy-tailed and shifting workloads.
- **Hang sensitivity.** Adaptive heartbeat beat fixed heartbeat on duplicates, recovery and latency at every hang rate tested (0%, 2%, 8%, 20%) and with a 10 s liveness probe, in 15–20 of 20 seeds on each metric. The adaptive budget beats the fixed 40 s budget once hangs exist: at 0% hangs the fixed budget has the lower trade-off cost (0.05 vs 0.12 s/job), at 2% the adaptive budget does (0.35 vs 0.82 s/job).

### Phase 4: cost-aware and conformal timeouts (20 seeds)

All eight hypotheses were supported. Six went the expected way in all 20 seeds; H5 and H8 in 13 seeds, with the other 7 exactly tied and none reversed. With the family since grown to eleven, every Holm-adjusted *p* for H1–H8 is ≤ 0.005.

| ID | Claim | Mean difference |
|---|---|---:|
| H1 | Shift: adaptive P90 has fewer duplicates than pre-tuned per class | −14.7 points |
| H2 | Mixed with hangs: adaptive heartbeat recovers faster than fixed heartbeat | −4.3 s |
| H3 | Mixed with hangs: adaptive heartbeat has fewer duplicates | −2.2 points |
| H4 | Mixed with crashes: adaptive heartbeat has fewer duplicates | −2.5 points |
| H5 | Partial cold start has fewer duplicates than the fixed fallback | −0.95 points |
| H6 | Heavy-tailed, no failures: cost-aware has fewer duplicates than P90 | −5.4 points |
| H7 | Shift (2% crashes): cost-aware has a lower trade-off cost than P90 | −0.33 s/job |
| H8 | Mixed with crashes: conformal has fewer duplicates than P90 | −0.6 points |

1. **Cost-aware selection explains when P90 is right.** With equal costs, q* = 1 − p/(1 − p). At 8% crashes that is 0.91, almost exactly P90, so cost-aware and P90 behave alike there (heavy-tailed with failures: 8.0% vs 8.8% duplicates). When crashes are rare the quantile rises: with no failures, heavy-tailed duplicates fell from 9.4% to 4.0%. The heavy-tail "weakness" at 8% crashes is the cost of fast recovery, not a defect.
2. **Conformal timeouts deliver their coverage.** Per job, the conformal quantile covered 90.8% of mixed and 89.1% of heavy-tailed first attempts, against 86–87% for interpolated P90. The small shortfall on heavy tails matches a completion-order bias (finished jobs over-represent short ones), measured offline at 0.9 points. After a shift coverage fell to 82.6%, as expected when recent durations stop being exchangeable.
3. **The cost model needs calibration.** In the cost-ratio sweep, r = 0.25 had a lower measured trade-off cost than r = 1 (2.70 vs 3.21 s/job). The derivation charges only the overrun of a slow job, while the metric charges the full duplicate attempt, so r must be calibrated to the real cost of a duplicate.
4. **Heartbeat policies still win overall.** Adaptive heartbeat has the lowest trade-off cost in most scenarios (e.g. 0.33 s/job vs 1.06–1.17 for the Phase 4 policies on mixed with crashes). Phase 4's ideas are applied to the lease-only controller so far; applying them to the heartbeat budget is the obvious next step.
5. **Known degeneracy.** With a 40-sample window, conformal ranks above 40/41 ≈ 0.976 do not exist. When the cost-aware level exceeds that (rare failures), `adaptive_cost_conformal` always falls back to the cold-start rule, so its results in no-failure and low-failure scenarios reflect that rule, not conformal prediction.

### Phase 4.3: reacting to workload shifts (20 seeds)

Two mechanisms target the shift weakness. Their settings were chosen on tuning seeds 1001–1010, and H9–H11 were committed together with the code (`e49c2ea`) before the evaluation run.

- **Change detection.** A two-sided Page–Hinkley test runs per class on log durations. It accumulates each observation's deviation from the running mean, minus a drift allowance of 0.05; when the accumulated rise or fall exceeds 3, the class history is cut to its last 3 observations, so the window refills with post-change durations instead of waiting for 40 completions.
- **Dynamic heartbeat budget.** The budget is re-read at every heartbeat. Jobs already in flight when durations grow get the larger budget at once; hung jobs received during cold start get the tighter learned budget once history exists. Workers keep heartbeating after reaching a dynamic budget, since it may still grow.

| ID | Claim | Mean difference | Seeds better / tied / worse | Holm *p* |
|---|---|---:|---|---:|
| H9 | Shift: change detection lowers adaptive P90's duplicates | −1.0 points (5.9% → 4.9%) | 17 / 3 / 0 | 0.0001 |
| H10 | Shift: dynamic budget lowers adaptive heartbeat's duplicates | −0.5 points (0.9% → 0.4%) | 12 / 8 / 0 | 0.005 |
| H11 | Mixed with hangs: dynamic budget speeds hang recovery | −1.3 s (11.3 → 10.1 s) | 15 / 0 / 5 | 0.012 |

1. **The heartbeat shift tie is closed.** After a shift, the dynamic budget brings adaptive heartbeat to 0.4% duplicates and 12.3 s latency against fixed heartbeat's 0.3% and 12.2 s. With hangs after a shift it now beats fixed heartbeat (exploratory): hang recovery 13.6 vs 15.2 s, latency 17.4 vs 18.1 s, trade-off cost 1.27 vs 1.44 s/job.
2. **Change detection helps the lease-only controller, not the heartbeat one.** Adaptive P90 gains 1.1 s of latency after a shift; adding it to the dynamic heartbeat changes almost nothing.
3. **False alarms cost something on heavy tails.** The detector fires about 0.1–0.15 times per run on mixed workloads but about 3.5 times on Pareto workloads, where it slightly raises duplicates for the heartbeat variant (2.4–2.7% vs 2.2–2.5%) and slows P90's recovery by 0.8–1.8 s.
4. **Heavy-tailed hangs remain the open gap.** Fixed heartbeat still recovers hung heavy-tailed jobs faster (16.5 s vs 20.3 s for the dynamic budget, 18.8 s with change detection).

### Phase 5: realism (20 seeds)

The simulator was first pinned by seven verification tests (closed-form redelivery and API counts, crash recovery, heartbeat counts, and the Erlang C mean wait of an M/M/c queue). Then optional realism was added, all off by default; the smoke results regenerate byte for byte with the defaults.

| Feature | Setting in the realistic scenarios |
|---|---|
| Dead-letter queue | `maxReceiveCount` = 5 (2, 3, 5, 10 in the DLQ sweep) |
| Poison messages | 3% of jobs fail on every attempt (`realistic_poison`) |
| Retry jitter | each attempt's duration × a mean-one log-normal factor, σ = 0.2 |
| Worker speeds | fixed mean-one log-normal factor per worker, σ = 0.25 |
| API latency | 50 ms before a receive starts, a delete lands or an extension applies |
| Batched receives | up to 10 messages per call (counted per dispatch round) |
| Correlated outages | about one per 100 s, crashing 3 of 10 workers at once |

H12–H16 were committed with the code (`71a1a81`) before the evaluation run. All five held in all 20 seeds.

| ID | Claim | Mean difference |
|---|---|---:|
| H12 | `maxReceiveCount` = 3: adaptive P90 dead-letters fewer healthy jobs than the overall-mean timeout | −4.6 points (4.6% → 0.0%) |
| H13 | Realistic shift: adaptive P90 has fewer duplicates than pre-tuned per class (H1 survives) | −10.4 points |
| H14 | Realistic hangs: dynamic heartbeat recovers hung jobs faster than fixed heartbeat (H11 survives) | −5.3 s |
| H15 | Realistic hangs: dynamic heartbeat has fewer duplicates than fixed heartbeat (H3 survives) | −2.6 points |
| H16 | Realistic poison: dynamic heartbeat quarantines poison faster than fixed heartbeat | −3.6 s (29.8 → 26.2 s) |

1. **Short timeouts lose healthy jobs to the DLQ.** At `maxReceiveCount` = 2, `fixed_short` dead-lettered 23% of healthy jobs and `fixed_overall_mean` 15%; adaptive and heartbeat policies stayed at or below 0.2%. Every wrongly dead-lettered job had already been processed, so replaying the DLQ would duplicate it. At 10 receives no policy lost a job.
2. **The headline results survive realism.** Adaptive heartbeat with a dynamic budget has the lowest trade-off cost in the realistic mixed, hang, poison and shift scenarios (e.g. 1.08 vs 2.12 s/job for fixed heartbeat with hangs), and after a realistic shift it now beats fixed heartbeat on duplicates (0.7% vs 1.5%).
3. **Poison quarantine depends on lease length.** Five redeliveries take 26 s under the adaptive heartbeat, 30 s under the fixed heartbeat, about 40 s under lease-only adaptive policies and 150 s under `fixed_long`.
4. **Heavy-tailed hangs remain the exception.** In the realistic heavy-tailed hang scenario, fixed heartbeat still has the lower trade-off cost (2.31 vs 2.54 s/job).

### Final improvements

- **Cost-aware heartbeat budget (rejected).** Choosing the budget quantile from the hang rate (q* = 1 − h·r/(1 − h)) shortens budgets and trades hang recovery for duplicates. On tuning seeds no ratio beat fixed heartbeat's trade-off cost on heavy-tailed hangs (best 2.58 vs 2.54 s/job), and every setting raised duplicates elsewhere. It is kept as the exploratory variant `adaptive_heartbeat_cost_budget`. Conclusion: for Pareto durations, the remaining gap is a trade-off frontier, not a fixable defect.
- **Spread-scaled change detection (adopted as `adaptive_p90_cpd_scaled`).** The Page–Hinkley threshold is multiplied by max(1, spread / 0.25), where spread is the class's standard deviation of log durations. On tuning seeds this cut heavy-tailed false alarms from 2.5 to 0.6 per run with the full post-shift benefit kept.

On the evaluation seeds both final hypotheses were supported, bringing the declared family to **18 of 18**:

| ID | Claim | Mean difference | Seeds better / tied / worse | Holm *p* |
|---|---|---:|---|---:|
| H17 | Heavy-tailed: scaled detection raises fewer false alarms than unscaled | −2.85 per run (3.65 → 0.80) | 19 / 1 / 0 | 0.0008 |
| H18 | Shift: scaled detection still lowers adaptive P90's duplicates | −0.6 points (5.9% → 5.2%) | 13 / 5 / 2 | 0.0012 |

The scaled detector keeps most, not all, of the shift benefit (5.2% against 4.9% unscaled) and recovers part of the heavy-tail recovery cost (8.5 s against 9.1 s with failures). The rejected cost-aware heartbeat budget confirmed its rejection on evaluation seeds: on heavy-tailed hangs its trade-off cost was 3.10 s/job against 2.50 for fixed heartbeat.

### Phase 6.1: real-SQS replication

`src/replication.py` runs the same seeded jobs and policies through a real SQS queue (worker threads, `sleep` as work, heartbeats, crashes from the same random draws) and through the simulator, and writes a real-versus-simulated comparison. It never creates or deletes queues, refuses a non-empty queue, only touches messages tagged with its run id, and cleans up after itself. An in-memory fake client that follows SQS visibility rules lets it run offline. The checklist, safety notes and cost estimate are in [docs/aws_replication.md](docs/aws_replication.md).

**Real run (22 Oct 2026, AWS CloudShell, Standard queue):** 40 mixed jobs, 4 workers, seed 11, 8% crashes, one run per policy. Results and figures are in `results/aws_replication/` (`"mode": "aws"`).

| Policy | Duplicates real / sim | Attempts real / sim | Extensions real / sim | Crashes | Recovery real / sim (s) | Latency real / sim (s) |
|---|---:|---:|---:|---:|---:|---:|
| fixed_short | 20.0% / 28.6% | 50 / 56 | 0 / 0 | 3 | 7.96 / 2.28 | 27.0 / 44.7 |
| fixed_heartbeat | 0% / 0% | 41 / 41 | 28 / 28 | 1 | 3.55 / 4.12 | 24.7 / 20.3 |
| adaptive_p90 | 2.4% / 4.7% | 42 / 43 | 0 / 0 | 1 | 5.49 / 19.46 | 25.0 / 24.5 |
| adaptive_heartbeat_dynamic | 0% / 0% | 41 / 41 | 28 / 28 | 1 | 9.77 / 4.12 | 23.8 / 20.3 |

1. **Mechanics agree.** Attempt and heartbeat-extension counts match almost exactly, and the duplicate ranking is preserved (fixed_short > adaptive_p90 > heartbeat policies = 0).
2. **Real SQS produced fewer duplicates than simulated**, and heartbeat runs were 3.5–4.4 s slower, consistent with real API and polling latency the simulator does not charge. These causes were not measured separately.
3. **Recovery times are within noise.** Three policies had a single crash; the two heartbeat policies are identical in simulation yet recovered in 3.55 s and 9.77 s.
4. On real SQS, fixed_short finished 7 jobs a second time; the idempotency ledger rejected those results.

One run per policy validates mechanics, not performance; repeating it several times would be needed for claims about recovery and latency.

## Limitations

The simulator models one Standard queue with instantaneous worker replacement and first-success deletion after concurrent redelivery. Phase 5 added optional constant API latency, batched receives, retry jitter, heterogeneous workers, correlated outages and a dead-letter queue, but latency is a constant rather than a distribution, and SQS's rare replicated-copy duplicates, message-retention expiry, in-flight quotas, autoscaling delay and long-poll billing are not modelled. Workloads are synthetic. The per-class oracle uses the entire generated dataset and is therefore stronger than a realistically pre-tuned baseline. Twenty seeds give moderate statistical power. The real-SQS evidence is one 40-job run per policy. See [docs/limitations.md](docs/limitations.md).

## Future extensions

Compare rolling P90 with EWMA or streaming quantile sketches; estimate uncertainty explicitly; tune margins from a declared loss function; model latency as a distribution and SQS replicated-copy duplicates; evaluate on real job-duration traces; repeat the real-SQS replication several times per policy; and add a related-work comparison (TCP retransmission timeouts, leases, failure detectors, speculative execution).

## Suggested report chapters

1. Introduction and research question
2. SQS background and literature review
3. Problem formulation and hypotheses
4. Architecture and simulator assumptions
5. Controller and baseline algorithms
6. Experimental design and reproducibility
7. Simulation results
8. Optional AWS validation (clearly separated)
9. Discussion, threats to validity, and negative results
10. Conclusion and future work

## Suggested presentation (10 slides)

Problem; research gap; SQS timing example; architecture; adaptive algorithm; baselines; workload/failure design; duplicate/recovery trade-off; limitations and AWS validation; conclusion and questions.

## Likely viva questions

- **Why P90?** It is interpretable and protects most jobs while allowing recovery sooner than a worst-case timeout; the percentile should be sensitivity-tested.
- **Why not machine learning?** A statistical baseline is cheaper, explainable, and appropriate until a more complex model demonstrates measurable added value.
- **Is the class baseline fair?** It is deliberately strong and even optimistic. Beating it is harder and more academically informative.
- **Why can duplicates still occur?** At-least-once delivery and lease expiry permit concurrent attempts; idempotency protects logical effects, not compute cost.
- **Why bound heartbeats?** An unhealthy or stalled worker must not hide a message indefinitely.
- **What makes the work reproducible?** External configuration, fixed seeds, event-driven time, CSV outputs, tests, and stated metric definitions.
- **When can adaptation lose?** Cold start, sparse classes, very small windows, abrupt shifts, or a stable workload with a well-tuned static timeout.
- **What is external validity?** Simulation isolates causal effects; a small real-SQS run validates mechanics but cannot establish production-scale performance.
