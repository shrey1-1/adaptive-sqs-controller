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

Before `H` samples, it uses a configured default. Optional extensions occur only while the original worker is healthy and are bounded by both a maximum count and a maximum total visibility horizon. The novelty is not queue automation; it is the interpretable, workload-aware control policy and its reproducible comparison with an optimistic static per-class baseline.

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
- `fixed_per_class`: static class P90 plus margin, fitted to the generated workload; this is an intentionally strong, optimistic comparator.
- `adaptive_p90`: rolling class-specific P90 plus margin with cold-start fallback.
- `fixed_heartbeat`: a fixed initial lease with bounded extensions.

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

Use `configs/full.json` for the larger repeated experiment. Configuration stays outside policy logic. CSV files are written to the selected output folder and ten PNG figures to its `plots/` child.

## Google Colab

Open `Adaptive_SQS_Controller_Colab.ipynb`. It installs packages, clones a repository URL or accepts an uploaded project, exposes editable configuration, runs tests, executes a small demonstration, displays tables/plots, provides a separate full run, and optionally exports to Google Drive. The simulator uses no Docker and no real-time sleeps.

## Optional AWS validation

Simulation requires no credentials. For a small real validation, first create a dedicated Standard queue manually, then set `ADAPTIVE_SQS_TEST_QUEUE_URL` and `AWS_REGION`. Let Boto3 use its standard credential chain or map values from Colab Secrets at runtime. Never put credentials in notebook cells, source files, CSV files, or Git.

Use [docs/iam-policy.json](docs/iam-policy.json) after replacing the account, region, and queue placeholders. It grants only Send/Receive/Delete/ChangeVisibility/GetAttributes on one test queue. Delete or purge the test queue in the AWS console after validation. The adapter never creates a queue and thus cannot create a chargeable resource silently.

## Outputs

- `attempt_metrics.csv`: one record per processing attempt, including required timing and outcome fields.
- `run_metrics.csv`: one row per scenario/policy/seed.
- `aggregate_metrics.csv`: repeated-seed means, SDs, and 95% CIs.
- `plots/*.png`: the ten required visualizations.

Values in `results/smoke/` are generated by the recorded smoke execution; they are simulation findings only. No real-AWS results are claimed.

### Recorded smoke findings

The included smoke run used 50 jobs, 6 workers, 3 scenarios, 2 seeds, and all 6 policies (36 runs; 2,210 attempt rows). It was regenerated after fixing a dispatch bug that could strand a waiting job, switching failures to common random numbers, and replacing normal-approximation CIs with Student-t and bootstrap intervals. It is a functional demonstration, not a sufficiently powered final study. Selected two-seed means for the most important comparison are:

| Scenario | Policy | Duplicate processing | Mean recovery (s) | Mean latency (s) | Throughput (jobs/s) |
|---|---|---:|---:|---:|---:|
| Mixed, no failure | Adaptive P90 | 0.074 | n/a | 6.278 | 0.778 |
| Mixed, no failure | Fixed per class | 0.000 | n/a | 4.674 | 0.910 |
| Mixed, medium failure | Adaptive P90 | 0.064 | 10.953* | 6.263 | 0.790 |
| Mixed, medium failure | Fixed per class | 0.000 | 7.289* | 4.962 | 0.910 |
| Sudden shift, low failure | Adaptive P90 | 0.098 | n/a† | 13.495 | 0.531 |
| Sudden shift, low failure | Fixed per class | 0.048 | n/a† | 11.681 | 0.552 |

\* Single seed: seed 22 sampled no recoverable worker failures. † No worker failures were sampled at the 2% level in either seed.

These results do **not** support a blanket claim that adaptation wins. The optimistic static class baseline had fewer duplicates and lower latency in this small run. Most adaptive duplicates in the stationary scenario occur during cold start, before five observations exist and while the 12 s fallback is shorter than long jobs. With 50 jobs and 2 seeds, recovery-time comparisons are not estimable; run `full.json` (or larger) before drawing academic conclusions.

## Limitations

The simulator models one Standard queue with zero network latency, per-message rather than batched API calls, deterministic per-job service time across retries, instantaneous worker replacement, and first-success deletion after concurrent redelivery. It does not model SQS's rare replicated-copy duplicates, message-retention expiry, in-flight quotas, long-poll billing nuances, network partitions, or AWS service variance. The per-class oracle uses the entire generated dataset and is therefore stronger than a realistically pre-tuned baseline. A normal CI is only approximate with few seeds. See [docs/limitations.md](docs/limitations.md).

## Future extensions

Compare rolling P90 with EWMA or streaming quantile sketches; estimate uncertainty explicitly; model worker heterogeneity and correlated failures; add queue batching and network latency; tune margins from a declared loss function; and perform a preregistered, small-scale real-SQS replication.

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
# adaptive-sqs-controller
