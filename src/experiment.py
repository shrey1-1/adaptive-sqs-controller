from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .failure_model import FailureModel
from .metrics import aggregate_runs, summarize_run
from .policies import (
    AdaptiveHeartbeatPolicy,
    AdaptivePercentilePolicy,
    ClassBasedPolicy,
    FixedHeartbeatPolicy,
    FixedPolicy,
    MeanFixedPolicy,
)
from .simulator import SQSSimulator
from .stats import holm_adjust, paired_comparisons, pareto_optimal, primary_tests
from .workload import WorkloadSpec, generate_workload, oracle_class_timeouts, pretuned_class_timeouts


DEFAULT_SCENARIOS = {
    "mostly_short_no_failure": {"workload": "mostly_short", "failure": "none"},
    "mixed_no_failure": {"workload": "mixed", "failure": "none"},
    "heavy_tailed_no_failure": {"workload": "heavy_tailed", "failure": "none"},
    "mixed_with_failures": {"workload": "mixed", "failure": "medium"},
    "heavy_tailed_with_failures": {"workload": "heavy_tailed", "failure": "medium"},
    "sudden_shift": {"workload": "sudden_shift", "failure": "low"},
}


# Extra adaptive configurations run alongside `adaptive_p90`. Keys are policy
# names; values override AdaptivePercentilePolicy arguments, or
# AdaptiveHeartbeatPolicy arguments when they include "type": "heartbeat".
# Scenarios may add their own via an "adaptive_variants" entry (used for
# sensitivity sweeps).
DEFAULT_ADAPTIVE_VARIANTS = {
    "adaptive_p90_relative": {"safety_margin": 0.0, "relative_margin": 0.25},
    # Ablation of the partial-history cold start (the pre-fix behaviour).
    "adaptive_p90_fixed_fallback": {"cold_start": "default"},
    # Phase 4: each differs from adaptive_p90 in one setting, the last in two.
    "adaptive_cost_aware": {"cost_ratio": 1.0},
    "adaptive_conformal": {"quantile_method": "conformal"},
    "adaptive_cost_conformal": {"cost_ratio": 1.0, "quantile_method": "conformal"},
}


def build_policies(
    messages,
    safety_margin: float = 2.0,
    history_window: int = 40,
    spec: WorkloadSpec | None = None,
    seed: int | None = None,
    calibration_fraction: float = 0.2,
    adaptive_variants: dict[str, dict[str, Any]] | None = None,
):
    durations = np.asarray([m.processing_duration for m in messages])
    adaptive_defaults = dict(
        safety_margin=safety_margin,
        history_window=history_window,
        default_timeout=12.0,
        min_history=5,
    )
    policies = [
        FixedPolicy(3.0, "fixed_short"),
        FixedPolicy(30.0, "fixed_long"),
        MeanFixedPolicy(float(durations.mean()), safety_margin=0.0),
        ClassBasedPolicy(oracle_class_timeouts(messages, safety_margin), 15.0),
    ]
    if spec is not None and seed is not None:
        policies.append(
            ClassBasedPolicy(
                pretuned_class_timeouts(spec, seed, safety_margin, calibration_fraction),
                15.0,
                name="pretuned_per_class",
            )
        )
    policies.append(AdaptivePercentilePolicy(**adaptive_defaults))
    # Heartbeat policies share the fixed heartbeat's 5 s lease and 40 s global
    # cap so the comparison isolates what adaptation adds.
    heartbeat_defaults = dict(
        safety_margin=safety_margin, history_window=history_window, min_history=5,
        lease_cap=5.0, default_lease=5.0, max_total_visibility=40.0,
    )
    policies.append(AdaptiveHeartbeatPolicy(**heartbeat_defaults))
    policies.append(AdaptiveHeartbeatPolicy(name="adaptive_heartbeat_fixed_budget", adaptive_budget=False, **heartbeat_defaults))
    for name, overrides in (adaptive_variants or {}).items():
        overrides = dict(overrides)
        if overrides.pop("type", "percentile") == "heartbeat":
            cls, base = AdaptiveHeartbeatPolicy, heartbeat_defaults
        else:
            cls, base = AdaptivePercentilePolicy, adaptive_defaults
        params = {**base, **overrides}
        params["history_window"] = max(int(params["history_window"]), int(params["min_history"]))
        policies.append(cls(name=name, **params))
    policies.append(FixedHeartbeatPolicy(5.0, heartbeat_interval=3.5, max_extensions=4, max_total_visibility=40.0))
    return policies


def run_experiments(config: dict[str, Any], output_dir: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    all_attempts: list[pd.DataFrame] = []
    run_rows: list[dict[str, Any]] = []
    scenarios = config.get("scenarios", DEFAULT_SCENARIOS)
    for scenario_name, scenario in scenarios.items():
        for seed in config.get("seeds", [11, 22, 33]):
            experiment_id = f"{scenario_name}-seed{seed}"
            spec = WorkloadSpec(
                scenario["workload"],
                n_messages=int(config.get("n_messages", 120)),
                arrival_rate=float(config.get("arrival_rate", 1.2)),
                burst_probability=float(config.get("burst_probability", 0.0)),
            )
            messages, shift_time = generate_workload(spec, seed, experiment_id)
            margins = scenario.get("safety_margins", [config.get("safety_margin", 2.0)])
            windows = scenario.get("history_windows", [config.get("history_window", 40)])
            variants = {
                **config.get("adaptive_variants", DEFAULT_ADAPTIVE_VARIANTS),
                **scenario.get("adaptive_variants", {}),
            }
            selected = scenario.get("policies", config.get("policies"))
            for margin in margins:
                for window in windows:
                    policies = build_policies(
                        messages,
                        float(margin),
                        int(window),
                        spec=spec,
                        seed=seed,
                        calibration_fraction=float(config.get("calibration_fraction", 0.2)),
                        adaptive_variants=variants,
                    )
                    for policy in policies:
                        if selected and policy.name not in selected:
                            continue
                        sim = SQSSimulator(
                            messages,
                            policy,
                            FailureModel.from_level(scenario["failure"], scenario.get("hang", "none")),
                            workers=int(config.get("workers", 8)),
                            seed=seed + 1000,
                            hang_detection_timeout=scenario.get("hang_detection"),
                        )
                        result = sim.run()
                        attempts = pd.DataFrame(result["attempt_rows"])
                        attempts["scenario"] = scenario_name
                        attempts["seed"] = seed
                        attempts["safety_margin"] = float(margin)
                        attempts["history_window"] = int(window)
                        all_attempts.append(attempts)
                        row = summarize_run(result, shift_time, float(config.get("warmup_fraction", 0.2)))
                        row.update(
                            scenario=scenario_name,
                            policy_name=policy.name,
                            seed=seed,
                            safety_margin=float(margin),
                            history_window=int(window),
                        )
                        run_rows.append(row)
    attempt_df = pd.concat(all_attempts, ignore_index=True)
    run_df = pd.DataFrame(run_rows)
    aggregate_df = aggregate_runs(run_df)
    aggregate_df["pareto_optimal_dup_vs_recovery"] = _pareto_flags(aggregate_df)
    references = config.get("paired_reference", ["adaptive_p90", "adaptive_heartbeat"])
    references = [references] if isinstance(references, str) else list(references)
    paired_df = pd.concat([paired_comparisons(run_df, reference=r) for r in references], ignore_index=True)
    if not paired_df.empty:
        paired_df["p_holm"] = holm_adjust(paired_df["p_value"])  # exploratory family = every test in the file
        paired_df["family"] = "exploratory"
    primary_df = primary_tests(run_df, config.get("primary_hypotheses", []))
    attempt_df.to_csv(output_dir / "attempt_metrics.csv", index=False)
    run_df.to_csv(output_dir / "run_metrics.csv", index=False)
    aggregate_df.to_csv(output_dir / "aggregate_metrics.csv", index=False)
    paired_df.to_csv(output_dir / "paired_comparisons.csv", index=False)
    primary_df.to_csv(output_dir / "primary_hypotheses.csv", index=False)
    return attempt_df, run_df, aggregate_df


PARETO_X = "mean_failure_recovery_time_s_mean"
PARETO_Y = "duplicate_processing_rate_mean"


def _pareto_flags(aggregates: pd.DataFrame) -> pd.Series:
    """Pareto optimality on (recovery, duplicates) within each scenario/sweep cell."""
    keys = [k for k in ["scenario", "safety_margin", "history_window"] if k in aggregates.columns]
    flags = pd.Series(False, index=aggregates.index)
    for _, frame in aggregates.groupby(keys, dropna=False):
        flags.loc[frame.index] = pareto_optimal(frame, PARETO_X, PARETO_Y)
    return flags


def generate_plots(attempts: pd.DataFrame, runs: pd.DataFrame, output_dir: str | Path) -> None:
    import matplotlib.pyplot as plt
    import seaborn as sns

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    def bar(metric: str, ylabel: str, filename: str, title: str) -> None:
        plt.figure(figsize=(10, 5))
        sns.barplot(data=runs, x="policy_name", y=metric, hue="scenario", errorbar="sd")
        plt.title(title); plt.xlabel("Visibility-timeout policy"); plt.ylabel(ylabel)
        plt.xticks(rotation=30, ha="right"); plt.tight_layout(); plt.savefig(output_dir / filename, dpi=160); plt.close()

    bar("duplicate_processing_rate", "Duplicate-processing rate (fraction)", "01_duplicate_processing_rate.png", "Duplicate processing by policy")
    bar("mean_failure_recovery_time_s", "Mean recovery time (simulated seconds)", "02_failure_recovery.png", "Failure recovery by policy")

    latency = runs.melt(id_vars=["policy_name", "scenario"], value_vars=["mean_end_to_end_latency_s", "p95_end_to_end_latency_s"], var_name="statistic", value_name="latency")
    plt.figure(figsize=(11, 5)); sns.barplot(data=latency, x="policy_name", y="latency", hue="statistic", errorbar="sd")
    plt.title("Mean and P95 end-to-end latency"); plt.xlabel("Visibility-timeout policy"); plt.ylabel("Latency (simulated seconds)"); plt.xticks(rotation=30, ha="right"); plt.tight_layout(); plt.savefig(output_dir / "03_latency.png", dpi=160); plt.close()
    bar("throughput_jobs_per_s", "Throughput (jobs/simulated second)", "04_throughput.png", "Throughput by policy")

    adaptive_runs = runs[runs.policy_name == "adaptive_p90"]
    for x, y, name, title, xlabel, ylabel in [
        ("safety_margin", "duplicate_processing_rate", "05_margin_duplicate.png", "Safety margin versus duplicates", "Safety margin (seconds)", "Duplicate-processing rate (fraction)"),
        ("safety_margin", "mean_failure_recovery_time_s", "06_margin_recovery.png", "Safety margin versus recovery", "Safety margin (seconds)", "Mean recovery time (simulated seconds)"),
    ]:
        plt.figure(figsize=(8, 5)); sns.lineplot(data=adaptive_runs, x=x, y=y, hue="scenario", marker="o", errorbar="sd")
        plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel); plt.tight_layout(); plt.savefig(output_dir / name, dpi=160); plt.close()

    adaptive_attempts = attempts[(attempts.policy_name == "adaptive_p90") & (attempts.successful_completion)]
    sample = adaptive_attempts.sample(min(len(adaptive_attempts), 1500), random_state=7) if len(adaptive_attempts) else adaptive_attempts
    plt.figure(figsize=(7, 6)); sns.scatterplot(data=sample, x="actual_processing_duration", y="predicted_processing_duration", hue="job_type", alpha=.6)
    if len(sample):
        limit = float(max(sample.actual_processing_duration.max(), sample.predicted_processing_duration.max())); plt.plot([0, limit], [0, limit], "k--", label="Perfect prediction")
    plt.title("Adaptive prediction versus actual duration"); plt.xlabel("Actual duration (simulated seconds)"); plt.ylabel("Predicted duration (simulated seconds)"); plt.tight_layout(); plt.savefig(output_dir / "07_prediction_actual.png", dpi=160); plt.close()

    shift = adaptive_attempts[adaptive_attempts.scenario == "sudden_shift"].sort_values("receive_time")
    plt.figure(figsize=(9, 5));
    if len(shift):
        sns.lineplot(data=shift, x="receive_time", y="predicted_processing_duration", label="Predicted", errorbar=None); sns.scatterplot(data=shift, x="receive_time", y="actual_processing_duration", label="Actual", alpha=.35)
    plt.title("Adaptive controller around sudden workload shift"); plt.xlabel("Simulated time (seconds)"); plt.ylabel("Duration (seconds)"); plt.tight_layout(); plt.savefig(output_dir / "08_shift_behaviour.png", dpi=160); plt.close()
    bar("queue_api_operations_per_completed_message", "Queue operations / completed job", "09_api_overhead.png", "Queue API-operation overhead")
    plt.figure(figsize=(8, 6)); sns.scatterplot(data=runs, x="mean_failure_recovery_time_s", y="duplicate_processing_rate", hue="policy_name", style="scenario", s=90)
    plt.title("Duplicate processing versus failure recovery"); plt.xlabel("Mean recovery time (simulated seconds)"); plt.ylabel("Duplicate-processing rate (fraction)"); plt.tight_layout(); plt.savefig(output_dir / "10_tradeoff.png", dpi=160); plt.close()

    _plot_pareto_front(runs, output_dir / "11_pareto_front.png")
    _plot_steady_state(runs, output_dir / "12_cold_start_vs_steady.png")


def _plot_pareto_front(runs: pd.DataFrame, path: Path) -> None:
    """Per-scenario policy means with ±1 SD bars; the non-dominated set is joined."""
    import matplotlib.pyplot as plt

    x, y = "mean_failure_recovery_time_s", "duplicate_processing_rate"
    base = runs
    for column in ("safety_margin", "history_window"):  # keep the default setting, not sweep cells
        if column in base:
            base = base[base[column] == base[column].mode().iat[0]]
    stats = base.groupby(["scenario", "policy_name"])[[x, y]].agg(["mean", "std"])
    stats.columns = [f"{metric}_{stat}" for metric, stat in stats.columns]
    stats = stats.reset_index().dropna(subset=[f"{x}_mean", f"{y}_mean"])
    scenarios = sorted(stats["scenario"].unique())
    if not scenarios:
        return
    policies = sorted(stats["policy_name"].unique())
    palette = plt.get_cmap("tab20").colors
    colours = {policy: palette[i % len(palette)] for i, policy in enumerate(policies)}
    cols = min(3, len(scenarios))
    rows = -(-len(scenarios) // cols)
    fig, axes = plt.subplots(rows, cols, figsize=(5.2 * cols, 4.4 * rows), squeeze=False)
    for ax, scenario in zip(axes.flat, scenarios):
        frame = stats[stats["scenario"] == scenario].copy()
        frame["optimal"] = pareto_optimal(frame, f"{x}_mean", f"{y}_mean")
        for _, row in frame.iterrows():
            ax.errorbar(
                row[f"{x}_mean"], row[f"{y}_mean"], xerr=row[f"{x}_std"], yerr=row[f"{y}_std"],
                fmt="o" if row["optimal"] else "x", color=colours[row["policy_name"]],
                markersize=8, capsize=3, alpha=0.9 if row["optimal"] else 0.55,
            )
        front = frame[frame["optimal"]].sort_values(f"{x}_mean")
        ax.plot(front[f"{x}_mean"], front[f"{y}_mean"], "k--", linewidth=1)
        ax.set_title(scenario); ax.set_xlabel("Mean recovery (s)"); ax.set_ylabel("Duplicate-processing rate")
    for ax in list(axes.flat)[len(scenarios):]:
        ax.set_visible(False)
    handles = [plt.Line2D([], [], marker="o", linestyle="", color=colours[p], label=p) for p in policies]
    fig.legend(handles=handles, loc="lower center", ncol=min(len(policies), 4), frameon=False)
    fig.suptitle("Duplicate/recovery Pareto front (circles = non-dominated, dashed = front)")
    fig.tight_layout(rect=(0, 0.12, 1, 0.96)); fig.savefig(path, dpi=160); plt.close(fig)


def _plot_steady_state(runs: pd.DataFrame, path: Path) -> None:
    """Duplicate rate over all jobs versus after warm-up, exposing cold-start cost."""
    import matplotlib.pyplot as plt
    import seaborn as sns

    if "steady_duplicate_processing_rate" not in runs:
        return
    long = runs.melt(
        id_vars=["policy_name"],
        value_vars=["duplicate_processing_rate", "steady_duplicate_processing_rate"],
        var_name="window", value_name="rate",
    )
    long["window"] = long["window"].map({"duplicate_processing_rate": "All jobs", "steady_duplicate_processing_rate": "After warm-up"})
    plt.figure(figsize=(11, 5)); sns.barplot(data=long, x="policy_name", y="rate", hue="window", errorbar="sd")
    plt.title("Duplicate processing: all jobs versus steady state"); plt.xlabel("Visibility-timeout policy"); plt.ylabel("Duplicate-processing rate (fraction)")
    plt.xticks(rotation=30, ha="right"); plt.tight_layout(); plt.savefig(path, dpi=160); plt.close()


def _read_optional_csv(path: Path) -> pd.DataFrame:
    """A results CSV that may be missing or empty (e.g. no declared hypotheses)."""
    try:
        return pd.read_csv(path)
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return pd.DataFrame()


def write_html_report(
    attempts: pd.DataFrame,
    runs: pd.DataFrame,
    aggregates: pd.DataFrame,
    output_dir: str | Path,
) -> Path:
    """Write a human-readable report while retaining machine-readable CSVs."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    plot_dir = output_dir / "plots"
    selected = [
        "scenario", "policy_name", "repetitions",
        "duplicate_processing_rate_mean", "steady_duplicate_processing_rate_mean",
        "mean_end_to_end_latency_s_mean",
        "p95_end_to_end_latency_s_mean", "throughput_jobs_per_s_mean",
        "mean_failure_recovery_time_s_mean", "queue_api_operations_per_completed_message_mean",
        "pareto_optimal_dup_vs_recovery",
    ]
    selected = [column for column in selected if column in aggregates.columns]
    summary = aggregates[selected].copy()
    rename = {
        "policy_name": "Policy",
        "scenario": "Scenario",
        "repetitions": "Seeds",
        "duplicate_processing_rate_mean": "Duplicate processing rate",
        "mean_end_to_end_latency_s_mean": "Mean latency (s)",
        "p95_end_to_end_latency_s_mean": "P95 latency (s)",
        "throughput_jobs_per_s_mean": "Throughput (jobs/s)",
        "mean_failure_recovery_time_s_mean": "Mean recovery (s)",
        "queue_api_operations_per_completed_message_mean": "API operations / job",
        "steady_duplicate_processing_rate_mean": "Duplicate rate after warm-up",
        "pareto_optimal_dup_vs_recovery": "Pareto-optimal",
    }
    summary = summary.rename(columns=rename)
    numeric = summary.select_dtypes(include="number").columns
    summary[numeric] = summary[numeric].round(3)
    primary_html = "<p>No primary hypotheses were declared in the configuration.</p>"
    primary = _read_optional_csv(output_dir / "primary_hypotheses.csv")
    if not primary.empty:
        primary_html = (
            '<p class="note">Declared family; Holm correction applies within it only. '
            "Mean difference = reference − comparator over seeds.</p>"
            + primary.round(4).to_html(index=False, na_rep="—")
        )
    paired_html = "<p>No paired comparisons were produced.</p>"
    paired = _read_optional_csv(output_dir / "paired_comparisons.csv")
    if not paired.empty:
        headline = paired[paired["metric"].isin(["duplicate_processing_rate", "steady_duplicate_processing_rate", "mean_failure_recovery_time_s"])]
        headline = headline[["scenario", "reference", "comparator", "metric", "n_pairs", "mean_difference", "ci_low", "ci_high", "p_value", "p_holm"]].round(4)
        paired_html = (
            '<p class="note">Mean difference = reference − comparator over seeds (negative = reference lower). '
            "Wilcoxon signed-rank p-values; <code>p_holm</code> is Holm-adjusted across every exploratory test in "
            '<a href="paired_comparisons.csv">paired_comparisons.csv</a>.</p>'
            + headline.to_html(index=False, na_rep="—")
        )
    plot_links = "\n".join(
        f'<div class="figure"><h3>{index:02d} {path.stem[3:].replace("_", " ").title()}</h3><a href="plots/{path.name}"><img src="plots/{path.name}" alt="{path.stem.replace("_", " ")}"></a></div>'
        for index, path in enumerate(sorted(plot_dir.glob("*.png")), start=1)
    )
    report = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Adaptive SQS Controller Report</title>
<style>
body {{ font-family: Segoe UI, Arial, sans-serif; margin: 32px; color: #1f2937; background: #f8fafc; }}
.card {{ background: white; border-radius: 12px; padding: 22px; margin: 18px 0; box-shadow: 0 2px 10px #0001; overflow-x: auto; }}
h1 {{ color: #12355b; }} h2 {{ color: #205493; }}
table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
th {{ background: #205493; color: white; text-align: left; padding: 9px; position: sticky; top: 0; }}
td {{ padding: 8px; border-bottom: 1px solid #dbe3ec; }} tr:nth-child(even) {{ background: #f1f5f9; }}
a {{ color: #1769aa; }} .note {{ color: #475569; }}
.figure {{ margin: 26px 0 34px; }}
.figure h3 {{ color: #205493; margin-bottom: 10px; }}
.figure img {{ display: block; max-width: 100%; width: 900px; height: auto; border: 1px solid #dbe3ec; }}
</style></head><body>
<h1>Adaptive SQS Visibility Timeout Controller</h1>
<p class="note">Readable summary generated from executed simulation results. Detailed attempt-level data remains in the CSV files beside this report.</p>
<div class="card"><h2>Experiment overview</h2>
<p><b>Runs:</b> {len(runs):,} &nbsp; <b>Attempt records:</b> {len(attempts):,} &nbsp; <b>Scenarios:</b> {runs['scenario'].nunique()} &nbsp; <b>Policies:</b> {runs['policy_name'].nunique()}</p>
</div>
<div class="card"><h2>Policy comparison</h2>{summary.to_html(index=False, na_rep="—", classes="summary")}</div>
<div class="card"><h2>Primary hypotheses</h2>{primary_html}</div>
<div class="card"><h2>Exploratory paired comparisons</h2>{paired_html}</div>
<div class="card"><h2>Figures</h2>{plot_links or '<p>Run with --plots to generate figures.</p>'}</div>
<div class="card"><h2>Files</h2><ul><li><a href="run_metrics.csv">Run metrics CSV</a></li><li><a href="aggregate_metrics.csv">Aggregate metrics CSV</a></li><li><a href="primary_hypotheses.csv">Primary hypotheses CSV</a></li><li><a href="paired_comparisons.csv">Paired comparisons CSV</a></li><li><a href="attempt_metrics.csv">Attempt metrics CSV</a></li></ul></div>
</body></html>"""
    report_path = output_dir / "report.html"
    report_path.write_text(report, encoding="utf-8")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", default="results")
    parser.add_argument("--plots", action="store_true")
    parser.add_argument("--verbose", action="store_true", help="Print the full aggregate table in the terminal")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    attempts, runs, aggregates = run_experiments(config, args.output)
    if args.plots:
        try:
            generate_plots(attempts, runs, Path(args.output) / "plots")
        except ModuleNotFoundError as exc:
            if exc.name not in {"matplotlib", "seaborn"}:
                raise
            print("Plotting packages are unavailable; CSVs and the HTML report will still be generated.")
    report_path = write_html_report(attempts, runs, aggregates, args.output)
    if args.verbose:
        print(aggregates.to_string(index=False))
    else:
        headline = runs.groupby("policy_name", as_index=False)[
            ["duplicate_processing_rate", "mean_end_to_end_latency_s", "throughput_jobs_per_s"]
        ].mean().round(3)
        print(f"Completed {len(runs):,} runs and recorded {len(attempts):,} attempts.")
        print(f"Readable report: {report_path}")
        print(headline.to_string(index=False))
        primary = _read_optional_csv(Path(args.output) / "primary_hypotheses.csv")
        if not primary.empty:
            print()
            print("Primary hypotheses (Holm within family):")
            print(primary[["hypothesis", "scenario", "reference", "comparator", "metric", "mean_difference", "p_holm", "supported"]].round(4).to_string(index=False))


if __name__ == "__main__":
    main()
