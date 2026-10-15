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
    AdaptivePercentilePolicy,
    ClassBasedPolicy,
    FixedHeartbeatPolicy,
    FixedPolicy,
    MeanFixedPolicy,
)
from .simulator import SQSSimulator
from .workload import WorkloadSpec, generate_workload, oracle_class_timeouts


DEFAULT_SCENARIOS = {
    "mostly_short_no_failure": {"workload": "mostly_short", "failure": "none"},
    "mixed_no_failure": {"workload": "mixed", "failure": "none"},
    "heavy_tailed_no_failure": {"workload": "heavy_tailed", "failure": "none"},
    "mixed_with_failures": {"workload": "mixed", "failure": "medium"},
    "heavy_tailed_with_failures": {"workload": "heavy_tailed", "failure": "medium"},
    "sudden_shift": {"workload": "sudden_shift", "failure": "low"},
}


def build_policies(messages, safety_margin: float = 2.0, history_window: int = 40):
    durations = np.asarray([m.processing_duration for m in messages])
    return [
        FixedPolicy(3.0, "fixed_short"),
        FixedPolicy(30.0, "fixed_long"),
        MeanFixedPolicy(float(durations.mean()), safety_margin=0.0),
        ClassBasedPolicy(oracle_class_timeouts(messages, safety_margin), 15.0),
        AdaptivePercentilePolicy(
            safety_margin=safety_margin,
            history_window=history_window,
            default_timeout=12.0,
            min_history=5,
        ),
        FixedHeartbeatPolicy(5.0, heartbeat_interval=3.5, max_extensions=4, max_total_visibility=40.0),
    ]


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
            for margin in margins:
                for window in windows:
                    for policy in build_policies(messages, float(margin), int(window)):
                        selected = config.get("policies")
                        if selected and policy.name not in selected:
                            continue
                        sim = SQSSimulator(
                            messages,
                            policy,
                            FailureModel.from_level(scenario["failure"]),
                            workers=int(config.get("workers", 8)),
                            seed=seed + 1000,
                        )
                        result = sim.run()
                        attempts = pd.DataFrame(result["attempt_rows"])
                        attempts["scenario"] = scenario_name
                        attempts["seed"] = seed
                        attempts["safety_margin"] = float(margin)
                        attempts["history_window"] = int(window)
                        all_attempts.append(attempts)
                        row = summarize_run(result, shift_time)
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
    attempt_df.to_csv(output_dir / "attempt_metrics.csv", index=False)
    run_df.to_csv(output_dir / "run_metrics.csv", index=False)
    aggregate_df.to_csv(output_dir / "aggregate_metrics.csv", index=False)
    return attempt_df, run_df, aggregate_df


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
        "duplicate_processing_rate_mean", "mean_end_to_end_latency_s_mean",
        "p95_end_to_end_latency_s_mean", "throughput_jobs_per_s_mean",
        "mean_failure_recovery_time_s_mean", "queue_api_operations_per_completed_message_mean",
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
    }
    summary = summary.rename(columns=rename)
    numeric = summary.select_dtypes(include="number").columns
    summary[numeric] = summary[numeric].round(3)
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
<div class="card"><h2>Figures</h2>{plot_links or '<p>Run with --plots to generate figures.</p>'}</div>
<div class="card"><h2>Files</h2><ul><li><a href="run_metrics.csv">Run metrics CSV</a></li><li><a href="aggregate_metrics.csv">Aggregate metrics CSV</a></li><li><a href="attempt_metrics.csv">Attempt metrics CSV</a></li></ul></div>
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


if __name__ == "__main__":
    main()
