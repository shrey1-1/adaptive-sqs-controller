"""HTML report for a real-SQS replication run (real versus simulated, with figures).

Regenerate from saved results with:
    python -m src.replication_report results/aws_replication
"""
from __future__ import annotations

import html
import json
import sys
import webbrowser
from pathlib import Path

import pandas as pd

METRICS = [
    ("duplicate_processing_rate", "Duplicate-processing rate", "%", 100.0),
    ("mean_failure_recovery_time_s", "Mean failure recovery", "s", 1.0),
    ("mean_end_to_end_latency_s", "Mean end-to-end latency", "s", 1.0),
    ("attempts", "Processing attempts", "", 1.0),
    ("extensions", "Heartbeat extensions", "", 1.0),
    ("makespan_s", "Makespan (all jobs done)", "s", 1.0),
]
REAL, SIM = "#13807A", "#5C7C8A"
OUTCOME_COLORS = {"Committed": "#13807A", "Duplicate result": "#F2A541", "Crashed": "#C8553D"}


def _plots(summary: pd.DataFrame, attempts: pd.DataFrame, plot_dir: Path, run_label: str) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plot_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    policies = list(summary["policy_name"])
    x = range(len(policies))
    for index, (key, label, unit, scale) in enumerate(METRICS, start=1):
        real = summary[f"real_{key}"] * scale
        sim = summary[f"simulated_{key}"] * scale
        fig, ax = plt.subplots(figsize=(9, 4.6))
        width = 0.38
        bars_real = ax.bar([i - width / 2 for i in x], real, width, label=run_label, color=REAL)
        bars_sim = ax.bar([i + width / 2 for i in x], sim, width, label="Simulator prediction", color=SIM)
        for bars in (bars_real, bars_sim):
            for bar in bars:
                value = bar.get_height()
                if pd.notna(value):
                    ax.annotate(f"{value:.2f}" if abs(value) < 100 else f"{value:.0f}", (bar.get_x() + bar.get_width() / 2, value),
                                ha="center", va="bottom", fontsize=9, xytext=(0, 2), textcoords="offset points")
        ax.set_xticks(list(x), policies, rotation=15, ha="right")
        ax.set_ylabel(f"{label} ({unit})" if unit else label)
        ax.set_title(f"{label}: {run_label.lower()} vs simulator")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", color="#E3ECEE")
        ax.set_axisbelow(True)
        ax.legend(frameon=False)
        fig.tight_layout()
        path = plot_dir / f"{index:02d}_{key}.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(path)

    if not attempts.empty:
        names = list(dict.fromkeys(attempts["policy_name"]))
        fig, axes = plt.subplots(len(names), 1, figsize=(10, 2.6 * len(names)), sharex=True, squeeze=False)
        for ax, name in zip(axes[:, 0], names):
            frame = attempts[attempts["policy_name"] == name]
            jobs = sorted(frame["job_id"].unique())
            row = {job: i for i, job in enumerate(jobs)}
            for _, r in frame.iterrows():
                outcome = "Crashed" if r["worker_failure"] else ("Committed" if r["committed_result"] else "Duplicate result")
                ax.barh(row[r["job_id"]], r["actual_processing_duration"], left=r["receive_time"], height=0.8,
                        color=OUTCOME_COLORS[outcome], alpha=0.85)
            ax.set_title(name, loc="left", fontsize=11)
            ax.set_ylabel("job")
            ax.set_yticks([])
            ax.spines[["top", "right"]].set_visible(False)
        axes[-1, 0].set_xlabel("time since the run started (s)")
        handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in OUTCOME_COLORS.values()]
        fig.legend(handles, list(OUTCOME_COLORS), loc="upper right", ncol=3, frameon=False)
        fig.suptitle(f"Every processing attempt in the {run_label.lower()}, by outcome", x=0.01, ha="left")
        fig.tight_layout(rect=(0, 0, 1, 0.96))
        path = plot_dir / f"{len(METRICS) + 1:02d}_attempt_timeline.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(path)
    return paths


def _agreement(summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in summary.iterrows():
        for key, label, unit, scale in METRICS:
            real, sim = r[f"real_{key}"] * scale, r[f"simulated_{key}"] * scale
            diff = real - sim if pd.notna(real) and pd.notna(sim) else float("nan")
            rel = diff / sim * 100 if pd.notna(diff) and sim not in (0, 0.0) else float("nan")
            rows.append({"Policy": r["policy_name"], "Metric": label + (f" ({unit})" if unit else ""),
                         "Queue run": real, "Simulated": sim, "Difference": diff, "Relative difference (%)": rel})
    return pd.DataFrame(rows)


def write_replication_report(output_dir: str | Path) -> Path:
    output_dir = Path(output_dir)
    summary = pd.read_csv(output_dir / "replication_summary.csv")
    attempts_path = output_dir / "replication_attempts.csv"
    attempts = pd.read_csv(attempts_path) if attempts_path.exists() else pd.DataFrame()
    config_path = output_dir / "replication_config.json"
    config = json.loads(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    fake = config.get("mode") == "fake" or not config.get("integer_timeouts", True)
    try:
        plots = _plots(summary, attempts, output_dir / "plots", "Fake-queue run" if fake else "Real SQS run")
    except ModuleNotFoundError:
        plots = []

    banner = (
        '<div class="banner fake"><b>Dry run against an in-memory fake queue.</b> The fake follows the simulator\'s rules, '
        "so close agreement shows the harness works; it is not evidence about Amazon SQS.</div>"
        if fake else
        '<div class="banner real"><b>Real Amazon SQS run.</b> Differences from the simulator are findings: '
        "network latency, polling, thread scheduling and SQS behaviour all play a part.</div>"
    )
    timed_out = summary.get("real_timed_out", pd.Series(dtype=float)).fillna(0).astype(bool)
    if timed_out.any():
        banner += f'<div class="banner warn">Policies that hit the time limit: {html.escape(", ".join(summary.loc[timed_out, "policy_name"]))}</div>'

    agreement = _agreement(summary).round(3)
    figures = "\n".join(
        f'<div class="figure"><a href="plots/{p.name}"><img src="plots/{p.name}" alt="{html.escape(p.stem)}"></a></div>' for p in plots
    ) or "<p>Install matplotlib to generate figures.</p>"
    details = {k: config[k] for k in ("n_jobs", "workers", "seed", "failure_probability", "workload", "time_scale", "policies") if k in config}
    config_rows = "".join(f"<tr><td>{html.escape(str(k))}</td><td>{html.escape(str(v))}</td></tr>" for k, v in details.items())

    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>SQS Replication Report</title>
<style>
body {{ font-family: Segoe UI, Arial, sans-serif; margin: 32px; color: #1f2937; background: #f8fafc; }}
.card {{ background: white; border-radius: 12px; padding: 22px; margin: 18px 0; box-shadow: 0 2px 10px #0001; overflow-x: auto; }}
h1 {{ color: #0B3C49; }} h2 {{ color: #13807A; }}
table {{ border-collapse: collapse; font-size: 13px; }}
th {{ background: #0B3C49; color: white; text-align: left; padding: 8px 10px; }}
td {{ padding: 7px 10px; border-bottom: 1px solid #dbe3ec; }} tr:nth-child(even) {{ background: #f1f5f9; }}
.banner {{ padding: 14px 18px; border-radius: 10px; margin: 12px 0; }}
.fake {{ background: #FEF3DC; }} .real {{ background: #DDF0EE; }} .warn {{ background: #F9E0DA; }}
.figure img {{ display: block; max-width: 100%; width: 900px; height: auto; border: 1px solid #dbe3ec; margin: 18px 0; }}
.note {{ color: #475569; }}
</style></head><body>
<h1>SQS Replication Report</h1>
<p class="note">The same seeded jobs and timeout policies run through a queue and through the simulator, then compared. One run per policy: this tests whether the simulator predicts the queue, not which policy is better.</p>
{banner}
<div class="card"><h2>Figures</h2>{figures}</div>
<div class="card"><h2>Real vs simulated, every metric</h2>{agreement.to_html(index=False, na_rep="—")}</div>
<div class="card"><h2>Run settings</h2><table>{config_rows}</table>
<p class="note">Files: <a href="replication_summary.csv">replication_summary.csv</a> · <a href="replication_attempts.csv">replication_attempts.csv</a> · <a href="replication_config.json">replication_config.json</a></p></div>
</body></html>"""
    path = output_dir / "replication_report.html"
    path.write_text(page, encoding="utf-8")
    return path


if __name__ == "__main__":
    report = write_replication_report(sys.argv[1] if len(sys.argv) > 1 else "results/aws_replication")
    print(f"Report: {report}")
    webbrowser.open(report.resolve().as_uri())
