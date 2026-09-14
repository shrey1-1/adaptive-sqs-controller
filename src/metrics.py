from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def summarize_run(sim_result: dict[str, Any], shift_time: float | None = None) -> dict[str, float]:
    df = pd.DataFrame(sim_result["attempt_rows"])
    completed = df[df["committed_result"]]
    successful_attempts = df[df["successful_completion"]]
    total_attempts = max(len(df), 1)
    completed_jobs = int(sim_result["completed_jobs"])
    sent_jobs = int(df["job_id"].nunique()) if not df.empty else 0
    end_to_end = completed["completion_time"] - completed["send_time"]
    failures = df[df["worker_failure"]]
    recoveries = np.asarray(sim_result["failure_recoveries"], dtype=float)
    api_total = sum(sim_result["api_operations"].values())
    makespan = max(float(sim_result["simulation_end"]), 1e-12)
    prediction_error = np.abs(
        successful_attempts["actual_processing_duration"]
        - successful_attempts["predicted_processing_duration"]
    )
    wasted = float(failures["actual_processing_duration"].sum())
    wasted += float(
        successful_attempts.loc[successful_attempts["duplicate_processing"], "actual_processing_duration"].sum()
    )
    adaptation_time = math.nan
    if shift_time is not None and not completed.empty:
        post = completed[completed["receive_time"] >= shift_time].sort_values("receive_time")
        if len(post) >= 5:
            target = float(np.percentile(post["actual_processing_duration"], 90))
            tolerance = max(0.2 * target, 0.5)
            matches = (post["predicted_processing_duration"] - target).abs() <= tolerance
            for i in range(0, len(post) - 4):
                if bool(matches.iloc[i : i + 5].all()):
                    adaptation_time = float(post["receive_time"].iloc[i] - shift_time)
                    break
    return {
        "attempts": float(len(df)),
        "completed_jobs": float(completed_jobs),
        "duplicate_delivery_rate": float(df["duplicate_delivery"].mean()) if len(df) else 0.0,
        "duplicate_processing_rate": float(df["duplicate_processing"].mean()) if len(df) else 0.0,
        "wasted_processing_time_s": wasted,
        "mean_end_to_end_latency_s": float(end_to_end.mean()) if len(end_to_end) else math.nan,
        "p95_end_to_end_latency_s": float(end_to_end.quantile(0.95)) if len(end_to_end) else math.nan,
        "throughput_jobs_per_s": completed_jobs / makespan,
        "mean_failure_recovery_time_s": float(recoveries.mean()) if recoveries.size else math.nan,
        "p95_failure_recovery_time_s": float(np.percentile(recoveries, 95)) if recoveries.size else math.nan,
        "mean_absolute_prediction_error_s": float(prediction_error.mean()) if len(prediction_error) else math.nan,
        "timeout_extensions_per_completed_job": float(df["number_of_timeout_extensions"].sum() / max(completed_jobs, 1)),
        "successful_completion_rate": completed_jobs / max(sent_jobs, 1),
        "queue_api_operations_per_completed_message": api_total / max(completed_jobs, 1),
        "adaptation_time_s": adaptation_time,
        "worker_failures": float(len(failures)),
        "event_limit_reached": float(bool(sim_result["event_limit_reached"])),
    }


def aggregate_runs(run_metrics: pd.DataFrame, confidence: float = 0.95) -> pd.DataFrame:
    """Mean, sample SD, and normal-approximation CI across independent seeds."""
    group_cols = [c for c in ["scenario", "policy_name", "safety_margin", "history_window"] if c in run_metrics]
    numeric = [c for c in run_metrics.select_dtypes(include="number").columns if c not in {"seed", "safety_margin", "history_window"}]
    rows: list[dict[str, Any]] = []
    z = 1.96 if confidence == 0.95 else 1.96
    for keys, frame in run_metrics.groupby(group_cols, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        row = dict(zip(group_cols, keys))
        row["repetitions"] = len(frame)
        for metric in numeric:
            values = frame[metric].dropna()
            mean = float(values.mean()) if len(values) else math.nan
            std = float(values.std(ddof=1)) if len(values) > 1 else 0.0 if len(values) else math.nan
            half = z * std / math.sqrt(len(values)) if len(values) > 1 else 0.0 if len(values) else math.nan
            row[f"{metric}_mean"] = mean
            row[f"{metric}_std"] = std
            row[f"{metric}_ci95_low"] = mean - half if not math.isnan(half) else math.nan
            row[f"{metric}_ci95_high"] = mean + half if not math.isnan(half) else math.nan
        rows.append(row)
    return pd.DataFrame(rows)

