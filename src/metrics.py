from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def summarize_run(
    sim_result: dict[str, Any],
    shift_time: float | None = None,
    warmup_fraction: float = 0.2,
) -> dict[str, float]:
    df = pd.DataFrame(sim_result["attempt_rows"])
    completed = df[df["committed_result"]]
    successful_attempts = df[df["successful_completion"]]
    total_attempts = max(len(df), 1)
    completed_jobs = int(sim_result["completed_jobs"])
    sent_jobs = int(df["job_id"].nunique()) if not df.empty else 0
    end_to_end = completed["completion_time"] - completed["send_time"]
    failures = df[df["worker_failure"]]
    recoveries = np.asarray(sim_result["failure_recoveries"], dtype=float)
    kinds = np.asarray(sim_result.get("failure_recovery_kinds", ["crash"] * recoveries.size))
    hang_recoveries = recoveries[kinds == "hang"] if recoveries.size else recoveries
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
    predicts = bool(sim_result.get("policy_predicts_duration", True))
    if not predicts:
        prediction_error = prediction_error.iloc[0:0]
    # Coverage: share of jobs whose first attempt finished within the predicted
    # quantile itself (before any margin), excluding cold-start fallbacks. A
    # conformal q-quantile should cover at least q of exchangeable durations.
    # First attempts only: a job that overran is redelivered and would otherwise
    # be counted as uncovered again on every retry, biasing coverage low.
    coverage = math.nan
    if predicts and len(successful_attempts):
        learned = successful_attempts[successful_attempts["receive_count"] == 1]
        if "used_fallback_timeout" in learned:
            learned = learned[~learned["used_fallback_timeout"].astype(bool)]
        if len(learned):
            coverage = float((learned["actual_processing_duration"] <= learned["predicted_processing_duration"]).mean())
    duplicate_seconds = float(df.loc[df["duplicate_processing"], "actual_processing_duration"].sum()) if len(df) else 0.0
    adaptation_time = math.nan
    if predicts and shift_time is not None and not completed.empty:
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
        "prediction_coverage": coverage,
        # The quantity the cost-aware policy minimises with cost ratio 1: seconds
        # of duplicated work plus seconds of failure-recovery delay, per job.
        "tradeoff_cost_s_per_job": (duplicate_seconds + float(recoveries.sum())) / max(completed_jobs, 1),
        "worker_failures": float(len(failures)),
        "worker_hangs": float(sim_result.get("worker_hangs", 0)),
        "mean_hang_recovery_time_s": float(hang_recoveries.mean()) if hang_recoveries.size else math.nan,
        "p95_hang_recovery_time_s": float(np.percentile(hang_recoveries, 95)) if hang_recoveries.size else math.nan,
        "event_limit_reached": float(bool(sim_result["event_limit_reached"])),
        **_steady_state_metrics(sim_result, df, predicts, warmup_fraction),
    }


STEADY_STATE_METRICS = (
    "duplicate_processing_rate",
    "mean_end_to_end_latency_s",
    "p95_end_to_end_latency_s",
    "mean_failure_recovery_time_s",
    "mean_absolute_prediction_error_s",
)


def _steady_state_metrics(
    sim_result: dict[str, Any], df: pd.DataFrame, predicts: bool, warmup_fraction: float
) -> dict[str, float]:
    """Key metrics restricted to jobs sent after the first `warmup_fraction` of jobs.

    Separates cold-start cost (before the adaptive controller has history) from
    steady-state behaviour. Every policy is filtered identically, so the
    comparison stays paired. Columns are prefixed `steady_`.
    """
    if not 0 <= warmup_fraction < 1:
        raise ValueError("warmup_fraction must be in [0, 1)")
    order = sim_result.get("job_ids_in_send_order") or []
    keep = set(order[int(len(order) * warmup_fraction):])
    steady = df[df["job_id"].isin(keep)] if len(df) else df
    committed = steady[steady["committed_result"]] if len(steady) else steady
    successful = steady[steady["successful_completion"]] if len(steady) else steady
    end_to_end = committed["completion_time"] - committed["send_time"] if len(committed) else pd.Series(dtype=float)
    recoveries = np.asarray(
        [t for t, job in zip(sim_result["failure_recoveries"], sim_result.get("failure_recovery_job_ids", [])) if job in keep],
        dtype=float,
    )
    error = (
        (successful["actual_processing_duration"] - successful["predicted_processing_duration"]).abs()
        if predicts and len(successful)
        else pd.Series(dtype=float)
    )
    values = {
        "duplicate_processing_rate": float(steady["duplicate_processing"].mean()) if len(steady) else math.nan,
        "mean_end_to_end_latency_s": float(end_to_end.mean()) if len(end_to_end) else math.nan,
        "p95_end_to_end_latency_s": float(end_to_end.quantile(0.95)) if len(end_to_end) else math.nan,
        "mean_failure_recovery_time_s": float(recoveries.mean()) if recoveries.size else math.nan,
        "mean_absolute_prediction_error_s": float(error.mean()) if len(error) else math.nan,
    }
    return {f"steady_{key}": values[key] for key in STEADY_STATE_METRICS}


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (modified Lentz)."""
    tiny = 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > tiny else tiny)
    h = d
    for m in range(1, 500):
        m2 = 2 * m
        for aa in (
            m * (b - m) * x / ((qam + m2) * (a + m2)),
            -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2)),
        ):
            d = 1.0 + aa * d
            d = 1.0 / (d if abs(d) > tiny else tiny)
            c = 1.0 + aa / c
            c = c if abs(c) > tiny else tiny
            delta = d * c
            h *= delta
        if abs(delta - 1.0) < 1e-15:
            break
    return h


def _betainc(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    log_front = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x)
    if x < (a + 1.0) / (a + b + 2.0):
        return math.exp(log_front) * _betacf(a, b, x) / a
    return 1.0 - math.exp(log_front) * _betacf(b, a, 1.0 - x) / b


def t_critical(confidence: float, df: int) -> float:
    """Two-sided Student-t critical value, computed without SciPy.

    P(|T| > t) = I_{df/(df+t^2)}(df/2, 1/2), which is monotone in x, so the
    quantile is found by bisection on x.
    """
    if not 0 < confidence < 1 or df < 1:
        raise ValueError("confidence must be in (0, 1) and df >= 1")
    alpha = 1.0 - confidence
    lo, hi = 0.0, 1.0
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if _betainc(df / 2.0, 0.5, mid) < alpha:
            lo = mid
        else:
            hi = mid
    x = 0.5 * (lo + hi)
    return math.sqrt(df * (1.0 - x) / x)


def aggregate_runs(
    run_metrics: pd.DataFrame,
    confidence: float = 0.95,
    bootstrap_samples: int = 2000,
    bootstrap_seed: int = 0,
) -> pd.DataFrame:
    """Mean, sample SD, Student-t CI, and percentile-bootstrap CI across seeds.

    The t interval replaces a normal approximation that was far too narrow for
    the small seed counts used here (t_0.975 is 12.7 for two seeds, not 1.96).
    The bootstrap interval makes no normality assumption but is itself crude
    with very few seeds; report both and inspect per-seed values.
    """
    group_cols = [c for c in ["scenario", "policy_name", "safety_margin", "history_window"] if c in run_metrics]
    numeric = [c for c in run_metrics.select_dtypes(include="number").columns if c not in {"seed", "safety_margin", "history_window"}]
    rows: list[dict[str, Any]] = []
    label = f"ci{round(confidence * 100):d}"
    tail = (1.0 - confidence) / 2.0
    rng = np.random.default_rng(bootstrap_seed)
    for keys, frame in run_metrics.groupby(group_cols, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        row = dict(zip(group_cols, keys))
        row["repetitions"] = len(frame)
        for metric in numeric:
            values = frame[metric].dropna().to_numpy(dtype=float)
            n = len(values)
            mean = float(values.mean()) if n else math.nan
            std = float(values.std(ddof=1)) if n > 1 else 0.0 if n else math.nan
            half = t_critical(confidence, n - 1) * std / math.sqrt(n) if n > 1 else 0.0 if n else math.nan
            if n > 1 and bootstrap_samples > 0:
                means = values[rng.integers(0, n, size=(bootstrap_samples, n))].mean(axis=1)
                boot_low, boot_high = (float(q) for q in np.quantile(means, [tail, 1.0 - tail]))
            else:
                boot_low = boot_high = mean
            row[f"{metric}_mean"] = mean
            row[f"{metric}_std"] = std
            row[f"{metric}_{label}_low"] = mean - half if not math.isnan(half) else math.nan
            row[f"{metric}_{label}_high"] = mean + half if not math.isnan(half) else math.nan
            row[f"{metric}_boot_{label}_low"] = boot_low
            row[f"{metric}_boot_{label}_high"] = boot_high
        rows.append(row)
    return pd.DataFrame(rows)

