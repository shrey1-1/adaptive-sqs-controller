"""Paired statistics across seeds, implemented without SciPy.

Every policy in a scenario/seed runs on the same workload and, via common
random numbers, the same failure draws, so policies are compared with paired
tests on per-seed differences rather than by overlapping independent CIs.
"""
from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd

from .metrics import t_critical

PAIR_KEYS = ["scenario", "seed", "safety_margin", "history_window"]

DEFAULT_PAIRED_METRICS = (
    "duplicate_processing_rate",
    "mean_failure_recovery_time_s",
    "mean_end_to_end_latency_s",
    "throughput_jobs_per_s",
    "steady_duplicate_processing_rate",
    "steady_mean_failure_recovery_time_s",
    "steady_mean_end_to_end_latency_s",
)


def _normal_sf(z: float) -> float:
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def _rank_abs(values: np.ndarray) -> np.ndarray:
    """Average ranks (1-based) of |values|, ties sharing their mean rank."""
    order = np.argsort(np.abs(values), kind="mergesort")
    sorted_abs = np.abs(values)[order]
    ranks = np.empty(len(values), dtype=float)
    i = 0
    while i < len(values):
        j = i
        while j + 1 < len(values) and sorted_abs[j + 1] == sorted_abs[i]:
            j += 1
        ranks[order[i : j + 1]] = (i + j) / 2.0 + 1.0
        i = j + 1
    return ranks


def wilcoxon_signed_rank(differences: Iterable[float], zero_tol: float = 1e-12) -> tuple[float, float, int]:
    """Two-sided Wilcoxon signed-rank test on paired differences.

    Zero differences are dropped (Wilcoxon's convention). Without ties and with
    n <= 50 the p-value is exact, from the null distribution of W+ built by
    dynamic programming; otherwise a tie-corrected normal approximation with
    continuity correction is used. Returns (W+, p-value, n used).
    """
    d = np.asarray([x for x in differences if not math.isnan(x)], dtype=float)
    d = d[np.abs(d) > zero_tol]
    n = len(d)
    if n == 0:
        return math.nan, math.nan, 0
    ranks = _rank_abs(d)
    w_plus = float(ranks[d > 0].sum())
    has_ties = len(np.unique(np.abs(d))) < n
    if not has_ties and n <= 50:
        total = n * (n + 1) // 2
        counts = np.zeros(total + 1)
        counts[0] = 1.0
        for r in range(1, n + 1):
            counts[r:] = counts[r:] + counts[: total + 1 - r].copy()
        probabilities = counts / counts.sum()
        w = int(round(w_plus))
        lower = probabilities[: w + 1].sum()
        upper = probabilities[w:].sum()
        return w_plus, float(min(1.0, 2.0 * min(lower, upper))), n
    mean = n * (n + 1) / 4.0
    _, tie_counts = np.unique(ranks, return_counts=True)
    variance = n * (n + 1) * (2 * n + 1) / 24.0 - float((tie_counts**3 - tie_counts).sum()) / 48.0
    if variance <= 0:
        return w_plus, 1.0, n
    z = (abs(w_plus - mean) - 0.5) / math.sqrt(variance)
    return w_plus, float(min(1.0, 2.0 * _normal_sf(max(z, 0.0)))), n


def holm_adjust(p_values: Iterable[float]) -> list[float]:
    """Holm-Bonferroni step-down adjustment; NaNs are passed through."""
    p = list(p_values)
    valid = sorted((value, i) for i, value in enumerate(p) if not math.isnan(value))
    m = len(valid)
    adjusted = [math.nan] * len(p)
    running = 0.0
    for k, (value, i) in enumerate(valid):
        running = max(running, min(1.0, (m - k) * value))
        adjusted[i] = running
    return adjusted


def paired_comparisons(
    runs: pd.DataFrame,
    reference: str = "adaptive_p90",
    metrics: Iterable[str] = DEFAULT_PAIRED_METRICS,
    confidence: float = 0.95,
) -> pd.DataFrame:
    """Compare every policy against `reference` within each scenario.

    `mean_difference` is reference minus comparator, so a negative value means
    the reference is lower on that metric. Holm adjustment is applied across all
    tests in the returned table.
    """
    keys = [k for k in PAIR_KEYS if k in runs.columns]
    group_keys = [k for k in keys if k != "seed"]
    metrics = [m for m in metrics if m in runs.columns]
    rows: list[dict] = []
    if reference not in set(runs["policy_name"]):
        return pd.DataFrame()
    for group, frame in runs.groupby(group_keys, dropna=False):
        group = group if isinstance(group, tuple) else (group,)
        ref = frame[frame["policy_name"] == reference].set_index("seed")
        if ref.empty:
            continue
        for policy in sorted(set(frame["policy_name"]) - {reference}):
            other = frame[frame["policy_name"] == policy].set_index("seed")
            seeds = ref.index.intersection(other.index)
            for metric in metrics:
                diff = (ref.loc[seeds, metric] - other.loc[seeds, metric]).dropna().to_numpy(dtype=float)
                n = len(diff)
                mean = float(diff.mean()) if n else math.nan
                half = t_critical(confidence, n - 1) * float(diff.std(ddof=1)) / math.sqrt(n) if n > 1 else math.nan
                w_plus, p_value, n_nonzero = wilcoxon_signed_rank(diff)
                rows.append(
                    {
                        **dict(zip(group_keys, group)),
                        "reference": reference,
                        "comparator": policy,
                        "metric": metric,
                        "n_pairs": n,
                        "n_nonzero": n_nonzero,
                        "mean_difference": mean,
                        "ci_low": mean - half,
                        "ci_high": mean + half,
                        "reference_better_seeds": int((diff < 0).sum()) if "throughput" not in metric else int((diff > 0).sum()),
                        "wilcoxon_w_plus": w_plus,
                        "p_value": p_value,
                    }
                )
    table = pd.DataFrame(rows)
    if not table.empty:
        table["p_holm"] = holm_adjust(table["p_value"])
    return table


def pareto_optimal(points: pd.DataFrame, x: str, y: str) -> pd.Series:
    """True for rows not dominated on (x, y), where lower is better on both.

    A row is dominated when another row is no worse on both axes and strictly
    better on at least one. Rows with a missing coordinate are never optimal.
    """
    values = points[[x, y]].to_numpy(dtype=float)
    optimal = []
    for i, (xi, yi) in enumerate(values):
        if math.isnan(xi) or math.isnan(yi):
            optimal.append(False)
            continue
        dominated = any(
            (xj <= xi and yj <= yi) and (xj < xi or yj < yi)
            for j, (xj, yj) in enumerate(values)
            if j != i and not (math.isnan(xj) or math.isnan(yj))
        )
        optimal.append(not dominated)
    return pd.Series(optimal, index=points.index)


def primary_tests(runs: pd.DataFrame, hypotheses: Iterable[dict], confidence: float = 0.95) -> pd.DataFrame:
    """Test a small, declared family of hypotheses with Holm correction within it.

    Each hypothesis names a scenario, a reference and a comparator policy, a
    metric, and the expected direction of reference minus comparator
    ("lower" or "higher"). Only the default setting of any sweep column is
    used. `supported` requires the mean difference in the expected direction
    and a Holm-adjusted p below 1 - confidence. Everything outside this family
    is exploratory and is corrected separately in `paired_comparisons`.
    """
    alpha = 1.0 - confidence
    rows: list[dict] = []
    base = runs
    for column in ("safety_margin", "history_window"):
        if column in base:
            base = base[base[column] == base[column].mode().iat[0]]
    for h in hypotheses:
        frame = base[base["scenario"] == h["scenario"]]
        ref = frame[frame["policy_name"] == h["reference"]].set_index("seed")
        other = frame[frame["policy_name"] == h["comparator"]].set_index("seed")
        seeds = ref.index.intersection(other.index)
        metric = h["metric"]
        if metric in frame and len(seeds):
            diff = (ref.loc[seeds, metric] - other.loc[seeds, metric]).dropna().to_numpy(dtype=float)
        else:
            diff = np.asarray([], dtype=float)
        n = len(diff)
        mean = float(diff.mean()) if n else math.nan
        half = t_critical(confidence, n - 1) * float(diff.std(ddof=1)) / math.sqrt(n) if n > 1 else math.nan
        _, p_value, _ = wilcoxon_signed_rank(diff)
        expected = h.get("expected", "lower")
        in_direction = int((diff < 0).sum()) if expected == "lower" else int((diff > 0).sum())
        rows.append(
            {
                "hypothesis": h["id"],
                "declared": h.get("declared", ""),
                "scenario": h["scenario"],
                "reference": h["reference"],
                "comparator": h["comparator"],
                "metric": metric,
                "expected": expected,
                "n_pairs": n,
                "mean_difference": mean,
                "ci_low": mean - half,
                "ci_high": mean + half,
                "seeds_in_expected_direction": in_direction,
                "p_value": p_value,
            }
        )
    table = pd.DataFrame(rows)
    if table.empty:
        return table
    table["p_holm"] = holm_adjust(table["p_value"])
    direction_ok = np.where(table["expected"] == "lower", table["mean_difference"] < 0, table["mean_difference"] > 0)
    table["supported"] = direction_ok & (table["p_holm"] < alpha)
    return table
