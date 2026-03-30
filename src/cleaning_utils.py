from __future__ import annotations

from typing import Dict, Iterable, List, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def iqr_bounds(df: DataFrame, col_name: str, multiplier: float = 3.0) -> Tuple[float, float]:
    q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    return lower, upper


def remove_outliers_iqr(df: DataFrame, col_name: str, multiplier: float = 3.0) -> DataFrame:
    lower, upper = iqr_bounds(df, col_name, multiplier=multiplier)
    return df.filter((F.col(col_name) >= lower) & (F.col(col_name) <= upper))


def apply_iqr_filters(df: DataFrame, cols: Sequence[str], multiplier: float = 3.0) -> DataFrame:
    filtered = df
    for col in cols:
        filtered = remove_outliers_iqr(filtered, col, multiplier=multiplier)
    return filtered


def evaluate_iqr_candidates(
    df: DataFrame,
    cols: Sequence[str],
    multipliers: Iterable[float],
) -> List[Dict[str, float]]:
    baseline_count = df.count()
    results: List[Dict[str, float]] = []

    for mult in multipliers:
        candidate_df = apply_iqr_filters(df, cols, multiplier=mult)
        candidate_count = candidate_df.count()
        removed = baseline_count - candidate_count
        results.append(
            {
                "multiplier": float(mult),
                "rows_after": float(candidate_count),
                "rows_removed": float(removed),
                "retain_ratio": (candidate_count / baseline_count) if baseline_count else 0.0,
                "remove_ratio": (removed / baseline_count) if baseline_count else 0.0,
            }
        )

    return results


def pick_best_multiplier(
    results: Sequence[Dict[str, float]],
    min_retain_ratio: float = 0.9,
) -> Dict[str, float]:
    if not results:
        raise ValueError("results must not be empty")

    qualified = [r for r in results if r["retain_ratio"] >= min_retain_ratio]
    if qualified:
        # Prefer stricter cutoff among valid candidates.
        return min(qualified, key=lambda r: r["multiplier"])

    # Fallback when all candidates remove too much data.
    return max(results, key=lambda r: r["retain_ratio"])
