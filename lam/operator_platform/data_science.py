from __future__ import annotations

import math
import statistics
from typing import Any, Dict, Iterable, List


def _numeric_values(rows: Iterable[Dict[str, Any]], field: str) -> List[float]:
    values: List[float] = []
    for row in rows:
        try:
            value = row.get(field)
            if value in {"", None}:
                continue
            values.append(float(value))
        except Exception:
            continue
    return values


def data_profile(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    fields = sorted({key for row in rows for key in row.keys()})
    return {"row_count": len(rows), "field_count": len(fields), "fields": fields}


def missing_value_report(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    fields = sorted({key for row in rows for key in row.keys()})
    report: Dict[str, Any] = {"row_count": len(rows), "missing_by_field": {}}
    for field in fields:
        missing = sum(1 for row in rows if row.get(field) in {"", None})
        report["missing_by_field"][field] = {"missing": missing, "missing_ratio": (missing / len(rows)) if rows else 0.0}
    return report


def descriptive_statistics(rows: List[Dict[str, Any]], numeric_fields: List[str]) -> Dict[str, Any]:
    stats: Dict[str, Any] = {}
    for field in numeric_fields:
        values = _numeric_values(rows, field)
        if not values:
            continue
        stats[field] = {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "mean": statistics.fmean(values),
            "median": statistics.median(values),
        }
    return stats


def detect_outliers(rows: List[Dict[str, Any]], field: str, multiplier: float = 1.5) -> List[Dict[str, Any]]:
    values = sorted(_numeric_values(rows, field))
    if len(values) < 4:
        return []
    q1 = statistics.median(values[: len(values) // 2])
    q3 = statistics.median(values[(len(values) + 1) // 2 :])
    iqr = q3 - q1
    upper = q3 + multiplier * iqr
    lower = q1 - multiplier * iqr
    return [row for row in rows if row.get(field) not in {"", None} and (float(row[field]) > upper or float(row[field]) < lower)]


def correlation_analysis(rows: List[Dict[str, Any]], x_field: str, y_field: str) -> Dict[str, Any]:
    xs = _numeric_values(rows, x_field)
    ys = _numeric_values(rows, y_field)
    size = min(len(xs), len(ys))
    if size < 2:
        return {"correlation": 0.0, "sample_size": size}
    xs = xs[:size]
    ys = ys[:size]
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denom_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    denom_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    corr = numerator / (denom_x * denom_y) if denom_x and denom_y else 0.0
    return {"correlation": corr, "sample_size": size}


def trend_analysis(rows: List[Dict[str, Any]], x_field: str, y_field: str) -> Dict[str, Any]:
    regression = simple_regression(rows, x_field, y_field)
    slope = regression.get("slope", 0.0)
    direction = "flat"
    if slope > 0:
        direction = "up"
    elif slope < 0:
        direction = "down"
    return {"direction": direction, "slope": slope, "sample_size": regression.get("sample_size", 0)}


def cohort_group_comparison(rows: List[Dict[str, Any]], group_field: str, value_field: str) -> Dict[str, Any]:
    grouped: Dict[str, List[float]] = {}
    for row in rows:
        key = str(row.get(group_field, "unknown"))
        try:
            value = float(row.get(value_field))
        except Exception:
            continue
        grouped.setdefault(key, []).append(value)
    return {key: {"count": len(values), "mean": statistics.fmean(values)} for key, values in grouped.items() if values}


def simple_regression(rows: List[Dict[str, Any]], x_field: str, y_field: str) -> Dict[str, Any]:
    xs = _numeric_values(rows, x_field)
    ys = _numeric_values(rows, y_field)
    size = min(len(xs), len(ys))
    if size < 2:
        return {"slope": 0.0, "intercept": 0.0, "sample_size": size}
    xs = xs[:size]
    ys = ys[:size]
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    ss_xx = sum((x - mean_x) ** 2 for x in xs)
    if ss_xx == 0:
        return {"slope": 0.0, "intercept": mean_y, "sample_size": size}
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / ss_xx
    intercept = mean_y - slope * mean_x
    return {"slope": slope, "intercept": intercept, "sample_size": size}


def chart_recommendation(row_count: int, field_names: List[str]) -> Dict[str, Any]:
    if row_count <= 1:
        return {"chart_type": "table", "reason": "Not enough rows for a chart."}
    if len(field_names) >= 3:
        return {"chart_type": "bar", "reason": "Best for categorical comparison in first pass."}
    return {"chart_type": "line", "reason": "Good default for trend-oriented numeric data."}


def generate_chart_spec(chart_type: str, x_field: str, y_field: str, title: str) -> Dict[str, Any]:
    return {"chart_type": chart_type, "x_field": x_field, "y_field": y_field, "title": title}


def insight_generation(profile: Dict[str, Any], stats: Dict[str, Any], outliers: List[Dict[str, Any]]) -> List[str]:
    insights = [f"Dataset contains {profile.get('row_count', 0)} row(s) across {profile.get('field_count', 0)} field(s)."]
    for field, payload in stats.items():
        insights.append(f"{field} median is {payload.get('median')}.")
    if outliers:
        insights.append(f"Detected {len(outliers)} potential outlier row(s).")
    return insights


def caveat_generation(profile: Dict[str, Any], missing_report: Dict[str, Any]) -> List[str]:
    caveats: List[str] = []
    if profile.get("row_count", 0) < 5:
        caveats.append("Small sample size limits confidence.")
    for field, payload in (missing_report.get("missing_by_field", {}) or {}).items():
        if float(payload.get("missing_ratio", 0.0) or 0.0) > 0.25:
            caveats.append(f"{field} has material missingness.")
    return caveats
