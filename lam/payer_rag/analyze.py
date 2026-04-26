from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from pathlib import Path


def read_csv_rows(path: str | Path) -> list[dict]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _float_or_none(value: str | float | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def build_lookup(rows: list[dict], key: str) -> dict[str, dict]:
    return {row[key]: row for row in rows}


def is_comparable_service(service: dict) -> bool:
    code_type = (service.get("code_type") or "").upper()
    description = (service.get("description") or "").lower()
    excluded_terms = (
        "device ",
        "cover ",
        "binder ",
        "guidewire",
        "catheter",
        "tablet",
        "(ea)",
    )
    if code_type == "RC":
        return False
    if any(term in description for term in excluded_terms):
        return False
    return True


def analyze_outliers(
    *,
    rates: list[dict],
    plans: list[dict],
    payers: list[dict],
    services: list[dict],
    outlier_threshold: float = 0.2,
    min_peer_count: int = 3,
) -> dict[str, list[dict]]:
    plan_lookup = build_lookup(plans, "plan_id")
    payer_lookup = build_lookup(payers, "payer_id")
    service_lookup = build_lookup(services, "service_id")
    grouped: dict[tuple[str, str, str, str], list[dict]] = defaultdict(list)
    for rate in rates:
        negotiated = _float_or_none(rate.get("negotiated_rate"))
        if negotiated is None:
            continue
        group_key = (
            rate.get("service_id", ""),
            rate.get("facility_name", ""),
            rate.get("setting", ""),
            rate.get("billing_class", ""),
        )
        grouped[group_key].append(rate)

    candidates: list[dict] = []
    payer_rollup: dict[str, dict] = defaultdict(
        lambda: {
            "payer_name": "",
            "number_of_plans": 0,
            "number_of_services_analyzed": 0,
            "number_of_outlier_flags": 0,
            "average_variance": 0.0,
            "top_concern": "",
            "confidence": 0.0,
        }
    )

    for group_rows in grouped.values():
        rates_only = [_float_or_none(row.get("negotiated_rate")) for row in group_rows]
        numeric_rates = [rate for rate in rates_only if rate is not None]
        if len(numeric_rates) < min_peer_count:
            continue
        peer_median = statistics.median(numeric_rates)
        peer_min = min(numeric_rates)
        peer_max = max(numeric_rates)
        for row in group_rows:
            negotiated = _float_or_none(row.get("negotiated_rate"))
            if negotiated is None or peer_median == 0:
                continue
            variance = (negotiated - peer_median) / peer_median
            if variance <= outlier_threshold:
                continue
            plan = plan_lookup.get(row["plan_id"], {})
            payer = payer_lookup.get(row["payer_id"], {})
            service = service_lookup.get(row["service_id"], {})
            if not is_comparable_service(service):
                continue
            confidence = min(
                0.99,
                round(
                    ((_float_or_none(row.get("confidence")) or 0.7) * 0.7)
                    + (min(len(numeric_rates), 8) / 20.0)
                    + 0.1,
                    2,
                ),
            )
            reason = (
                f"Negotiated rate appears {variance * 100:.1f}% above the peer median "
                f"for comparable {service.get('description', 'service')} rates at {row.get('facility_name', '')}."
            )
            evidence = (
                f"{row.get('source_url', '')} | {row.get('raw_reference', '')} | "
                f"median={peer_median:.2f} | facility={row.get('facility_name', '')}"
            )
            candidates.append(
                {
                    "priority_rank": 0,
                    "payer_name": payer.get("payer_name", row["payer_id"]),
                    "plan_name": plan.get("plan_name", row["plan_id"]),
                    "service": service.get("description", row["service_id"]),
                    "code": service.get("code", ""),
                    "payer_rate": negotiated,
                    "peer_median": peer_median,
                    "peer_min": peer_min,
                    "peer_max": peer_max,
                    "variance_percent": variance,
                    "reason_flagged": reason,
                    "confidence": confidence,
                    "recommended_action": (
                        "Validate contract terms and sample claims, then consider payer outreach if the "
                        "higher rate is confirmed and operationally meaningful."
                    ),
                    "source_evidence": evidence,
                    "compared_service": service.get("description", row["service_id"]),
                    "facility_name": row.get("facility_name", ""),
                }
            )
            summary = payer_rollup[payer.get("payer_name", row["payer_id"])]
            summary["payer_name"] = payer.get("payer_name", row["payer_id"])
            summary["number_of_outlier_flags"] += 1
            summary["number_of_services_analyzed"] += 1
            summary["average_variance"] += variance
            if not summary["top_concern"] or variance > summary.get("_top_variance", 0):
                summary["top_concern"] = f"{service.get('description', '')} at {row.get('facility_name', '')}"
                summary["_top_variance"] = variance
            summary["confidence"] = max(summary["confidence"], confidence)

    for plan in plans:
        payer = payer_lookup.get(plan["payer_id"], {})
        summary = payer_rollup[payer.get("payer_name", plan["payer_id"])]
        summary["payer_name"] = payer.get("payer_name", plan["payer_id"])
        summary["number_of_plans"] += 1

    ordered = sorted(
        candidates,
        key=lambda row: (
            -row["variance_percent"],
            -row["confidence"],
            row["payer_name"],
            row["plan_name"],
        ),
    )
    for idx, row in enumerate(ordered, start=1):
        row["priority_rank"] = idx

    payer_summary: list[dict] = []
    for row in payer_rollup.values():
        if row["number_of_outlier_flags"]:
            row["average_variance"] = row["average_variance"] / row["number_of_outlier_flags"]
        payer_summary.append(
            {
                "payer_name": row["payer_name"],
                "number_of_plans": row["number_of_plans"],
                "number_of_services_analyzed": row["number_of_services_analyzed"],
                "number_of_outlier_flags": row["number_of_outlier_flags"],
                "average_variance": row["average_variance"],
                "top_concern": row["top_concern"],
                "confidence": row["confidence"],
            }
        )
    payer_summary.sort(key=lambda item: (-item["number_of_outlier_flags"], -item["average_variance"], item["payer_name"]))
    return {"candidates": ordered, "payer_summary": payer_summary}
