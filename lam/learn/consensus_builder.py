from __future__ import annotations

from typing import Dict, List


def build_consensus_workflow(all_steps: List[Dict[str, object]]) -> List[Dict[str, object]]:
    buckets: Dict[str, Dict[str, object]] = {}
    for row in all_steps:
        text = str(row.get("description", "") or "").strip()
        if not text:
            continue
        key = _normalize(text)
        entry = buckets.setdefault(
            key,
            {
                "description": text,
                "action_type": str(row.get("action_type", "workflow") or "workflow"),
                "support_count": 0,
                "supporting_sources": [],
                "timestamp_refs": [],
                "confidence": 0.0,
                "risk_level": str(row.get("risk_level", "low") or "low"),
                "approval_required": bool(row.get("approval_required", False)),
            },
        )
        entry["support_count"] = int(entry.get("support_count", 0) or 0) + 1
        entry["supporting_sources"] = list(dict.fromkeys(list(entry.get("supporting_sources", []) or []) + list(row.get("supporting_sources", []) or [])))
        entry["timestamp_refs"] = list(dict.fromkeys(list(entry.get("timestamp_refs", []) or []) + list(row.get("timestamp_refs", []) or [])))
        entry["confidence"] = max(float(entry.get("confidence", 0.0) or 0.0), float(row.get("confidence", 0.0) or 0.0))
    consensus = list(buckets.values())
    consensus.sort(key=lambda item: (-int(item.get("support_count", 0) or 0), str(item.get("description", ""))))
    return consensus[:16]


def _normalize(text: str) -> str:
    low = text.lower()
    for token in [",", ".", ";", ":"]:
        low = low.replace(token, "")
    return " ".join(low.split()[:8])
