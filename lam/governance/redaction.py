from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass(slots=True)
class Detection:
    kind: str
    match: str


class Redactor:
    """Redacts potential PHI/PII before persistence."""

    DETECTOR_VERSION = "2026.04.20"

    def __init__(self) -> None:
        self._patterns: Dict[str, re.Pattern[str]] = {
            "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
            "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
            "phone": re.compile(r"\b(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
            "dob": re.compile(r"\b(?:19|20)\d{2}[-/](?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01])\b"),
            "mrn": re.compile(r"\b(?:MRN|Patient\s*ID)[:\s#-]*[A-Z0-9]{5,16}\b", re.IGNORECASE),
            "address": re.compile(
                r"\b\d{1,6}\s+[A-Za-z0-9.\s]{2,40}\s(?:St|Street|Ave|Avenue|Rd|Road|Blvd|Lane|Ln|Dr|Drive)\b",
                re.IGNORECASE,
            ),
        }

    def detect(self, text: str) -> List[Detection]:
        detections: List[Detection] = []
        for kind, pattern in self._patterns.items():
            for match in pattern.findall(text):
                detections.append(Detection(kind=kind, match=match))
        return detections

    def mask_text(self, text: str) -> Tuple[str, List[Detection]]:
        detections = self.detect(text)
        masked = text
        for detection in detections:
            masked = masked.replace(detection.match, f"<REDACTED:{detection.kind.upper()}>")
        return masked, detections

    def redact_for_persistence(self, payload: Any) -> Tuple[Any, Dict[str, Any]]:
        """Recursively redact payload and return metadata for audit."""
        detections: List[Detection] = []
        masked_count = 0

        def _walk(value: Any) -> Any:
            nonlocal masked_count
            if isinstance(value, str):
                masked, found = self.mask_text(value)
                detections.extend(found)
                masked_count += len(found)
                return masked
            if isinstance(value, list):
                return [_walk(item) for item in value]
            if isinstance(value, dict):
                return {key: _walk(item) for key, item in value.items()}
            return value

        clean_payload = _walk(copy.deepcopy(payload))
        total = len(detections)
        confidence = 1.0 if total == 0 else min(1.0, masked_count / float(total))

        detector_counts: Dict[str, int] = {}
        for detection in detections:
            detector_counts[detection.kind] = detector_counts.get(detection.kind, 0) + 1

        metadata = {
            "confidence": round(confidence, 4),
            "detectors": sorted(detector_counts.keys()),
            "detector_counts": detector_counts,
            "detector_version": self.DETECTOR_VERSION,
            "total_detections": total,
        }
        return clean_payload, metadata

