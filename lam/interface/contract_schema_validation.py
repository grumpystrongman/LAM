from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


_SCHEMA_FILES = {
    "task_envelope": "task-envelope.json",
    "plan_contract": "plan.json",
    "execution_trace": "execution-trace.json",
    "verification_report": "verification-report.json",
    "final_report": "final-report.json",
}


def _schema_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "schemas"


def _load_schema(file_name: str) -> Dict[str, Any]:
    path = _schema_dir() / file_name
    return json.loads(path.read_text(encoding="utf-8-sig"))


def validate_contract_objects(contract_objects: Dict[str, Any]) -> Tuple[bool, List[str]]:
    try:
        from jsonschema import Draft202012Validator
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return False, [f"jsonschema runtime unavailable: {exc}"]

    errors: List[str] = []
    for key, schema_file in _SCHEMA_FILES.items():
        payload = contract_objects.get(key, None)
        if payload is None:
            errors.append(f"{key}: missing payload")
            continue
        try:
            schema = _load_schema(schema_file)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            errors.append(f"{key}: failed to load schema {schema_file}: {exc}")
            continue
        validator = Draft202012Validator(schema)
        for err in sorted(validator.iter_errors(payload), key=lambda e: list(e.path)):
            p = ".".join(str(x) for x in err.path) or "<root>"
            errors.append(f"{key}:{p}: {err.message}")
    return len(errors) == 0, errors
