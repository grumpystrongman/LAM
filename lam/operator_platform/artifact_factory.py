from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


class ArtifactFactory:
    def __init__(self, manifests_root: str | Path = "data/operator_platform/manifests") -> None:
        self.manifests_root = Path(manifests_root)
        self.manifests_root.mkdir(parents=True, exist_ok=True)

    def write_manifest(
        self,
        *,
        task_id: str,
        task_contract: Dict[str, Any],
        artifacts: Dict[str, Any],
        generated_by_capabilities: Iterable[str],
        validation_status: str,
        source_data: Iterable[str],
    ) -> Path:
        target = self._manifest_path(task_id=task_id, artifacts=artifacts)
        payload = {
            "task_id": task_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "geography": str(task_contract.get("geography", "")),
            "domain": str(task_contract.get("domain", "")),
            "source_data": [str(x) for x in source_data],
            "generated_by_capabilities": [str(x) for x in generated_by_capabilities],
            "validation_status": validation_status or "unknown",
            "task_contract": dict(task_contract),
            "artifacts": {k: v for k, v in artifacts.items() if isinstance(v, str)},
        }
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return target

    def validate_manifest(self, path: str | Path) -> Tuple[bool, List[str]]:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        errors: List[str] = []
        for key in ["task_id", "created_at", "domain", "generated_by_capabilities", "validation_status", "artifacts"]:
            if key not in payload:
                errors.append(f"missing:{key}")
        if not isinstance(payload.get("artifacts", {}), dict) or not payload.get("artifacts"):
            errors.append("missing:artifacts")
        return (len(errors) == 0, errors)

    def _manifest_path(self, *, task_id: str, artifacts: Dict[str, Any]) -> Path:
        locations = [str(v) for v in artifacts.values() if isinstance(v, str) and str(v).strip()]
        if not locations:
            return self.manifests_root / f"{task_id}_artifact_manifest.json"
        candidate = Path(locations[0])
        cwd = Path.cwd().resolve()
        try:
            if candidate.resolve().anchor and os.path.commonpath([str(cwd), str(candidate.resolve())]) != str(cwd):
                return self.manifests_root / f"{task_id}_artifact_manifest.json"
        except Exception:
            return self.manifests_root / f"{task_id}_artifact_manifest.json"
        if candidate.is_dir():
            return candidate / "artifact_manifest.json"
        if "artifacts" in candidate.parts:
            idx = candidate.parts.index("artifacts")
            root = Path(*candidate.parts[: idx + 1])
            return root / "artifact_manifest.json"
        return candidate.parent / "artifact_manifest.json"
