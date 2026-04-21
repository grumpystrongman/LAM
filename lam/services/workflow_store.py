from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any, Dict, List

import yaml


class WorkflowStore:
    """Filesystem-backed versioned workflow store for on-prem MVP."""

    def __init__(self, root: str | Path = "data/workflows") -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def save_draft(self, workflow: Dict[str, Any]) -> Path:
        workflow_id = workflow["id"]
        version = workflow["version"]
        path = self.root / workflow_id / f"{version}.draft.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump({"workflow": workflow}, sort_keys=False), encoding="utf-8")
        self._write_manifest(path=path, workflow=workflow, state="draft")
        return path

    def publish(self, workflow: Dict[str, Any], approvers: List[str]) -> Path:
        publication = workflow.setdefault("publication", {})
        if publication.get("two_person_rule", False) and len(set(approvers)) < 2:
            raise ValueError("Two-person rule requires at least two distinct approvers.")
        publication["state"] = "published"
        publication["approved_by"] = list(approvers)
        path = self.root / workflow["id"] / f"{workflow['version']}.published.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(workflow, indent=2), encoding="utf-8")
        self._write_manifest(path=path, workflow=workflow, state="published")
        return path

    def load(self, workflow_id: str, version: str, published_only: bool = True) -> Dict[str, Any]:
        suffix = "published.json" if published_only else "draft.yaml"
        path = self.root / workflow_id / f"{version}.{suffix}"
        if path.suffix == ".json":
            return json.loads(path.read_text(encoding="utf-8"))
        return yaml.safe_load(path.read_text(encoding="utf-8")).get("workflow", {})

    def list_versions(self, workflow_id: str) -> List[Dict[str, Any]]:
        folder = self.root / workflow_id
        if not folder.exists():
            return []
        items: List[Dict[str, Any]] = []
        for path in sorted(folder.glob("*.*")):
            if path.suffix not in {".json", ".yaml"}:
                continue
            if path.name.endswith(".manifest.json"):
                continue
            state = "published" if ".published." in path.name else "draft"
            version = path.name.split(".")[0]
            items.append({"version": version, "state": state, "path": str(path)})
        return items

    def verify_published(self, workflow_id: str, version: str) -> bool:
        path = self.root / workflow_id / f"{version}.published.json"
        manifest_path = self._manifest_path(path)
        if not path.exists() or not manifest_path.exists():
            return False
        workflow = json.loads(path.read_text(encoding="utf-8"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected_hash = manifest.get("workflow_hash", "")
        computed = self._hash_workflow(workflow)
        return expected_hash == computed and manifest.get("state") == "published"

    def _write_manifest(self, path: Path, workflow: Dict[str, Any], state: str) -> None:
        manifest = {
            "workflow_id": workflow.get("id", ""),
            "version": workflow.get("version", ""),
            "state": state,
            "workflow_hash": self._hash_workflow(workflow),
            "artifact": path.name,
        }
        self._manifest_path(path).write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    @staticmethod
    def _hash_workflow(workflow: Dict[str, Any]) -> str:
        canonical = json.dumps(workflow, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _manifest_path(path: Path) -> Path:
        return path.with_name(f"{path.name}.manifest.json")
