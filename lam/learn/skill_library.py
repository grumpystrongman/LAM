from __future__ import annotations

import difflib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


class SkillLibrary:
    def __init__(self, root: str | Path = "data/learned_skills") -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def save_skill(self, skill: Dict[str, Any], *, editor_note: str = "", parent_version: str = "") -> Dict[str, Any]:
        skill_id = str(skill.get("skill_id", "") or "learned_skill").strip()
        skill_dir = self.root / skill_id
        versions_dir = skill_dir / "versions"
        versions_dir.mkdir(parents=True, exist_ok=True)
        manifest = self._load_manifest(skill_id)
        next_version = self._next_version(manifest)
        skill_payload = dict(skill)
        skill_payload["version"] = next_version
        version_path = versions_dir / f"{next_version}.json"
        version_path.write_text(json.dumps(skill_payload, indent=2), encoding="utf-8")
        manifest.setdefault("versions", [])
        manifest["skill_id"] = skill_id
        manifest["skill_name"] = str(skill_payload.get("skill_name", ""))
        manifest["topic"] = str(skill_payload.get("topic", ""))
        manifest["updated_at"] = time.time()
        manifest["versions"].append(
            {
                "version": next_version,
                "path": str(version_path.resolve()),
                "created_at": time.time(),
                "editor_note": editor_note,
                "parent_version": parent_version or (manifest["versions"][-1]["version"] if manifest.get("versions") else ""),
                "confidence_score": float(skill_payload.get("confidence_score", 0.0) or 0.0),
            }
        )
        self._save_manifest(skill_id, manifest)
        diff = self.diff_versions(skill_id, manifest["versions"][-2]["version"], next_version) if len(manifest["versions"]) >= 2 else {"summary": "initial version", "unified_diff": ""}
        return {
            "skill_id": skill_id,
            "version": next_version,
            "path": str(version_path.resolve()),
            "manifest_path": str((skill_dir / "manifest.json").resolve()),
            "diff": diff,
            "history": list(manifest.get("versions", []) or []),
        }

    def load_skill(self, skill_id: str, version: str = "") -> Dict[str, Any]:
        manifest = self._load_manifest(skill_id)
        versions = list(manifest.get("versions", []) or [])
        if not versions:
            return {}
        skill = self._load_raw_skill_version(skill_id, version)
        if not skill:
            return {}
        skill["feedback_summary"] = dict(manifest.get("feedback_summary", {}) or {})
        skill["practice_history"] = list(manifest.get("practice_history", []) or [])[-20:]
        skill["refresh_history"] = list(manifest.get("refresh_history", []) or [])[-20:]
        skill["editor_schema"] = self.editor_schema(skill)
        return skill

    def list_skills(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for manifest_path in self.root.glob("*/manifest.json"):
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            versions = list(payload.get("versions", []) or [])
            latest = versions[-1] if versions else {}
            items.append(
                {
                    "skill_id": str(payload.get("skill_id", "")),
                    "skill_name": str(payload.get("skill_name", "")),
                    "topic": str(payload.get("topic", "")),
                    "latest_version": str(latest.get("version", "")),
                    "updated_at": float(payload.get("updated_at", 0.0) or 0.0),
                    "feedback_summary": dict(payload.get("feedback_summary", {}) or {}),
                    "practice_history_count": len(list(payload.get("practice_history", []) or [])),
                    "refresh_history_count": len(list(payload.get("refresh_history", []) or [])),
                }
            )
        items.sort(key=lambda item: float(item.get("updated_at", 0.0) or 0.0), reverse=True)
        return items

    def list_versions(self, skill_id: str) -> List[Dict[str, Any]]:
        return list(self._load_manifest(skill_id).get("versions", []) or [])

    def diff_versions(self, skill_id: str, left_version: str, right_version: str) -> Dict[str, Any]:
        left = self._load_raw_skill_version(skill_id, left_version)
        right = self._load_raw_skill_version(skill_id, right_version)
        left_lines = json.dumps(left, indent=2, sort_keys=True).splitlines()
        right_lines = json.dumps(right, indent=2, sort_keys=True).splitlines()
        diff_lines = list(difflib.unified_diff(left_lines, right_lines, fromfile=left_version or "left", tofile=right_version or "right", lineterm=""))
        return {
            "skill_id": skill_id,
            "left_version": left_version,
            "right_version": right_version,
            "summary": f"{len(diff_lines)} diff lines",
            "unified_diff": "\n".join(diff_lines),
        }

    def record_feedback(self, skill_id: str, version: str, *, rating: int, comment: str = "", signal: str = "") -> Dict[str, Any]:
        manifest = self._load_manifest(skill_id)
        feedback = list(manifest.get("feedback", []) or [])
        feedback.append(
            {
                "version": version,
                "rating": int(rating),
                "comment": comment[:500],
                "signal": signal[:120],
                "timestamp": time.time(),
            }
        )
        manifest["feedback"] = feedback[-100:]
        manifest["feedback_summary"] = self._feedback_summary(manifest["feedback"])
        self._save_manifest(skill_id, manifest)
        return dict(manifest["feedback_summary"])

    def build_refresh_plan(self, skill_id: str, version: str, *, reason: str = "", source_url: str = "") -> Dict[str, Any]:
        skill = self.load_skill(skill_id, version)
        topic = str(skill.get("topic", ""))
        source_urls = list(skill.get("source_urls", []) or [])
        refresh_policy = dict(skill.get("refresh_policy", {}) or {})
        adapter_summary = dict(skill.get("source_adapter_summary", {}) or {})
        days = int(refresh_policy.get("preferred_refresh_window_days", 30) or 30)
        return {
            "skill_id": skill_id,
            "version": version,
            "topic": topic,
            "reason": reason or "version_sensitive_topic_refresh",
            "seed_url": source_url or (source_urls[0] if source_urls else ""),
            "recommended_instruction": f"Learn {topic}. Refresh the learned skill using updated related videos and supporting sources.",
            "refresh_window_days": days,
            "live_source_recommended": bool(adapter_summary.get("version_sensitive_count", 0)),
            "official_source_target": max(1, int(adapter_summary.get("official_source_count", 0) or 0)),
            "source_adapter_summary": adapter_summary,
        }

    def practice_schedule_payload(self, skill_id: str, version: str) -> Dict[str, Any]:
        skill = self.load_skill(skill_id, version)
        topic = str(skill.get("topic", ""))
        name = f"Practice {topic}"[:80]
        automation_name = f"learn_skill_practice:{skill_id}:{version}"
        instruction = (
            f"Practice the learned skill '{str(skill.get('skill_name', skill_id))}' for topic '{topic}' safely. "
            "Validate prerequisites, simulate harmless steps only, and stop before destructive or production actions."
        )
        return {
            "name": name,
            "automation_name": automation_name,
            "instruction": instruction,
            "skill_id": skill_id,
            "version": version or str(skill.get("version", "")),
            "checkpoint_policy": dict(skill.get("practice_policy", {}) or {}),
            "next_review_at": str(skill.get("next_review_at", "")),
        }

    def record_practice_run(self, skill_id: str, version: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        manifest = self._load_manifest(skill_id)
        history = list(manifest.get("practice_history", []) or [])
        history.append(
            {
                "version": version,
                "timestamp": time.time(),
                "ok": bool(payload.get("ok", False)),
                "checkpoint_count": int(payload.get("checkpoint_count", 0) or 0),
                "failed_checkpoint_id": str(payload.get("failed_checkpoint_id", "") or ""),
                "failed_checkpoint_name": str(payload.get("failed_checkpoint_name", "") or ""),
            }
        )
        manifest["practice_history"] = history[-100:]
        self._save_manifest(skill_id, manifest)
        return {"count": len(manifest["practice_history"]), "latest": manifest["practice_history"][-1]}

    def record_refresh_run(self, skill_id: str, version: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        manifest = self._load_manifest(skill_id)
        history = list(manifest.get("refresh_history", []) or [])
        history.append(
            {
                "version": version,
                "timestamp": time.time(),
                "status": str(payload.get("status", "") or ""),
                "selected_sources": int(payload.get("selected_sources", 0) or 0),
                "runtime_quality": str(payload.get("runtime_quality", "") or ""),
            }
        )
        manifest["refresh_history"] = history[-100:]
        self._save_manifest(skill_id, manifest)
        return {"count": len(manifest["refresh_history"]), "latest": manifest["refresh_history"][-1]}

    def editor_schema(self, skill: Dict[str, Any]) -> Dict[str, Any]:
        workflow = [dict(item) for item in list(skill.get("workflow", []) or []) if isinstance(item, dict)]
        return {
            "fields": [
                {"id": "skill_name", "label": "Skill Name", "type": "text", "value": str(skill.get("skill_name", ""))},
                {"id": "topic", "label": "Topic", "type": "text", "value": str(skill.get("topic", ""))},
                {"id": "purpose", "label": "Purpose", "type": "text", "value": str(skill.get("purpose", ""))},
                {"id": "domain", "label": "Domain", "type": "text", "value": str(skill.get("domain", ""))},
                {"id": "confidence_score", "label": "Confidence", "type": "number", "value": str(skill.get("confidence_score", ""))},
                {"id": "required_tools", "label": "Required Tools", "type": "list", "value": ", ".join(list(skill.get("required_tools", []) or []))},
                {"id": "prerequisites", "label": "Prerequisites", "type": "list", "value": ", ".join(list(skill.get("prerequisites", []) or []))},
                {"id": "safety_gates", "label": "Safety Gates", "type": "list", "value": " | ".join(list(skill.get("safety_gates", []) or []))},
                {"id": "validation_checks", "label": "Validation Checks", "type": "list", "value": " | ".join(list(skill.get("validation_checks", []) or []))},
            ],
            "workflow_preview": [
                {
                    "step": int(item.get("step", 0) or 0),
                    "description": str(item.get("description", "")),
                    "action_type": str(item.get("action_type", "")),
                    "checkpoint_name": str(item.get("checkpoint_name", "")),
                }
                for item in workflow[:12]
            ],
        }

    def _load_manifest(self, skill_id: str) -> Dict[str, Any]:
        path = self.root / skill_id / "manifest.json"
        if not path.exists():
            return {"skill_id": skill_id, "versions": [], "feedback": [], "feedback_summary": {}, "practice_history": [], "refresh_history": []}
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_raw_skill_version(self, skill_id: str, version: str) -> Dict[str, Any]:
        manifest = self._load_manifest(skill_id)
        versions = list(manifest.get("versions", []) or [])
        if not versions:
            return {}
        chosen = next((item for item in versions if str(item.get("version", "")) == version), versions[-1])
        path = Path(str(chosen.get("path", "")))
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_manifest(self, skill_id: str, manifest: Dict[str, Any]) -> None:
        skill_dir = self.root / skill_id
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def _next_version(self, manifest: Dict[str, Any]) -> str:
        versions = list(manifest.get("versions", []) or [])
        if not versions:
            return "1.0"
        latest = str(versions[-1].get("version", "1.0"))
        try:
            major, minor = latest.split(".", 1)
            return f"{int(major)}.{int(minor) + 1}"
        except Exception:
            return f"{latest}.1"

    def _feedback_summary(self, feedback: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not feedback:
            return {"count": 0, "average_rating": 0.0, "signals": []}
        avg = round(sum(int(item.get("rating", 0) or 0) for item in feedback) / max(1, len(feedback)), 2)
        signals = sorted({str(item.get("signal", "")) for item in feedback if str(item.get("signal", ""))})
        return {"count": len(feedback), "average_rating": avg, "signals": signals[:8]}
