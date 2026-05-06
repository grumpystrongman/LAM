from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List

from .artifact_specific_critics import ArtifactCriticResult
from .work_product_engine import WorkProductEngine


class RevisionRuntime:
    def __init__(self, *, engine: WorkProductEngine | None = None, max_revisions: int = 2) -> None:
        self.engine = engine or WorkProductEngine()
        self.max_revisions = max(0, int(max_revisions))

    def revise_until_pass(
        self,
        *,
        artifact_key: str,
        artifact_path: str | Path,
        critic_name: str,
        evaluate: Callable[[str], ArtifactCriticResult],
    ) -> Dict[str, Any]:
        path = Path(artifact_path)
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        history: List[Dict[str, Any]] = []
        attempt = 0
        result = evaluate(text)
        history.append({"attempt": attempt, "critic": critic_name, **result.to_dict()})
        while not result.passed and result.auto_repair_allowed and attempt < self.max_revisions:
            attempt += 1
            self.engine.revise_artifact(artifact_path=path, instructions=list(result.revision_instructions))
            text = path.read_text(encoding="utf-8") if path.exists() else ""
            result = evaluate(text)
            history.append({"attempt": attempt, "critic": critic_name, **result.to_dict()})
        return {
            "artifact_key": artifact_key,
            "artifact_path": str(path.resolve()),
            "critic": critic_name,
            "passed": bool(result.passed),
            "attempts": attempt,
            "history": history,
            "final_result": result.to_dict(),
            "content": text,
        }
