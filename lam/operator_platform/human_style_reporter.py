from __future__ import annotations

from typing import Any, Dict, Iterable, List


class HumanStyleReporter:
    def build(
        self,
        *,
        task_contract: Dict[str, Any],
        execution_graph: Dict[str, Any],
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        outputs = sorted((result.get("artifacts") or {}).keys())[:6]
        capability_names = [str(node.get("capability", "")) for node in execution_graph.get("nodes", [])[:6]]
        summary_lines: List[str] = []
        if str(task_contract.get("geography", "")).strip():
            summary_lines.append(f"Handled as a {task_contract.get('geography')} task.")
        if capability_names:
            summary_lines.append(f"Used capabilities: {', '.join(capability_names)}.")
        if outputs:
            summary_lines.append(f"Outputs available: {', '.join(outputs)}.")
        if not summary_lines:
            summary_lines.append("Task executed with reusable platform capabilities.")
        next_steps = self._next_steps(task_contract=task_contract, outputs=outputs)
        return {
            "summary": " ".join(summary_lines),
            "outputs": outputs,
            "next_steps": next_steps,
        }

    def _next_steps(self, *, task_contract: Dict[str, Any], outputs: Iterable[str]) -> List[str]:
        domain = str(task_contract.get("domain", ""))
        items = list(outputs)
        if domain == "payer_pricing_review":
            return ["Validate flagged rows with contracting data before outreach.", "Review the spreadsheet and dashboard together."]
        if "presentation" in items:
            return ["Review the slide outline before generating a final deck."]
        if "code" in items or domain == "deep_analysis":
            return ["Extend the scaffold with task-specific logic.", "Run the smoke tests after each change."]
        return ["Open the artifacts in Canvas and review the evidence."]
