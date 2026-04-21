from __future__ import annotations

import ast
from types import SimpleNamespace
from typing import Any, Dict, List

from lam.dsl.schema import STEP_REQUIRED_FIELDS, SUPPORTED_STEP_TYPES, WORKFLOW_REQUIRED_FIELDS


def validate_workflow(workflow: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    for key in WORKFLOW_REQUIRED_FIELDS:
        if key not in workflow:
            errors.append(f"missing_workflow_field:{key}")

    publication = workflow.get("publication", {})
    if "state" not in publication:
        errors.append("missing_publication_state")
    if (
        publication.get("state") == "published"
        and publication.get("two_person_rule")
        and len(publication.get("approved_by", [])) < 2
    ):
        errors.append("two_person_rule_requires_two_approvers")

    steps = workflow.get("steps", [])
    if not isinstance(steps, list) or not steps:
        errors.append("steps_must_be_non_empty_list")
        return errors

    seen_ids = set()
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            errors.append(f"step_{index}_must_be_object")
            continue
        for key in STEP_REQUIRED_FIELDS:
            if key not in step:
                errors.append(f"step_{index}_missing_field:{key}")
        step_id = step.get("id")
        if step_id in seen_ids:
            errors.append(f"duplicate_step_id:{step_id}")
        seen_ids.add(step_id)
        if step.get("type") not in SUPPORTED_STEP_TYPES:
            errors.append(f"unsupported_step_type:{step.get('type')}")
        if "sensitivity" in step:
            sensitivity = step["sensitivity"]
            if sensitivity.get("write_impact") not in {None, "read", "write", "submit"}:
                errors.append(f"invalid_write_impact:{step_id}")
            if sensitivity.get("data_classification") not in {None, "none", "phi", "pii", "credential", "internal"}:
                errors.append(f"invalid_data_classification:{step_id}")
    return errors


def evaluate_condition(expression: str, runtime_state: Dict[str, Any]) -> bool:
    """
    Safe evaluator for simple conditions used by `if` steps.
    Supports bool, compare, names, constants, and dotted access via runtime_state.
    """

    def _to_namespace(value: Any) -> Any:
        if isinstance(value, dict):
            return SimpleNamespace(**{k: _to_namespace(v) for k, v in value.items()})
        if isinstance(value, list):
            return [_to_namespace(item) for item in value]
        return value

    class _Resolver(dict):
        def __missing__(self, key: str) -> Any:
            return _to_namespace(runtime_state.get(key))

    node = ast.parse(expression, mode="eval")
    for child in ast.walk(node):
        if not isinstance(
            child,
            (
                ast.Expression,
                ast.Compare,
                ast.Name,
                ast.Load,
                ast.Constant,
                ast.BoolOp,
                ast.And,
                ast.Or,
                ast.UnaryOp,
                ast.Not,
                ast.Eq,
                ast.NotEq,
                ast.Gt,
                ast.GtE,
                ast.Lt,
                ast.LtE,
                ast.Attribute,
            ),
        ):
            raise ValueError(f"Unsupported expression element: {type(child).__name__}")

    return bool(eval(compile(node, "<condition>", "eval"), {"__builtins__": {}}, _Resolver()))
