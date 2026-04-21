from __future__ import annotations

import copy
from dataclasses import asdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from lam.dsl.validator import evaluate_condition, validate_workflow
from lam.governance.approval_client import ApprovalClient
from lam.governance.audit_logger import AuditLogger
from lam.governance.policy_engine import PolicyDecision, PolicyEngine


class NeedUserInputError(RuntimeError):
    pass


@dataclass(slots=True)
class RunResult:
    status: str
    executed_steps: int
    blocked_step_id: str = ""
    errors: List[str] = field(default_factory=list)


class Runner:
    """
    Deterministic DSL runner.
    Policy is evaluated before every step. Sensitive actions block on approvals.
    """

    def __init__(
        self,
        policy_engine: PolicyEngine,
        approval_client: ApprovalClient,
        audit_logger: AuditLogger,
        adapters: Dict[str, Any],
        kill_switch: Any,
        ask_user_handler: Optional[Callable[[str, List[str], Dict[str, Any]], str]] = None,
    ) -> None:
        self.policy = policy_engine
        self.approval = approval_client
        self.audit = audit_logger
        self.adapters = adapters
        self.kill_switch = kill_switch
        self.ask_user_handler = ask_user_handler
        self.runtime_state: Dict[str, Any] = {}

    def run(self, workflow: Dict[str, Any], identity_ctx: Dict[str, Any]) -> RunResult:
        errors = validate_workflow(workflow)
        if errors:
            return RunResult(status="invalid_workflow", executed_steps=0, errors=errors)

        readiness = self.policy.readiness_report()
        if not readiness.get("ready", False):
            self.audit.append_event(
                "governance_blocked",
                {"missing_controls": readiness.get("missing_controls", [])},
                actor_id=identity_ctx.get("user", {}).get("user_id", ""),
                workflow_id=workflow.get("id", ""),
                workflow_version=workflow.get("version", ""),
                outcome="blocked",
            )
            return RunResult(status="governance_blocked", executed_steps=0, errors=readiness.get("missing_controls", []))

        publication = workflow.get("publication", {})
        if publication.get("state") != "published":
            return RunResult(status="not_published", executed_steps=0, errors=["workflow_not_published"])

        steps = copy.deepcopy(workflow.get("steps", []))
        loop_index = self._first_index(steps, "for_each_row")
        executed = 0

        try:
            if loop_index is None:
                status, executed, blocked_step_id = self._run_steps(
                    steps=steps,
                    identity_ctx=identity_ctx,
                    workflow=workflow,
                    executed_start=0,
                )
                return RunResult(status=status, executed_steps=executed, blocked_step_id=blocked_step_id)

            pre_steps = steps[:loop_index]
            loop_step = steps[loop_index]
            loop_steps = steps[loop_index + 1 :]

            pre_status, executed, blocked_step_id = self._run_steps(pre_steps, identity_ctx, workflow, executed_start=executed)
            if pre_status != "success":
                return RunResult(status=pre_status, executed_steps=executed, blocked_step_id=blocked_step_id)

            rows = self._read_rows(loop_step)
            for row in rows:
                self.runtime_state["row"] = row
                loop_status, executed, blocked_step_id = self._run_steps(loop_steps, identity_ctx, workflow, executed_start=executed)
                if loop_status != "success":
                    return RunResult(status=loop_status, executed_steps=executed, blocked_step_id=blocked_step_id)

            return RunResult(status="success", executed_steps=executed, blocked_step_id="")
        except NeedUserInputError as exc:
            return RunResult(status="paused_for_user", executed_steps=executed, errors=[str(exc)])
        except PermissionError as exc:
            return RunResult(status="approval_denied", executed_steps=executed, errors=[str(exc)])
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.audit.append_event(
                "run_exception",
                {"error": str(exc)},
                actor_id=identity_ctx.get("user", {}).get("user_id", ""),
                workflow_id=workflow.get("id", ""),
                workflow_version=workflow.get("version", ""),
                outcome="error",
            )
            return RunResult(status="error", executed_steps=executed, errors=[str(exc)])

    def _run_steps(
        self,
        steps: List[Dict[str, Any]],
        identity_ctx: Dict[str, Any],
        workflow: Dict[str, Any],
        executed_start: int,
    ) -> tuple[str, int, str]:
        executed = executed_start
        for step in steps:
            if self._should_skip_step_due_to_if(step):
                self.audit.append_event(
                    "step_skipped",
                    {"step_id": step.get("id", ""), "reason": "if_condition_true"},
                    actor_id=identity_ctx.get("user", {}).get("user_id", ""),
                    workflow_id=workflow.get("id", ""),
                    workflow_version=workflow.get("version", ""),
                    step_id=step.get("id", ""),
                    outcome="skipped",
                )
                continue

            if self.kill_switch.is_active():
                self.audit.append_event(
                    "kill_switch_abort",
                    {"reason": "kill_switch_active"},
                    actor_id=identity_ctx.get("user", {}).get("user_id", ""),
                    workflow_id=workflow.get("id", ""),
                    workflow_version=workflow.get("version", ""),
                    step_id=step.get("id", ""),
                    outcome="aborted",
                )
                return "aborted", executed, step.get("id", "")

            runtime_ctx = self._runtime_policy_context(step)
            decision: PolicyDecision = self.policy.evaluate(
                identity_ctx=identity_ctx,
                step_ctx=step,
                workflow_ctx=workflow,
                runtime_ctx=runtime_ctx,
            )

            self.audit.append_event(
                "policy_decision",
                {"step_id": step.get("id", ""), "decision": asdict(decision)},
                actor_id=identity_ctx.get("user", {}).get("user_id", ""),
                workflow_id=workflow.get("id", ""),
                workflow_version=workflow.get("version", ""),
                step_id=step.get("id", ""),
                outcome="allow" if decision.allow else "deny",
            )

            if not decision.allow:
                return "blocked", executed, step.get("id", "")

            if decision.required_approvals:
                approval = self.approval.wait_for_approval(
                    step=step,
                    approver_levels=decision.required_approvals,
                    context={"workflow_id": workflow.get("id"), "step_id": step.get("id")},
                )
                self.audit.append_event(
                    "approval_granted",
                    {"request_id": approval.request_id, "status": approval.status},
                    actor_id=identity_ctx.get("user", {}).get("user_id", ""),
                    workflow_id=workflow.get("id", ""),
                    workflow_version=workflow.get("version", ""),
                    step_id=step.get("id", ""),
                    outcome=approval.status,
                )

            self._execute_step(step)
            executed += 1

        return "success", executed, ""

    def _execute_step(self, step: Dict[str, Any]) -> None:
        step_type = step.get("type")
        if step_type == "for_each_row":
            return

        if step_type == "if":
            condition = step.get("data", {}).get("condition", "False")
            result = evaluate_condition(condition, self.runtime_state)
            self.runtime_state["last_if"] = result
            self.runtime_state["_if_pending"] = True
            return

        if step_type == "ask_user":
            question = step.get("data", {}).get("question", "Please choose")
            options = step.get("data", {}).get("options", [])
            if self.ask_user_handler is None:
                raise NeedUserInputError(question)
            answer = self.ask_user_handler(question, options, dict(self.runtime_state))
            self.runtime_state["last_user_answer"] = answer
            return

        if step_type == "require_approval":
            # Approval is already enforced in _run_steps via policy decision.
            return

        if step_type in {"read_cell", "set_cell"}:
            self._execute_excel(step)
            return

        if step_type in {"navigate_url", "assert_visible"}:
            self._execute_browser(step)
            return

        if step_type == "submit_action":
            # Submit may be web or desktop. Dispatch from selector strategy.
            strategy = step.get("target", {}).get("selector", {}).get("strategy", "")
            if strategy == "uia":
                self._execute_uia(step)
            else:
                self._execute_browser(step)
            return

        if step_type in {"open_app", "focus_window", "click", "type", "hotkey", "wait_for", "extract_field", "copy", "paste", "screenshot_redacted"}:
            strategy = step.get("target", {}).get("selector", {}).get("strategy", "")
            if strategy == "uia" or step.get("target", {}).get("app") == "ClaimsDesktop":
                self._execute_uia(step)
            else:
                self._execute_browser(step)
            return

        raise ValueError(f"Unsupported step type: {step_type}")

    def _execute_excel(self, step: Dict[str, Any]) -> None:
        adapter = self.adapters.get("excel")
        if adapter is None:
            raise RuntimeError("excel adapter is required")
        data = step.get("data", {})
        if step["type"] == "read_cell":
            value = adapter.read_cell(
                sheet=data.get("sheet", "Claims"),
                row=data.get("row", self.runtime_state.get("row", {}).get("_index", 2)),
                column=data.get("column", "A"),
            )
            save_as = data.get("save_as")
            if save_as:
                self.runtime_state[save_as] = value
        else:
            value = self._resolve_value(data.get("value_ref")) if data.get("value_ref") else data.get("value")
            adapter.set_cell(
                sheet=data.get("sheet", "Claims"),
                row=data.get("row", self.runtime_state.get("row", {}).get("_index", 2)),
                column=data.get("column", "A"),
                value=value,
            )

    def _execute_browser(self, step: Dict[str, Any]) -> None:
        adapter = self.adapters.get("playwright")
        if adapter is None:
            raise RuntimeError("playwright adapter is required")
        step_type = step["type"]
        target = step.get("target", {})
        data = step.get("data", {})

        if step_type == "navigate_url":
            adapter.navigate_url(target.get("url", ""))
            return
        if step_type in {"click", "submit_action"}:
            adapter.click(target.get("selector", {}))
            return
        if step_type == "type":
            text = self._resolve_value(data.get("value_ref")) if data.get("value_ref") else data.get("value", "")
            adapter.type(target.get("selector", {}), text)
            return
        if step_type == "wait_for":
            adapter.wait_for(target.get("selector", {}), timeout_ms=step.get("control", {}).get("timeout_ms"))
            return
        if step_type == "assert_visible":
            adapter.assert_visible(target.get("selector", {}), timeout_ms=step.get("control", {}).get("timeout_ms"))
            return
        if step_type == "extract_field":
            value = adapter.extract_field(target.get("selector", {}))
            save_as = data.get("save_as")
            if save_as:
                self.runtime_state[save_as] = value
            return
        if step_type == "screenshot_redacted":
            adapter.screenshot_redacted(reason=data.get("reason", "diagnostic"))
            return
        if step_type in {"copy", "paste", "open_app", "focus_window", "hotkey"}:
            adapter.generic_action(step_type, target, data)
            return
        raise ValueError(f"Unsupported browser step: {step_type}")

    def _execute_uia(self, step: Dict[str, Any]) -> None:
        adapter = self.adapters.get("uia")
        if adapter is None:
            raise RuntimeError("uia adapter is required")
        step_type = step["type"]
        target = step.get("target", {})
        data = step.get("data", {})

        if step_type == "open_app":
            adapter.open_app(target.get("path", ""))
            return
        if step_type == "focus_window":
            adapter.focus_window(target.get("selector", {}))
            return
        if step_type in {"click", "submit_action"}:
            adapter.click(target.get("selector", {}))
            return
        if step_type == "type":
            value = self._resolve_value(data.get("value_ref")) if data.get("value_ref") else data.get("value", "")
            adapter.type(target.get("selector", {}), value)
            return
        if step_type == "hotkey":
            adapter.hotkey(data.get("keys", ""))
            return
        if step_type == "wait_for":
            adapter.wait_for(target.get("selector", {}), timeout_ms=step.get("control", {}).get("timeout_ms"))
            return
        if step_type == "assert_visible":
            adapter.assert_visible(target.get("selector", {}), timeout_ms=step.get("control", {}).get("timeout_ms"))
            return
        if step_type == "extract_field":
            value = adapter.extract_field(target.get("selector", {}))
            save_as = data.get("save_as")
            if save_as:
                self.runtime_state[save_as] = value
            return
        if step_type in {"copy", "paste"}:
            adapter.generic_action(step_type, target, data)
            return
        if step_type == "screenshot_redacted":
            adapter.screenshot_redacted(reason=data.get("reason", "diagnostic"))
            return
        raise ValueError(f"Unsupported UIA step: {step_type}")

    def _runtime_policy_context(self, step: Dict[str, Any]) -> Dict[str, Any]:
        target = step.get("target", {})
        url = target.get("url", "")
        target_domain = urlparse(url).hostname if url else ""
        return {
            "kill_switch_active": self.kill_switch.is_active(),
            "target_domain": target_domain or "",
            "target_app": target.get("app", ""),
        }

    def _read_rows(self, loop_step: Dict[str, Any]) -> List[Dict[str, Any]]:
        adapter = self.adapters.get("excel")
        if adapter is None:
            raise RuntimeError("excel adapter is required for for_each_row")
        data = loop_step.get("data", {})
        rows = adapter.read_rows(
            sheet=data.get("sheet", "Claims"),
            start_row=int(data.get("start_row", 2)),
            end_row=data.get("end_row"),
        )
        return rows

    def _resolve_value(self, value_ref: str) -> Any:
        if not value_ref:
            return ""
        if "." in value_ref:
            left, right = value_ref.split(".", 1)
            if left == "row":
                return self.runtime_state.get("row", {}).get(right)
        return self.runtime_state.get(value_ref, value_ref)

    def _should_skip_step_due_to_if(self, step: Dict[str, Any]) -> bool:
        if not self.runtime_state.get("_if_pending"):
            return False
        if step.get("type") == "ask_user" and self.runtime_state.get("last_if") is True:
            self.runtime_state["_if_pending"] = False
            return True
        if step.get("type") != "ask_user":
            self.runtime_state["_if_pending"] = False
        return False

    @staticmethod
    def _first_index(steps: List[Dict[str, Any]], step_type: str) -> Optional[int]:
        for idx, step in enumerate(steps):
            if step.get("type") == step_type:
                return idx
        return None
