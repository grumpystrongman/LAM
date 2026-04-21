from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import yaml


@dataclass(slots=True)
class PolicyDecision:
    allow: bool
    reasons: List[str]
    required_approvals: List[str]
    obligations: Dict[str, Any]


class PolicyEngine:
    """Evaluates RBAC/ABAC, allowlists, and approval gating for every step."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._app_allowlist = config.get("app_allowlist", {}).get("apps", [])
        self._domain_allowlist = set(config.get("domain_allowlist", {}).get("domains", []))
        sensitive_cfg = config.get("sensitive_actions", {})
        self._sensitive_actions = set(sensitive_cfg.get("sensitive_actions", []))
        self._high_risk_actions = set(sensitive_cfg.get("high_risk_actions", []))
        approval_cfg = sensitive_cfg.get("approval_policy", {})
        self._low_risk_approvals = approval_cfg.get("default_low_risk", ["approver"])
        self._high_risk_approvals = approval_cfg.get("default_high_risk", ["manager", "compliance"])

    @classmethod
    def from_config_dir(cls, config_dir: str | Path) -> "PolicyEngine":
        config_dir = Path(config_dir)
        merged = _read_yaml(config_dir / "policy.yaml")
        merged["app_allowlist"] = _read_yaml(config_dir / "allowlist_apps.yaml")
        merged["domain_allowlist"] = _read_yaml(config_dir / "allowlist_domains.yaml")
        merged["sensitive_actions"] = _read_yaml(config_dir / "sensitive_actions.yaml")
        return cls(merged)

    def readiness_report(self) -> Dict[str, Any]:
        governance = self.config.get("governance", {})
        controls = governance.get(
            "controls",
            {"identity": False, "policy": False, "approval": False, "audit": False, "secrets": False},
        )
        missing = [name for name, enabled in controls.items() if not enabled]
        return {"ready": len(missing) == 0, "missing_controls": missing}

    def evaluate(
        self,
        identity_ctx: Dict[str, Any],
        step_ctx: Dict[str, Any],
        workflow_ctx: Optional[Dict[str, Any]] = None,
        runtime_ctx: Optional[Dict[str, Any]] = None,
    ) -> PolicyDecision:
        workflow_ctx = workflow_ctx or {}
        runtime_ctx = runtime_ctx or {}
        reasons: List[str] = []
        obligations: Dict[str, Any] = {}

        if runtime_ctx.get("kill_switch_active"):
            return PolicyDecision(False, ["kill_switch_active"], [], {"abort": True})

        action = step_ctx.get("type", "")
        if not action:
            return PolicyDecision(False, ["missing_step_type"], [], {})

        if not self._check_role(identity_ctx, workflow_ctx):
            reasons.append("role_not_permitted_for_workflow")

        if not self._check_abac(identity_ctx, workflow_ctx):
            reasons.append("abac_constraints_not_met")

        if not self._check_action_allowlist(action):
            reasons.append("os_action_not_allowlisted")

        app_reason = self._check_app_allowlist(step_ctx)
        if app_reason:
            reasons.append(app_reason)

        domain_reason = self._check_domain_allowlist(action, step_ctx)
        if domain_reason:
            reasons.append(domain_reason)

        deny_reason = self._check_deny_rules(action, step_ctx, runtime_ctx)
        if deny_reason:
            reasons.append(deny_reason)

        required_approvals = self._determine_approvals(action, step_ctx)
        if required_approvals:
            obligations["approval_required"] = True
            obligations["approval_levels"] = required_approvals

        allow = len(reasons) == 0
        return PolicyDecision(allow=allow, reasons=reasons, required_approvals=required_approvals, obligations=obligations)

    def _check_role(self, identity_ctx: Dict[str, Any], workflow_ctx: Dict[str, Any]) -> bool:
        allowed_roles = workflow_ctx.get("allowed_roles")
        if not allowed_roles:
            return True
        user_role = identity_ctx.get("user", {}).get("role", "")
        return user_role in set(allowed_roles)

    def _check_abac(self, identity_ctx: Dict[str, Any], workflow_ctx: Dict[str, Any]) -> bool:
        abac_cfg = self.config.get("abac", {})
        device = identity_ctx.get("device", {})
        user = identity_ctx.get("user", {})

        if abac_cfg.get("require_managed_device", True) and not device.get("managed", False):
            return False
        if abac_cfg.get("require_compliant_device", True) and not device.get("compliant", False):
            return False

        allowed_zones = set(abac_cfg.get("allowed_network_zones", []))
        if allowed_zones and device.get("network_zone") not in allowed_zones:
            return False

        workflow_departments = set(workflow_ctx.get("allowed_departments", []))
        if workflow_departments and user.get("department") not in workflow_departments:
            return False

        risk_tier = workflow_ctx.get("risk_tier", "high")
        user_clearance = user.get("clearance", "")
        if risk_tier == "high" and user_clearance and user_clearance not in {"high", "admin"}:
            return False
        return True

    def _check_action_allowlist(self, action: str) -> bool:
        allowed = set(self.config.get("policies", {}).get("os_action_allowlist", []))
        if not allowed:
            return True
        if action in {"for_each_row", "if", "else"}:
            return True
        return action in allowed or action in self._sensitive_actions

    def _check_app_allowlist(self, step_ctx: Dict[str, Any]) -> str:
        target = step_ctx.get("target", {})
        app_name = target.get("app")
        app_path = target.get("path")
        if not app_name and not app_path:
            return ""

        for item in self._app_allowlist:
            allowed_name = item.get("name")
            allowed_path = item.get("path")
            if app_name and allowed_name == app_name:
                return ""
            if app_path and allowed_path and str(app_path).lower() == str(allowed_path).lower():
                return ""
        return "target_application_not_allowlisted"

    def _check_domain_allowlist(self, action: str, step_ctx: Dict[str, Any]) -> str:
        if action not in {"navigate_url", "click", "type", "submit_action", "extract_field"}:
            return ""
        target = step_ctx.get("target", {})
        url = target.get("url")
        if not url:
            return ""
        host = (urlparse(url).hostname or "").lower()
        if not self._domain_allowlist:
            return ""
        if host and host in self._domain_allowlist:
            return ""
        return "target_domain_not_allowlisted"

    def _check_deny_rules(self, action: str, step_ctx: Dict[str, Any], runtime_ctx: Dict[str, Any]) -> str:
        rules = self.config.get("policies", {}).get("deny_rules", [])
        data_classification = step_ctx.get("sensitivity", {}).get("data_classification", "none")
        target_domain = runtime_ctx.get("target_domain", "")
        target_app = runtime_ctx.get("target_app", "")
        trusted_apps = {app.get("name") for app in self._app_allowlist}

        for rule in rules:
            if rule.get("action") != action:
                continue
            if rule.get("when_target_domain_not_in_allowlist"):
                if target_domain and target_domain not in self._domain_allowlist:
                    return "deny_rule_target_domain_not_allowlisted"
            if "when_data_classification_in" in rule:
                if data_classification in set(rule.get("when_data_classification_in", [])):
                    if rule.get("and_target_app_not_trusted") and target_app not in trusted_apps:
                        return "deny_rule_sensitive_copy_to_untrusted_target"
        return ""

    def _determine_approvals(self, action: str, step_ctx: Dict[str, Any]) -> List[str]:
        sensitivity = step_ctx.get("sensitivity", {})
        requires_approval = bool(sensitivity.get("requires_approval", False))
        write_impact = sensitivity.get("write_impact", "read")

        if action == "require_approval":
            return step_ctx.get("data", {}).get("approvers", self._high_risk_approvals)

        if action in self._high_risk_actions or write_impact == "submit":
            return list(self._high_risk_approvals)

        if action in self._sensitive_actions or requires_approval or write_impact == "write":
            return list(self._low_risk_approvals)

        return []


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8")
    loaded = yaml.safe_load(raw)
    return loaded or {}

