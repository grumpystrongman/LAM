import unittest

from lam.governance.policy_engine import PolicyEngine


class TestPolicyEngine(unittest.TestCase):
    def setUp(self) -> None:
        config = {
            "governance": {"controls": {"identity": True, "policy": True, "approval": True, "audit": True, "secrets": True}},
            "abac": {"require_managed_device": True, "require_compliant_device": True, "allowed_network_zones": ["corp"]},
            "policies": {
                "os_action_allowlist": ["click", "navigate_url", "submit_action", "type", "read_cell", "set_cell", "require_approval"],
                "deny_rules": [
                    {"action": "paste", "when_target_domain_not_in_allowlist": True},
                    {
                        "action": "copy",
                        "when_data_classification_in": ["phi", "pii", "credential"],
                        "and_target_app_not_trusted": True,
                    },
                ],
            },
            "app_allowlist": {"apps": [{"name": "Browser"}, {"name": "ClaimsDesktop"}, {"name": "Microsoft Excel"}]},
            "domain_allowlist": {"domains": ["claims.internal.health.local"]},
            "sensitive_actions": {
                "sensitive_actions": ["submit_action", "set_cell"],
                "high_risk_actions": ["submit_action"],
                "approval_policy": {"default_low_risk": ["approver"], "default_high_risk": ["manager", "compliance"]},
            },
        }
        self.engine = PolicyEngine(config)
        self.identity = {
            "user": {"user_id": "u1", "role": "Runner", "department": "Claims", "clearance": "high"},
            "device": {"managed": True, "compliant": True, "network_zone": "corp"},
        }
        self.workflow = {"allowed_roles": ["Runner"], "allowed_departments": ["Claims"], "risk_tier": "high"}

    def test_denies_unallowlisted_domain(self) -> None:
        step = {"type": "navigate_url", "target": {"app": "Browser", "url": "https://example.com"}}
        decision = self.engine.evaluate(self.identity, step, self.workflow, {})
        self.assertFalse(decision.allow)
        self.assertIn("target_domain_not_allowlisted", decision.reasons)

    def test_requires_dual_approval_for_submit(self) -> None:
        step = {"type": "submit_action", "target": {"app": "Browser"}}
        decision = self.engine.evaluate(self.identity, step, self.workflow, {})
        self.assertTrue(decision.allow)
        self.assertEqual(decision.required_approvals, ["manager", "compliance"])

    def test_denies_if_kill_switch_active(self) -> None:
        step = {"type": "click", "target": {"app": "Browser"}}
        decision = self.engine.evaluate(self.identity, step, self.workflow, {"kill_switch_active": True})
        self.assertFalse(decision.allow)
        self.assertIn("kill_switch_active", decision.reasons)


if __name__ == "__main__":
    unittest.main()

