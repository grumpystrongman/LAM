import unittest
from pathlib import Path

from lam.adapters.excel_adapter import ExcelAdapter
from lam.adapters.playwright_adapter import PlaywrightAdapter
from lam.adapters.uia_adapter import UIAAdapter
from lam.endpoint_agent.kill_switch import KillSwitch
from lam.endpoint_agent.runner import Runner
from lam.governance.approval_client import ApprovalClient
from lam.governance.audit_logger import AuditLogger, JsonlAuditSink
from lam.governance.policy_engine import PolicyEngine
from lam.governance.redaction import Redactor
from lam.services.approval_service import InMemoryApprovalService


class TestRunnerGovernance(unittest.TestCase):
    def setUp(self) -> None:
        config = {
            "governance": {"controls": {"identity": True, "policy": True, "approval": True, "audit": True, "secrets": True}},
            "abac": {"require_managed_device": True, "require_compliant_device": True, "allowed_network_zones": ["corp"]},
            "policies": {
                "os_action_allowlist": ["navigate_url", "type", "submit_action", "read_cell", "set_cell", "require_approval", "open_app", "for_each_row"],
                "deny_rules": [],
            },
            "app_allowlist": {"apps": [{"name": "Browser"}, {"name": "Microsoft Excel"}]},
            "domain_allowlist": {"domains": ["claims.internal.health.local"]},
            "sensitive_actions": {
                "sensitive_actions": ["submit_action", "set_cell"],
                "high_risk_actions": ["submit_action"],
                "approval_policy": {"default_low_risk": ["approver"], "default_high_risk": ["manager", "compliance"]},
            },
        }
        self.policy = PolicyEngine(config)
        self.approval_service = InMemoryApprovalService(auto_approve=True)
        self.approval_client = ApprovalClient(self.approval_service, poll_interval_seconds=0.01)
        self.kill_switch = KillSwitch()
        self.excel = ExcelAdapter()
        self.excel.set_memory_rows([{"member_name": "Alice", "claim_id": "C123", "status": ""}])
        self.playwright = PlaywrightAdapter(domain_allowlist=["claims.internal.health.local"], dry_run=True)
        self.uia = UIAAdapter(dry_run=True)
        artifacts = Path("test_artifacts")
        artifacts.mkdir(parents=True, exist_ok=True)
        self.audit_path = artifacts / "integration_audit.jsonl"
        if self.audit_path.exists():
            self.audit_path.unlink()
        sink = JsonlAuditSink(self.audit_path)
        self.audit = AuditLogger(sink=sink, redactor=Redactor())
        self.identity = {
            "user": {"user_id": "u1", "role": "Runner", "department": "Claims", "clearance": "high"},
            "device": {"managed": True, "compliant": True, "network_zone": "corp"},
        }
        self.workflow = {
            "id": "wf1",
            "version": "1.0.0",
            "risk_tier": "high",
            "allowed_roles": ["Runner"],
            "allowed_departments": ["Claims"],
            "publication": {"state": "published", "approved_by": ["a", "b"], "two_person_rule": True},
            "steps": [
                {"id": "1", "type": "for_each_row", "data": {"sheet": "Claims", "start_row": 2}},
                {"id": "2", "type": "read_cell", "data": {"sheet": "Claims", "column": "B", "save_as": "claim_id"}},
                {
                    "id": "3",
                    "type": "navigate_url",
                    "target": {"app": "Browser", "url": "https://claims.internal.health.local"},
                    "sensitivity": {"data_classification": "none", "write_impact": "read", "requires_approval": False},
                },
                {
                    "id": "4",
                    "type": "type",
                    "target": {"app": "Browser", "selector": {"strategy": "label", "value": "Claim ID"}},
                    "data": {"value_ref": "claim_id"},
                    "sensitivity": {"data_classification": "phi", "write_impact": "read", "requires_approval": False},
                },
                {
                    "id": "5",
                    "type": "submit_action",
                    "target": {"app": "Browser", "selector": {"strategy": "role_name", "value": "button:Submit"}},
                    "sensitivity": {"data_classification": "none", "write_impact": "submit", "requires_approval": True},
                },
            ],
        }

    def tearDown(self) -> None:
        if self.audit_path.exists():
            self.audit_path.unlink()

    def test_runner_success_with_governance(self) -> None:
        runner = Runner(
            policy_engine=self.policy,
            approval_client=self.approval_client,
            audit_logger=self.audit,
            adapters={"excel": self.excel, "playwright": self.playwright, "uia": self.uia},
            kill_switch=self.kill_switch,
            ask_user_handler=lambda q, o, c: o[0] if o else "ok",
        )
        result = runner.run(self.workflow, self.identity)
        self.assertEqual(result.status, "success")
        self.assertGreaterEqual(result.executed_steps, 4)


if __name__ == "__main__":
    unittest.main()
