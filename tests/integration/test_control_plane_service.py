from pathlib import Path
import uuid
import unittest

from lam.governance.audit_logger import AuditLogger
from lam.governance.policy_engine import PolicyEngine
from lam.governance.redaction import Redactor
from lam.services.api_server import ControlPlaneService
from lam.services.audit_store import SqliteAuditSink
from lam.services.sqlite_approval_service import SqliteApprovalService
from lam.services.workflow_store import WorkflowStore


class TestControlPlaneService(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("test_artifacts")
        self.root.mkdir(parents=True, exist_ok=True)
        run_id = uuid.uuid4().hex
        self.audit_db = self.root / f"cp_audit_{run_id}.db"
        self.approval_db = self.root / f"cp_approval_{run_id}.db"
        self.workflow_root = self.root / f"workflows_{run_id}"

        config = {
            "governance": {"controls": {"identity": True, "policy": True, "approval": True, "audit": True, "secrets": True}},
            "abac": {"require_managed_device": True, "require_compliant_device": True, "allowed_network_zones": ["corp"]},
            "policies": {"os_action_allowlist": ["click", "type", "submit_action"], "deny_rules": []},
            "app_allowlist": {"apps": [{"name": "Browser"}]},
            "domain_allowlist": {"domains": ["claims.internal.health.local"]},
            "sensitive_actions": {
                "sensitive_actions": ["submit_action"],
                "high_risk_actions": ["submit_action"],
                "approval_policy": {"default_low_risk": ["approver"], "default_high_risk": ["manager", "compliance"]},
            },
        }
        policy = PolicyEngine(config)
        approvals = SqliteApprovalService(self.approval_db)
        workflows = WorkflowStore(self.workflow_root)
        audit_logger = AuditLogger(sink=SqliteAuditSink(self.audit_db), redactor=Redactor())
        self.service = ControlPlaneService(
            policy_engine=policy,
            approval_service=approvals,
            workflow_store=workflows,
            audit_logger=audit_logger,
        )

        self.workflow = {
            "id": "wf_cp",
            "version": "1.0.0",
            "risk_tier": "high",
            "allowed_roles": ["Runner"],
            "publication": {"state": "draft", "approved_by": [], "two_person_rule": True},
            "steps": [{"id": "1", "type": "click", "target": {"app": "Browser"}}],
        }

    def tearDown(self) -> None:
        # Windows may hold file handles briefly; cleanup is handled out-of-band.
        pass

    def test_workflow_publish_and_policy_eval(self) -> None:
        actor = "admin1"
        draft = self.service.save_workflow_draft({"workflow": self.workflow}, actor_id=actor)
        self.assertIn("wf_cp", draft["path"])

        published = self.service.publish_workflow({"workflow": self.workflow, "approvers": ["a1", "a2"]}, actor_id=actor)
        self.assertTrue(published["verified"])

        decision = self.service.evaluate_policy(
            {
                "identity_ctx": {
                    "user": {"user_id": "u1", "role": "Runner", "department": "Claims", "clearance": "high"},
                    "device": {"managed": True, "compliant": True, "network_zone": "corp"},
                },
                "workflow_ctx": {"allowed_roles": ["Runner"], "risk_tier": "high"},
                "step_ctx": {"id": "1", "type": "submit_action", "target": {"app": "Browser"}},
                "runtime_ctx": {},
            },
            actor_id=actor,
        )
        self.assertTrue(decision["allow"])
        self.assertEqual(decision["required_approvals"], ["manager", "compliance"])


if __name__ == "__main__":
    unittest.main()
