# Governance-First LAM Blueprint (Implemented Scaffold)

## 1) Executive Summary

- Governance controls are implemented first and checked before autonomous run.
- Endpoint runner enforces policy before each DSL step.
- PHI/PII is redacted before persistence via centralized redaction hooks.
- Tamper-evident append-only audit chain is implemented.
- Sensitive write/submit actions trigger mandatory approval gates.
- Playwright-first web adapter and UIA-first desktop adapter are implemented.
- Selenium fallback exists but requires an explicit waiver.

## 2) Reference Architecture

- Endpoint Agent:
  - `lam/endpoint_agent/runner.py`
  - `lam/endpoint_agent/teach_capture.py`
  - `lam/endpoint_agent/kill_switch.py`
- Governance Services:
  - `lam/governance/policy_engine.py`
  - `lam/governance/approval_client.py`
  - `lam/governance/audit_logger.py`
  - `lam/governance/redaction.py`
- Control Plane Stubs:
  - `lam/services/approval_service.py`
  - `lam/services/workflow_store.py`
  - `lam/services/audit_store.py`
  - `lam/services/api_server.py`
- Workflow + Config:
  - `workflows/*.yaml`
  - `config/*.yaml`

## 3) Governance Layer Notes

- RBAC/ABAC:
  - Role and department checks are in `PolicyEngine`.
  - Device posture and network zone constraints are enforced.
- Policy:
  - App/domain/action allowlists loaded from `config/`.
  - Deny rules for sensitive copy/paste are implemented.
- Approvals:
  - High-risk actions default to manager+compliance.
  - `require_approval` step supports explicit approver routing.
- Audit:
  - Event hash chain with `prev_hash`/`event_hash`.
  - Optional HMAC signature support via `LAM_AUDIT_SIGNING_KEY`.
  - Redaction confidence threshold enforced before write.
- Publishing controls:
  - Two-person publish rule enforced in `WorkflowStore.publish()`.

## 4) Runtime Layer Notes

- Deterministic runner with safe fail behavior:
  - Governance readiness gate.
  - Kill switch abort path.
  - Policy + approvals before step execution.
- Uncertainty:
  - `ask_user` step pauses execution if no handler is attached.
  - `if` condition evaluation supports guarded prompt behavior.
- Teach Mode:
  - Captured actions with suppression for credential fields.

## 5) Testing and Validation

- Unit:
  - `tests/unit/test_policy_engine.py`
  - `tests/unit/test_redaction.py`
- Integration:
  - `tests/integration/test_runner_governance.py`
- Security:
  - `tests/security/test_no_phi_in_audit.py`

Run:

```powershell
python -m unittest discover -s tests -p "test_*.py"
```
