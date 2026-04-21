# Implementation Phases and Timeline

## Phase 0 (Weeks 1-4): Governance Foundation

- Identity context integration and role mapping.
- Policy engine + allowlist enforcement.
- Approval workflow service and client integration.
- Tamper-evident audit logging with redaction confidence gates.
- Secrets integration stubs (Credential Manager/DPAPI + enterprise vault hooks).
- Kill switch controls.

## Phase 1 (Weeks 5-8): Deterministic Automation

- DSL runner with policy-before-step checks.
- Playwright-first web adapter.
- UIA-first desktop adapter.
- Mandatory approval gates for sensitive writes/submissions.

## Phase 2 (Weeks 9-12): Teach Mode and Replay

- Action capture and selector labeling UX.
- Teach-to-DSL compilation and preview mode.
- Pause/resume with `ask_user` resolution.

## Phase 3 (Weeks 13-16): Optional Self-Hosted LLM Assistance

- Strictly gated by policy.
- No PHI persistence.
- Draft suggestion only, deterministic runner remains execution authority.

## Phase 4 (Weeks 17-20): Scale and Operations

- Multi-site rollout and endpoint fleet controls.
- Drift dashboards and run analytics.
- Change-control workflow marketplace with publication governance.
