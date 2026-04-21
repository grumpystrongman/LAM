# Commercial Readiness Checklist

## Security and Compliance

- [x] Governance gate blocks autonomous execution when controls are not ready.
- [x] Policy checks before each step execution.
- [x] Mandatory approvals for sensitive/high-risk actions.
- [x] PHI/PII redaction before persistence.
- [x] Audit hash chain validation and append-only audit storage.
- [x] Control-plane API authentication (API key or bearer token).

## Reliability and Operability

- [x] Durable approval storage (SQLite).
- [x] Durable audit storage with immutable triggers (SQLite).
- [x] Workflow versioning and publish-time integrity verification.
- [x] CLI entrypoints for run, serve, and audit validation.
- [x] Unit/integration/security test coverage for core controls.

## Required Enterprise Integrations Before Production

- [ ] Replace token/API key auth with Entra/AD-backed authN/authZ.
- [ ] Replace SQLite with enterprise HA datastore and immutable retention policy.
- [ ] Integrate enterprise vault + DPAPI/Credential Manager for secret lifecycle.
- [ ] Add centralized observability pipeline (SIEM/SOC integration).
- [ ] Add endpoint deployment controls (Intune/SCCM ringed rollout with signed artifacts).
- [ ] Conduct threat model, penetration test, and compliance validation.
