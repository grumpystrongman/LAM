# Testing and Validation Plan

## Unit Tests

- Policy evaluation matrix: role, ABAC, allowlist, deny rules, approval mapping.
- Redaction correctness and confidence threshold behavior.

## Integration Tests

- End-to-end deterministic workflow run with synthetic claims rows.
- Approval service interaction validation (pending/approved/denied paths).
- Audit chain generation per step and per decision.

## Security Tests

- Verify no raw PHI/PII persists in audit logs.
- Verify blocked exfiltration actions are denied and logged.
- Verify kill-switch abort path logs terminal event.

## Drift and Resilience

- Selector drift simulations.
- UI popup/timeouts/session expiry.
- Network latency and approval timeout handling.
