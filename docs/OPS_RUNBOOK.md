# Ops/Admin Runbook

## Workflow Onboarding

1. Capture draft in Teach Mode (`TeachCapture`).
2. Label sensitive steps and verification assertions.
3. Validate workflow DSL (`lam/dsl/validator.py`).
4. Save draft in workflow store.
5. Publish with required approvers (two-person rule when enabled).

## Publishing Approvals

1. Author submits version for publication.
2. Approvers review risk tier, selectors, and sensitivity metadata.
3. Publish via `WorkflowStore.publish()`.
4. Confirm immutable audit entries exist for publication event.

## Incident Response

1. Trigger kill switch (local or admin).
2. Stop active runs and quarantine endpoint.
3. Preserve audit chain and approval evidence.
4. Open compliance/security incident with event hashes.

## Secrets Rotation

1. Rotate shared secrets in enterprise vault.
2. Rotate endpoint credentials in Credential Manager/DPAPI-backed stores.
3. Validate auth-dependent workflows post-rotation.
4. Audit all rotation actions.

## Endpoint Deployment + Updates

1. Distribute builds via internal channel only (Intune/SCCM/internal package feed).
2. Validate signatures and policy bundle integrity.
3. Deploy by rings: pilot -> department -> enterprise.
4. Keep rollback package available for every promoted version.
