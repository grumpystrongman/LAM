# Control Plane API

Base URL: `http://<host>:8787`

## Authentication

- `x-api-key: <key>` if `LAM_API_KEY` is configured.
- `Authorization: Bearer <token>` if `LAM_BEARER_SECRET` is configured.
- `/health` can be anonymous by default.

## Endpoints

### `GET /health`
- Returns control-plane liveness and governance readiness.

### `POST /v1/policy/evaluate`
- Request:
  - `identity_ctx`
  - `workflow_ctx`
  - `step_ctx`
  - `runtime_ctx`
- Response:
  - `allow`, `reasons`, `required_approvals`, `obligations`

### `POST /v1/approvals`
- Request:
  - `step`
  - `approver_levels`
  - `context`
- Response:
  - `request_id`, `status`

### `POST /v1/approvals/{request_id}/decision`
- Request:
  - `decision`: `approve|deny`
  - `approver_level`
  - `reason`
- Response:
  - `request_id`, `status`

### `GET /v1/approvals/{request_id}`
- Response:
  - `request_id`, `status`, `request`

### `POST /v1/workflows/draft`
- Request:
  - `workflow`
- Response:
  - `path`

### `POST /v1/workflows/publish`
- Request:
  - `workflow` and `approvers`
  - or `workflow_id`, `version`, and `approvers`
- Response:
  - `path`, `verified`

### `GET /v1/workflows/{workflow_id}/versions`
- Response:
  - `workflow_id`, `versions`

### `GET /v1/audit/validate`
- Response:
  - `status`, `errors`
