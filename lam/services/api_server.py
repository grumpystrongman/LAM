from __future__ import annotations

import json
import os
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

from lam.dsl.validator import validate_workflow
from lam.governance.authn import TokenAuth


@dataclass(slots=True)
class ApiAuthConfig:
    api_key: str = ""
    bearer_secret: str = ""
    bearer_issuer: str = "lam-control-plane"
    allow_anonymous_health: bool = True


class ApiAuth:
    def __init__(self, config: ApiAuthConfig) -> None:
        self.config = config
        self.token_auth: Optional[TokenAuth] = None
        if self.config.bearer_secret:
            self.token_auth = TokenAuth(secret=self.config.bearer_secret, issuer=self.config.bearer_issuer)

    def authenticate(self, headers: Dict[str, str]) -> Dict[str, Any]:
        supplied_key = headers.get("x-api-key", "")
        if self.config.api_key and supplied_key == self.config.api_key:
            return {"actor_id": "api_key_client", "roles": ["Admin"], "department": "Security", "clearance": "admin"}

        authz = headers.get("authorization", "")
        if authz.lower().startswith("bearer ") and self.token_auth:
            principal = self.token_auth.verify(authz.split(" ", 1)[1].strip())
            return {
                "actor_id": principal.subject,
                "roles": principal.roles,
                "department": principal.department,
                "clearance": principal.clearance,
            }

        if self.config.api_key or self.config.bearer_secret:
            raise PermissionError("Unauthorized")
        # Local development fallback if no auth configured.
        return {"actor_id": "anonymous", "roles": ["Auditor"], "department": "", "clearance": ""}


@dataclass(slots=True)
class ControlPlaneService:
    policy_engine: Any
    approval_service: Any
    workflow_store: Any
    audit_logger: Any

    def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "governance": self.policy_engine.readiness_report(),
        }

    def evaluate_policy(self, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        identity = payload.get("identity_ctx", {})
        step = payload.get("step_ctx", {})
        workflow = payload.get("workflow_ctx", {})
        runtime = payload.get("runtime_ctx", {})
        decision = self.policy_engine.evaluate(identity, step, workflow, runtime)
        data = {
            "allow": decision.allow,
            "reasons": decision.reasons,
            "required_approvals": decision.required_approvals,
            "obligations": decision.obligations,
        }
        self.audit_logger.append_event(
            "policy_evaluated_api",
            {"request": payload, "decision": data},
            actor_id=actor_id,
            workflow_id=workflow.get("id", ""),
            workflow_version=workflow.get("version", ""),
            step_id=step.get("id", ""),
            outcome="allow" if decision.allow else "deny",
        )
        return data

    def create_approval(self, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        step = payload.get("step", {})
        levels = list(payload.get("approver_levels", []))
        context = dict(payload.get("context", {}))
        request_id = self.approval_service.create_request(step=step, approver_levels=levels, context=context)
        self.audit_logger.append_event(
            "approval_requested_api",
            {"request_id": request_id, "levels": levels, "context": context},
            actor_id=actor_id,
            workflow_id=context.get("workflow_id", ""),
            workflow_version=context.get("workflow_version", ""),
            step_id=context.get("step_id", ""),
            outcome="pending",
        )
        return {"request_id": request_id, "status": self.approval_service.get_status(request_id)}

    def approval_decision(self, request_id: str, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        decision = payload.get("decision", "").lower()
        level = payload.get("approver_level", "")
        reason = payload.get("reason", "")
        if decision == "approve":
            status = self.approval_service.approve(
                request_id=request_id, approver_id=actor_id, approver_level=level, reason=reason
            )
        elif decision == "deny":
            status = self.approval_service.deny(
                request_id=request_id, approver_id=actor_id, approver_level=level, reason=reason
            )
        else:
            raise ValueError("decision must be approve or deny")
        self.audit_logger.append_event(
            "approval_decision_api",
            {"request_id": request_id, "decision": decision, "status": status, "level": level, "reason": reason},
            actor_id=actor_id,
            outcome=status,
        )
        return {"request_id": request_id, "status": status}

    def approval_status(self, request_id: str, actor_id: str) -> Dict[str, Any]:
        status = self.approval_service.get_status(request_id)
        request = {}
        if hasattr(self.approval_service, "get_request"):
            request = self.approval_service.get_request(request_id)
        self.audit_logger.append_event(
            "approval_status_api",
            {"request_id": request_id, "status": status},
            actor_id=actor_id,
            outcome=status,
        )
        return {"request_id": request_id, "status": status, "request": request}

    def save_workflow_draft(self, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        workflow = payload.get("workflow", {})
        errors = validate_workflow(workflow)
        if errors:
            raise ValueError(f"workflow_validation_failed:{errors}")
        path = self.workflow_store.save_draft(workflow)
        self.audit_logger.append_event(
            "workflow_draft_saved_api",
            {"workflow_id": workflow.get("id", ""), "version": workflow.get("version", ""), "path": str(path)},
            actor_id=actor_id,
            workflow_id=workflow.get("id", ""),
            workflow_version=workflow.get("version", ""),
            outcome="saved",
        )
        return {"path": str(path)}

    def publish_workflow(self, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        workflow = payload.get("workflow")
        if workflow is None:
            workflow_id = payload.get("workflow_id")
            version = payload.get("version")
            workflow = self.workflow_store.load(workflow_id, version, published_only=False)
        approvers = list(payload.get("approvers", []))
        path = self.workflow_store.publish(workflow, approvers=approvers)
        verified = self.workflow_store.verify_published(workflow.get("id", ""), workflow.get("version", ""))
        self.audit_logger.append_event(
            "workflow_published_api",
            {
                "workflow_id": workflow.get("id", ""),
                "version": workflow.get("version", ""),
                "path": str(path),
                "verified": verified,
            },
            actor_id=actor_id,
            workflow_id=workflow.get("id", ""),
            workflow_version=workflow.get("version", ""),
            outcome="published" if verified else "published_unverified",
        )
        return {"path": str(path), "verified": verified}

    def list_workflow_versions(self, workflow_id: str, actor_id: str) -> Dict[str, Any]:
        versions = self.workflow_store.list_versions(workflow_id)
        self.audit_logger.append_event(
            "workflow_versions_listed_api",
            {"workflow_id": workflow_id, "count": len(versions)},
            actor_id=actor_id,
            workflow_id=workflow_id,
            outcome="ok",
        )
        return {"workflow_id": workflow_id, "versions": versions}

    def validate_audit(self, actor_id: str) -> Dict[str, Any]:
        errors = self.audit_logger.validate_chain()
        status = "valid" if not errors else "invalid"
        self.audit_logger.append_event(
            "audit_validated_api",
            {"status": status, "error_count": len(errors)},
            actor_id=actor_id,
            outcome=status,
        )
        return {"status": status, "errors": errors}


class _RequestHandler(BaseHTTPRequestHandler):
    service: ControlPlaneService
    auth: ApiAuth
    server_version = "LAMControlPlane/1.0"

    def do_GET(self) -> None:  # noqa: N802
        try:
            if self.path == "/health":
                if not self.auth.config.allow_anonymous_health:
                    self._require_auth()
                self._send_json(200, self.service.health())
                return

            actor = self._require_auth()
            if self.path.startswith("/v1/approvals/"):
                request_id = self.path.split("/v1/approvals/", 1)[1].strip("/")
                self._send_json(200, self.service.approval_status(request_id=request_id, actor_id=actor["actor_id"]))
                return
            if self.path == "/v1/audit/validate":
                self._send_json(200, self.service.validate_audit(actor_id=actor["actor_id"]))
                return
            if self.path.startswith("/v1/workflows/") and self.path.endswith("/versions"):
                workflow_id = self.path.split("/v1/workflows/", 1)[1].split("/versions", 1)[0]
                self._send_json(200, self.service.list_workflow_versions(workflow_id, actor_id=actor["actor_id"]))
                return

            self._send_json(404, {"error": "not_found"})
        except PermissionError as exc:
            self._send_json(401, {"error": str(exc)})
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self._send_json(500, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        try:
            actor = self._require_auth()
            body = self._read_json_body()

            if self.path == "/v1/policy/evaluate":
                self._send_json(200, self.service.evaluate_policy(body, actor_id=actor["actor_id"]))
                return
            if self.path == "/v1/approvals":
                self._send_json(200, self.service.create_approval(body, actor_id=actor["actor_id"]))
                return
            if self.path.startswith("/v1/approvals/") and self.path.endswith("/decision"):
                request_id = self.path.split("/v1/approvals/", 1)[1].split("/decision", 1)[0].strip("/")
                self._send_json(200, self.service.approval_decision(request_id, body, actor_id=actor["actor_id"]))
                return
            if self.path == "/v1/workflows/draft":
                self._send_json(200, self.service.save_workflow_draft(body, actor_id=actor["actor_id"]))
                return
            if self.path == "/v1/workflows/publish":
                self._send_json(200, self.service.publish_workflow(body, actor_id=actor["actor_id"]))
                return

            self._send_json(404, {"error": "not_found"})
        except PermissionError as exc:
            self._send_json(401, {"error": str(exc)})
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self._send_json(500, {"error": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep stdout clean for service embedding.
        _ = (fmt, args)

    def _require_auth(self) -> Dict[str, Any]:
        headers = {key.lower(): value for key, value in self.headers.items()}
        return self.auth.authenticate(headers)

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def run_http_server(
    service: ControlPlaneService,
    host: str = "127.0.0.1",
    port: int = 8787,
    auth_config: Optional[ApiAuthConfig] = None,
) -> None:
    config = auth_config or ApiAuthConfig(
        api_key=os.getenv("LAM_API_KEY", ""),
        bearer_secret=os.getenv("LAM_BEARER_SECRET", ""),
        bearer_issuer=os.getenv("LAM_BEARER_ISSUER", "lam-control-plane"),
        allow_anonymous_health=True,
    )
    auth = ApiAuth(config=config)

    class Handler(_RequestHandler):
        pass

    Handler.service = service
    Handler.auth = auth
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"LAM control plane listening on http://{host}:{port}")
    server.serve_forever()

