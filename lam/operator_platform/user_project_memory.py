from __future__ import annotations

from typing import Any, Dict, List

from .memory_store import MemoryStore
from .mission_contract import MissionContract


class UserProjectMemory:
    def __init__(self, store: MemoryStore | None = None) -> None:
        self.store = store or MemoryStore()

    def save_profile_fact(
        self,
        *,
        user_id: str,
        fact_type: str,
        content: Dict[str, Any],
        tags: List[str] | None = None,
        confidence: float = 0.9,
        sensitive: bool = False,
    ) -> str:
        return self.store.save_memory(
            {
                "type": fact_type,
                "scope": "user",
                "project_id": str(user_id),
                "content": dict(content),
                "tags": list(tags or []),
                "source": "user_profile",
                "confidence": confidence,
                "retrieval_policy": "sensitive_opt_in" if sensitive else "strict",
                "invalidation_keys": {},
            }
        )

    def save_project_context(
        self,
        *,
        project_id: str,
        context_type: str,
        content: Dict[str, Any],
        tags: List[str] | None = None,
        confidence: float = 0.8,
    ) -> str:
        return self.store.save_memory(
            {
                "type": context_type,
                "scope": "project",
                "project_id": str(project_id),
                "content": dict(content),
                "tags": list(tags or []),
                "source": "project_context",
                "confidence": confidence,
                "retrieval_policy": "strict",
                "invalidation_keys": {},
            }
        )

    def retrieve_for_mission(
        self,
        *,
        mission_contract: MissionContract,
        query: str,
        project_id: str = "",
        allow_sensitive: bool = False,
        limit: int = 8,
    ) -> Dict[str, Any]:
        payload = self.store.retrieve_relevant_memory(
            task_contract={
                "task_id": "",
                "user_goal": mission_contract.user_goal,
                "domain": mission_contract.domain,
                "audience": mission_contract.audience,
                "scope_dimensions": dict(mission_contract.scope_dimensions),
                "invalidation_keys": dict(mission_contract.invalidation_keys),
            },
            query=query,
            limit=limit,
            project_id=project_id,
        )
        filtered_used: List[Dict[str, Any]] = []
        filtered_rejected = list(payload.get("rejected", []) or [])
        for item in list(payload.get("used", []) or []):
            policy = str(item.get("retrieval_policy", "strict"))
            if policy == "sensitive_opt_in" and not allow_sensitive:
                filtered_rejected.append({"memory_id": item.get("memory_id", ""), "reason": "sensitive_memory_not_allowed", "type": item.get("type", "")})
                continue
            filtered_used.append(item)
        payload["used"] = filtered_used
        payload["rejected"] = filtered_rejected
        payload["retrieval_confidence"] = round(sum(float(item.get("retrieval_score", 0.0)) for item in filtered_used) / max(1, len(filtered_used)), 3)
        return payload
