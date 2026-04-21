from __future__ import annotations

from typing import Dict, List


AI_BACKENDS: List[str] = [
    "deterministic-local",
    "openai-gpt-5.4",
    "openai-gpt-5.4-mini",
    "openai-gpt-5.4-nano",
]


def normalize_backend(value: str) -> str:
    v = (value or "deterministic-local").strip().lower()
    return v if v in AI_BACKENDS else "deterministic-local"


def backend_metadata(value: str) -> Dict[str, str]:
    backend = normalize_backend(value)
    desc = {
        "deterministic-local": "No token spend. Local deterministic executor only.",
        "openai-gpt-5.4": "Strongest reasoning. Highest token cost.",
        "openai-gpt-5.4-mini": "Balanced reasoning and cost.",
        "openai-gpt-5.4-nano": "Cheapest assisted mode.",
    }.get(backend, "Local deterministic executor.")
    return {"backend": backend, "description": desc}

