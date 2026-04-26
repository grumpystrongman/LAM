from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

from lam.interface.local_vector_store import LocalVectorStore

from .analyze import read_csv_rows

SERVICE_SYNONYMS = {
    "mri": ["mri", "magnetic resonance", "magnetic resonance imaging"],
    "ct": ["ct", "computed tomography", "cat scan"],
    "xray": ["xray", "x-ray", "radiograph"],
    "ultrasound": ["ultrasound", "sonography"],
    "office": ["office visit", "clinic visit", "evaluation and management"],
}


def index_path(workspace: str | Path) -> Path:
    return Path(workspace) / "artifacts" / "rag_index" / "payer_rag.db"


def _artifact_csv(root: Path, generic_name: str, glob_pattern: str) -> Path:
    direct = root / "artifacts" / generic_name
    if direct.exists():
        return direct
    matches = sorted((root / "artifacts").glob(glob_pattern))
    return matches[0] if matches else direct


def _workspace_geography(root: Path) -> str:
    contract_path = root / "task_contract.json"
    if contract_path.exists():
        try:
            payload = json.loads(contract_path.read_text(encoding="utf-8"))
            geography = str(payload.get("geography", "")).strip()
            if geography:
                return geography
        except Exception:
            pass
    return "the current market"


def _clear_index(path: Path, app_name: str) -> None:
    if not path.exists():
        return
    conn = sqlite3.connect(path)
    try:
        conn.execute("DELETE FROM knowledge_docs WHERE app_name = ?", (app_name,))
        conn.commit()
    finally:
        conn.close()


def build_index(workspace: str | Path, app_name: str = "durham_payer_rag") -> Path:
    root = Path(workspace)
    payers = read_csv_rows(root / "normalized" / "payers.csv")
    plans = read_csv_rows(root / "normalized" / "plans.csv")
    services = read_csv_rows(root / "normalized" / "services.csv")
    rates = read_csv_rows(root / "normalized" / "rates.csv")
    manifest = read_csv_rows(_artifact_csv(root, "source_manifest.csv", "*_source_manifest.csv"))
    candidates_path = _artifact_csv(root, "outreach_candidates.csv", "*_outreach_candidates.csv")
    candidates = read_csv_rows(candidates_path) if candidates_path.exists() else []

    target = index_path(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    store = LocalVectorStore(path=target)
    _clear_index(target, app_name)

    for row in manifest:
        store.add_document(
            app_name=app_name,
            source_url=row["source_url_or_path"],
            title=f"Source: {row['source_name']}",
            content=json.dumps(row, sort_keys=True),
        )
    for row in payers:
        store.add_document(
            app_name=app_name,
            source_url=row["source_url"],
            title=f"Payer: {row['payer_name']}",
            content=f"Payer {row['payer_name']} confidence {row['confidence']}. Notes: {row['notes']}",
        )
    for row in plans:
        store.add_document(
            app_name=app_name,
            source_url=row["source_url"],
            title=f"Plan: {row['plan_name']}",
            content=(
                f"Plan {row['plan_name']} under payer {row['payer_id']} in {row['geography']} "
                f"network {row['network_name']} market_segment {row['market_segment']}."
            ),
        )
    service_lookup = {row["service_id"]: row for row in services}
    for row in rates[:600]:
        service = service_lookup.get(row["service_id"], {})
        store.add_document(
            app_name=app_name,
            source_url=row["source_url"],
            title=f"Rate: {service.get('description', row['service_id'])}",
            content=(
                f"{service.get('description', '')} code {service.get('code', '')} "
                f"facility {row['facility_name']} negotiated_rate {row['negotiated_rate']} "
                f"cash_price {row['cash_price']} setting {row['setting']} raw_reference {row['raw_reference']}"
            ),
        )
    for row in candidates[:300]:
        store.add_document(
            app_name=app_name,
            source_url=row["source_evidence"].split("|")[0].strip(),
            title=f"Candidate: {row['payer_name']} {row['plan_name']}",
            content=(
                f"{row['payer_name']} {row['plan_name']} flagged for {row['service']}. "
                f"payer_rate {row['payer_rate']} peer_median {row['peer_median']} "
                f"variance_percent {row['variance_percent']} evidence {row['source_evidence']}"
            ),
        )
    return target


def _expanded_query_terms(lowered_question: str) -> list[str]:
    base_terms = re.findall(r"[a-z0-9]+", lowered_question)
    expanded: list[str] = []
    for token in base_terms:
        expanded.append(token)
        if token in SERVICE_SYNONYMS:
            expanded.extend(SERVICE_SYNONYMS[token])
    if "magnetic resonance imaging" in lowered_question:
        expanded.append("mri")
    if "computed tomography" in lowered_question or "cat scan" in lowered_question:
        expanded.append("ct")
    return list(dict.fromkeys(term for term in expanded if term))


def ask_question(workspace: str | Path, question: str, top_k: int = 5) -> dict:
    root = Path(workspace)
    geography_label = _workspace_geography(root)
    candidates = read_csv_rows(_artifact_csv(root, "outreach_candidates.csv", "*_outreach_candidates.csv"))
    rates = read_csv_rows(root / "normalized" / "rates.csv")
    services = read_csv_rows(root / "normalized" / "services.csv")
    plans = read_csv_rows(root / "normalized" / "plans.csv")
    payers = read_csv_rows(root / "normalized" / "payers.csv")
    service_lookup = {row["service_id"]: row for row in services}
    plan_lookup = {row["plan_id"]: row for row in plans}
    payer_lookup = {row["payer_id"]: row for row in payers}
    lowered = question.lower()
    sources: list[str] = []
    if ("evidence" in lowered or "why" in lowered) and candidates:
        payer_names = sorted({row["payer_name"] for row in candidates}, key=len, reverse=True)
        matched_payer = next((name for name in payer_names if name.lower() in lowered), "")
        if matched_payer:
            payer_rows = [row for row in candidates if row["payer_name"].lower() == matched_payer.lower()]
            top = payer_rows[:5]
            answer = [
                f"{row['payer_name']} / {row['plan_name']} / {row['service']} "
                f"({float(row['variance_percent']) * 100:.1f}% above peer median)"
                for row in top
            ]
            sources = [row["source_evidence"] for row in top]
            text = "\n".join(answer) if answer else f"No flagged rows were found for {matched_payer}."
            return {"answer": text, "sources": sources}

    if "outlier" in lowered or "need outreach" in lowered or "flagged" in lowered:
        top = candidates[:5]
        answer = [
            f"{row['priority_rank']}. {row['payer_name']} / {row['plan_name']} / {row['service']} "
            f"({float(row['variance_percent']) * 100:.1f}% above peer median)"
            for row in top
        ]
        sources = [row["source_evidence"] for row in top]
        text = "\n".join(answer) if answer else "No pricing outlier candidates were identified in the current workspace."
        return {"answer": text, "sources": sources}

    stopwords = {"which", "plans", "plan", "most", "expensive", "show", "need", "outreach", "for", "the", "and", "durham", "what", "are"}
    query_terms = [token for token in _expanded_query_terms(lowered) if len(token) > 2 and token not in stopwords]
    service_matches = [row for row in services if any(token in row["description"].lower() for token in query_terms)]
    requested_services = [
        term
        for term in query_terms
        if term
        in {
            "mri",
            "magnetic resonance",
            "magnetic resonance imaging",
            "ct",
            "computed tomography",
            "cat scan",
            "colonoscopy",
            "ultrasound",
            "xray",
            "x-ray",
            "endoscopy",
            "emergency",
            "office",
            "office visit",
        }
    ]
    if ("most expensive" in lowered or "highest" in lowered) and service_matches:
        service_ids = {row["service_id"] for row in service_matches}
        relevant = [row for row in rates if row["service_id"] in service_ids]
        relevant.sort(key=lambda row: float(row.get("negotiated_rate") or 0), reverse=True)
        lines = []
        for row in relevant[:5]:
            service = service_lookup[row["service_id"]]
            plan = plan_lookup.get(row["plan_id"], {})
            payer = payer_lookup.get(row["payer_id"], {})
            lines.append(
                f"{payer.get('payer_name', row['payer_id'])} / {plan.get('plan_name', row['plan_id'])} "
                f"at {row['facility_name']}: ${float(row['negotiated_rate']):,.2f} for {service['description']}"
            )
            sources.append(f"{row['source_url']} | {row['raw_reference']}")
        return {
            "answer": "\n".join(lines) if lines else "No matching priced services were found.",
            "sources": sources,
        }
    if ("most expensive" in lowered or "highest" in lowered) and requested_services and not service_matches:
            return {
                "answer": (
                    "No exact service match was found in the current public corpus for "
                    f"{', '.join(sorted(set(requested_services)))}. The current build is based on Duke standard-charge rows, "
                    f"for {geography_label}, which may expose device or supply descriptions instead of a consumer-friendly exam label. "
                    "Add Duke shoppable-services data or expand the source manifest for a cleaner service-level answer."
                ),
                "sources": [str(_artifact_csv(root, "source_manifest.csv", "*_source_manifest.csv"))],
            }

    app_name = "durham_payer_rag"
    store = LocalVectorStore(path=index_path(root))
    hits = store.search(app_name=app_name, query=question, top_k=top_k)
    lines = [f"{idx + 1}. {hit['title']} (score {hit['score']:.3f})" for idx, hit in enumerate(hits)]
    sources = [hit["source_url"] for hit in hits]
    return {
        "answer": "\n".join(lines) if lines else "No relevant indexed content was found.",
        "sources": sources,
        "matches": hits,
    }
