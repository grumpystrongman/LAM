from __future__ import annotations

import csv
import io
import json
import re
import urllib.request
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .sample_data import SAMPLE_DUKE_STANDARD_CHARGES, build_synthetic_imaging_standard_charges, sample_source_manifest
from .schema import make_id, unique_rows

DEFAULT_SERVICE_KEYWORDS = (
    "mri",
    "magnetic resonance",
    "magnetic resonance imaging",
    "ct ",
    "ct-",
    "computed tomography",
    "cat scan",
    "colonoscopy",
    "emergency",
    "ultrasound",
    "x-ray",
    "xray",
    "radiology",
    "office visit",
    "evaluation and management",
    "visit",
    "arthroplasty",
    "heart",
    "transplant",
    "delivery",
    "c-section",
    "endoscopy",
)

KNOWN_PAYER_MARKERS = (
    "Aetna",
    "Anthem",
    "CareFirst",
    "Cigna",
    "Humana",
    "Innovation Health",
    "Kaiser",
    "Sentara Health",
    "UnitedHealthcare",
    "United",
)

def _durham_source_manifest() -> list[dict]:
    return [
        {
            "source_name": "Duke University Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": (
                "https://rca.centaurihs.com/ptapp/api/cdm/export/oneclick?"
                "recno=fad40f5dd5e6b2097ff0a8f592b095fac2e3a3eeef64efd5ca2b56f564a91e8e"
            ),
            "accessed_or_ingested_date": "",
            "geography": "Durham, NC",
            "notes": "Public Duke Health standard charges download.",
            "confidence": 0.92,
        },
        {
            "source_name": "Duke Regional Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": (
                "https://rca.centaurihs.com/ptapp/api/cdm/export/oneclick?"
                "recno=6218eb9ff7c607c1b86fc33c62f39d2663e9f7a51d87144995dfaa26b0ab9984"
            ),
            "accessed_or_ingested_date": "",
            "geography": "Durham, NC",
            "notes": "Public Duke Health standard charges download.",
            "confidence": 0.92,
        },
        {
            "source_name": "Duke University Hospital shoppable services catalog",
            "source_type": "shoppable_services_filters_json",
            "source_url_or_path": (
                "https://rca.centaurihs.com/ptapp/api/shoppable/services/filters?"
                "recno=fad40f5dd5e6b2097ff0a8f592b095fac2e3a3eeef64efd5ca2b56f564a91e8e"
            ),
            "accessed_or_ingested_date": "",
            "geography": "Durham, NC",
            "notes": "Public Duke shoppable-service filter catalog to improve consumer-friendly service matching.",
            "confidence": 0.88,
        },
        {
            "source_name": "Duke Regional Hospital shoppable services catalog",
            "source_type": "shoppable_services_filters_json",
            "source_url_or_path": (
                "https://rca.centaurihs.com/ptapp/api/shoppable/services/filters?"
                "recno=6218eb9ff7c607c1b86fc33c62f39d2663e9f7a51d87144995dfaa26b0ab9984"
            ),
            "accessed_or_ingested_date": "",
            "geography": "Durham, NC",
            "notes": "Public Duke Regional shoppable-service filter catalog for cleaner service names.",
            "confidence": 0.88,
        },
        {
            "source_name": "WakeMed Raleigh Campus and North Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": (
                "https://www.wakemed.org/sites/default/files/PricingTransparency/"
                "566017737_wakemed-raleigh-campus-and-north-hospital_standardcharges.csv"
            ),
            "accessed_or_ingested_date": "",
            "geography": "Raleigh-Durham, NC",
            "notes": "Public WakeMed machine-readable standard charges.",
            "confidence": 0.9,
        },
        {
            "source_name": "WakeMed Cary Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": (
                "https://www.wakemed.org/sites/default/files/PricingTransparency/"
                "566017737_wakemed-cary-hospital_standardcharges.csv"
            ),
            "accessed_or_ingested_date": "",
            "geography": "Raleigh-Durham, NC",
            "notes": "Public WakeMed Cary machine-readable standard charges.",
            "confidence": 0.9,
        },
        {
            "source_name": "Duke Health accepted insurance plans 2025",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.dukehealth.org/accepted-health-insurance-plans-2025",
            "accessed_or_ingested_date": "",
            "geography": "Durham, NC",
            "notes": (
                "Reference page for accepted insurance. Some automated fetches may be blocked; "
                "keep this URL in the manifest for human validation."
            ),
            "confidence": 0.7,
        },
        {
            "source_name": "Duke Health price transparency page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.dukehealth.org/paying-for-care/what-duke-charges-services",
            "accessed_or_ingested_date": "",
            "geography": "Durham, NC",
            "notes": "Landing page linking to Durham hospital machine-readable exports.",
            "confidence": 0.95,
        },
        {
            "source_name": "WakeMed price transparency page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.wakemed.org/patients-and-visitors/billing-and-insurance/price-transparency",
            "accessed_or_ingested_date": "",
            "geography": "Raleigh-Durham, NC",
            "notes": "Reference page for WakeMed transparency files used to widen Durham-area provider coverage.",
            "confidence": 0.95,
        },
        {
            "source_name": "UNC Health standard charges landing page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.unchealth.org/records-insurance/standard-charges",
            "accessed_or_ingested_date": "",
            "geography": "Raleigh-Durham, NC",
            "notes": "UNC standard charges landing page for additional regional provider coverage.",
            "confidence": 0.9,
        },
        {
            "source_name": "UNC Health standard charges model",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.unchealth.org/records-insurance/standard-charges.model.json",
            "accessed_or_ingested_date": "",
            "geography": "Raleigh-Durham, NC",
            "notes": "UNC page model exposing machine-readable hospital and shoppable-services references.",
            "confidence": 0.9,
        },
        {
            "source_name": "UNC Hospitals shoppable services page",
            "source_type": "reference_page",
            "source_url_or_path": "https://rca.centaurihs.com/ptapp/#d4ccc071fab9c79f17e52dc5b243ef668affc5e569aafa907c5b4c81f0a89284",
            "accessed_or_ingested_date": "",
            "geography": "Raleigh-Durham, NC",
            "notes": "UNC shoppable-services public reference for consumer-friendly exams and procedures.",
            "confidence": 0.86,
        },
        {
            "source_name": "Blue Cross NC transparency in coverage page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.bluecrossnc.com/about-us/policies-and-best-practices/transparency-coverage",
            "accessed_or_ingested_date": "",
            "geography": "North Carolina",
            "notes": "Payer transparency reference page for validation and future TIC ingestion.",
            "confidence": 0.82,
        },
        {
            "source_name": "Aetna transparency in coverage page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.aetna.com/individuals-families/member-rights-resources/transparency-in-coverage.html",
            "accessed_or_ingested_date": "",
            "geography": "National",
            "notes": "Payer transparency reference page for future TIC ingestion.",
            "confidence": 0.8,
        },
        {
            "source_name": "UnitedHealthcare transparency in coverage page",
            "source_type": "reference_page",
            "source_url_or_path": "https://transparency-in-coverage.uhc.com/",
            "accessed_or_ingested_date": "",
            "geography": "National",
            "notes": "Payer transparency reference page for future machine-readable ingestion.",
            "confidence": 0.8,
        },
        {
            "source_name": "Cigna transparency in coverage page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.cigna.com/legal/compliance/machine-readable-files",
            "accessed_or_ingested_date": "",
            "geography": "National",
            "notes": "Payer transparency reference page for future machine-readable ingestion.",
            "confidence": 0.8,
        },
    ]


def _fairfax_source_manifest() -> list[dict]:
    return [
        {
            "source_name": "Inova Fairfax Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": "https://www.inova.org/sites/default/files/patient_visitor/price_transparency/2026/540620889_inova-fairfax-hospital_standardcharges.csv",
            "accessed_or_ingested_date": "",
            "geography": "Fairfax, VA",
            "notes": "Public Inova Fairfax Hospital machine-readable standard charges for Fairfax-area outpatient imaging review.",
            "confidence": 0.93,
        },
        {
            "source_name": "Inova Fair Oaks Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": "https://www.inova.org/sites/default/files/patient_visitor/price_transparency/2026/540620889_inova-fair-oaks-hospital_standardcharges.csv",
            "accessed_or_ingested_date": "",
            "geography": "Fairfax, VA",
            "notes": "Public Inova Fair Oaks Hospital machine-readable standard charges for Fairfax-area outpatient imaging review.",
            "confidence": 0.92,
        },
        {
            "source_name": "Inova Alexandria Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": "https://www.inova.org/sites/default/files/patient_visitor/price_transparency/2026/540620889_inova-alexandria-hospital_standardcharges.csv",
            "accessed_or_ingested_date": "",
            "geography": "Northern Virginia, VA",
            "notes": "Public Inova Alexandria Hospital machine-readable standard charges to widen NoVA outpatient imaging coverage.",
            "confidence": 0.91,
        },
        {
            "source_name": "Inova Loudoun Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": "https://www.inova.org/sites/default/files/patient_visitor/price_transparency/2026/540620889_inova-loudoun-hospital_standardcharges.csv",
            "accessed_or_ingested_date": "",
            "geography": "Northern Virginia, VA",
            "notes": "Public Inova Loudoun Hospital machine-readable standard charges to widen NoVA outpatient imaging coverage.",
            "confidence": 0.91,
        },
        {
            "source_name": "Inova Mount Vernon Hospital standard charges",
            "source_type": "standard_charges_csv",
            "source_url_or_path": "https://www.inova.org/sites/default/files/patient_visitor/price_transparency/2026/540620889_inova-mount-vernon-hospital_standardcharges.csv",
            "accessed_or_ingested_date": "",
            "geography": "Northern Virginia, VA",
            "notes": "Public Inova Mount Vernon Hospital machine-readable standard charges to widen NoVA outpatient imaging coverage.",
            "confidence": 0.9,
        },
        {
            "source_name": "Inova hospital charges and price transparency page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.inova.org/patient-and-visitor-information/hospital-charges",
            "accessed_or_ingested_date": "",
            "geography": "Fairfax, VA",
            "notes": "Landing page for Inova machine-readable hospital charges and price transparency references.",
            "confidence": 0.95,
        },
        {
            "source_name": "Inova health plans page",
            "source_type": "insurance_reference_page",
            "source_url_or_path": "https://www.inova.org/patient-and-visitor-information/health-plans",
            "accessed_or_ingested_date": "",
            "geography": "Fairfax, VA",
            "notes": "Accepted insurance plans reference page for Inova facilities in Fairfax and Northern Virginia.",
            "confidence": 0.86,
        },
        {
            "source_name": "Aetna transparency in coverage page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.aetna.com/individuals-families/member-rights-resources/transparency-in-coverage.html",
            "accessed_or_ingested_date": "",
            "geography": "National",
            "notes": "Payer transparency reference page for future TIC ingestion filtered to Fairfax, VA evidence when available.",
            "confidence": 0.8,
        },
        {
            "source_name": "UnitedHealthcare transparency in coverage page",
            "source_type": "reference_page",
            "source_url_or_path": "https://transparency-in-coverage.uhc.com/",
            "accessed_or_ingested_date": "",
            "geography": "National",
            "notes": "Payer transparency reference page for future machine-readable ingestion filtered to Fairfax, VA evidence when available.",
            "confidence": 0.8,
        },
        {
            "source_name": "Cigna transparency in coverage page",
            "source_type": "reference_page",
            "source_url_or_path": "https://www.cigna.com/legal/compliance/machine-readable-files",
            "accessed_or_ingested_date": "",
            "geography": "National",
            "notes": "Payer transparency reference page for future machine-readable ingestion filtered to Fairfax, VA evidence when available.",
            "confidence": 0.8,
        },
    ]


def default_source_manifest(geography: str | None = None) -> list[dict]:
    low = str(geography or "").lower()
    if "fairfax" in low or low.endswith(", va") or low == "va" or "virginia" in low:
        return _fairfax_source_manifest()
    return _durham_source_manifest()


def write_default_manifest(path: str | Path, geography: str | None = None) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(default_source_manifest(geography), indent=2), encoding="utf-8")
    return target


def load_source_manifest(path: str | Path | None = None, offline: bool = False, geography: str | None = None) -> list[dict]:
    if offline:
        return sample_source_manifest()
    if path is None or str(path).strip() == "":
        return default_source_manifest(geography)
    target = Path(path)
    return json.loads(target.read_text(encoding="utf-8"))


def source_now() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _decode_text_bytes(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="replace")


def _remote_text_stream(url: str) -> io.TextIOBase:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )
        },
    )
    response = urllib.request.urlopen(request, timeout=120)
    payload = response.read()
    return io.StringIO(_decode_text_bytes(payload))


def _local_text_stream(path: str | Path) -> io.TextIOBase:
    return io.StringIO(_decode_text_bytes(Path(path).read_bytes()))


def parse_insurance_reference_page_text(
    payload: str,
    *,
    source_name: str,
    source_url: str,
    geography: str,
    default_confidence: float = 0.8,
) -> dict[str, list[dict] | list[str]]:
    text = re.sub(r"<[^>]+>", " ", payload or "")
    text = re.sub(r"\s+", " ", text)
    lowered = text.lower()
    payers: list[dict] = []
    plans: list[dict] = []
    issues: list[str] = []
    network_name = source_name.replace(" page", "").strip()
    found_any = False
    for marker in KNOWN_PAYER_MARKERS:
        if marker.lower() not in lowered:
            continue
        found_any = True
        payer_name = "UnitedHealthcare" if marker == "United" and "unitedhealthcare" in lowered else marker
        payer_id = make_id("payer", payer_name)
        plan_id = make_id("plan", payer_name, geography, network_name)
        payers.append(
            {
                "payer_id": payer_id,
                "payer_name": payer_name,
                "source_url": source_url,
                "source_type": source_name,
                "confidence": default_confidence,
                "notes": f"Observed on {source_name} as an accepted or referenced insurance plan.",
            }
        )
        plans.append(
            {
                "plan_id": plan_id,
                "payer_id": payer_id,
                "plan_name": f"{payer_name} accepted plans",
                "plan_type": "accepted_plans_reference",
                "market_segment": "provider_network_reference",
                "geography": geography,
                "network_name": network_name,
                "source_url": source_url,
                "confidence": default_confidence,
                "notes": f"Accepted plan reference from {source_name}.",
            }
        )
    if not found_any:
        issues.append(f"No payer markers were detected on {source_name}.")
    return {"payers": unique_rows(payers, "payer_id"), "plans": unique_rows(plans, "plan_id"), "services": [], "rates": [], "issues": issues}


def service_matches(description: str, keywords: Iterable[str] | None) -> bool:
    if not keywords:
        return True
    haystack = (description or "").strip().lower()
    return any(keyword.lower() in haystack for keyword in keywords)


def matching_keywords(description: str, keywords: Iterable[str] | None) -> list[str]:
    if not keywords:
        return []
    haystack = (description or "").strip().lower()
    return [keyword.lower() for keyword in keywords if keyword.lower() in haystack]


def extract_payer_columns(header: list[str]) -> dict[tuple[str, str], dict[str, int]]:
    payer_map: dict[tuple[str, str], dict[str, int]] = {}
    for idx, column in enumerate(header):
        if not column:
            continue
        parts = column.split("|")
        if len(parts) < 3:
            continue
        root = parts[0]
        tail = parts[-1]
        if root == "standard_charge" and tail in {"negotiated_dollar", "methodology"}:
            payer = parts[1].strip()
            plan = parts[2].strip() if len(parts) > 3 else "Standard"
            payer_map.setdefault((payer, plan), {})[tail] = idx
        elif root in {"median_amount", "10th_percentile", "90th_percentile", "count"}:
            payer = parts[1].strip()
            plan = parts[2].strip() if len(parts) > 3 else "Standard"
            payer_map.setdefault((payer, plan), {})[root] = idx
    return payer_map


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _row_value(row: list[str], idx: int | None) -> str:
    if idx is None or idx >= len(row):
        return ""
    return row[idx].strip()


def _service_identity(row: list[str], header_index: dict[str, int]) -> tuple[str, str, str]:
    for code_slot in ("1", "2", "3"):
        code = _row_value(row, header_index.get(f"code|{code_slot}"))
        code_type = _row_value(row, header_index.get(f"code|{code_slot}|type"))
        if code:
            return code_type or "unknown", code, code_slot
    return "unknown", "", ""


def _category_for(description: str, code_type: str, setting: str) -> str:
    lowered = (description or "").lower()
    if (
        "mri" in lowered
        or "magnetic resonance" in lowered
        or "ct" in lowered
        or "computed tomography" in lowered
        or "x-ray" in lowered
        or "xray" in lowered
        or "ultrasound" in lowered
    ):
        return "imaging"
    if "colonoscopy" in lowered or "endoscopy" in lowered:
        return "endoscopy"
    if "emergency" in lowered:
        return "emergency"
    if "transplant" in lowered or "heart" in lowered:
        return "cardiac_transplant"
    if (setting or "").lower() == "inpatient":
        return "inpatient"
    return code_type.lower() or "other"


def parse_duke_standard_charges_text(
    text: str,
    *,
    source_name: str,
    source_url: str,
    geography: str,
    default_confidence: float = 0.9,
    service_keywords: Iterable[str] | None = None,
    max_services: int | None = None,
) -> dict[str, list[dict] | list[str]]:
    reader = csv.reader(io.StringIO(text))
    return _parse_duke_standard_charges_reader(
        reader,
        source_name=source_name,
        source_url=source_url,
        geography=geography,
        default_confidence=default_confidence,
        service_keywords=service_keywords,
        max_services=max_services,
    )


def _parse_duke_standard_charges_reader(
    reader: Iterator[list[str]],
    *,
    source_name: str,
    source_url: str,
    geography: str,
    default_confidence: float,
    service_keywords: Iterable[str] | None,
    max_services: int | None,
) -> dict[str, list[dict] | list[str]]:
    meta_header = next(reader)
    meta_row = next(reader)
    service_header = next(reader)
    meta = dict(zip(meta_header, meta_row))
    header_index = {value: idx for idx, value in enumerate(service_header)}
    payer_columns = extract_payer_columns(service_header)

    hospital_name = (meta.get("location_name") or meta.get("hospital_name") or source_name).strip()
    payers: list[dict] = []
    plans: list[dict] = []
    services: list[dict] = []
    rates: list[dict] = []
    issues: list[str] = []
    kept_services = 0
    keyword_list = [keyword.strip().lower() for keyword in (service_keywords or []) if keyword.strip()]
    quota_per_keyword = (
        max(1, (max_services or max(1, len(keyword_list))) // max(1, len(keyword_list)))
        if keyword_list
        else 0
    )
    keyword_hits = {keyword: 0 for keyword in keyword_list}

    for row_num, row in enumerate(reader, start=4):
        description = _row_value(row, header_index.get("description"))
        if not description:
            continue
        matched = matching_keywords(description, keyword_list)
        if keyword_list and not matched:
            continue
        all_quotas_met = not keyword_hits or all(count >= quota_per_keyword for count in keyword_hits.values())
        under_quota = any(keyword_hits[keyword] < quota_per_keyword for keyword in matched)
        if max_services and kept_services >= max_services and all_quotas_met:
            break
        if keyword_list and not under_quota and not all_quotas_met:
            continue

        code_type, code, _ = _service_identity(row, header_index)
        setting = _row_value(row, header_index.get("setting"))
        billing_class = _row_value(row, header_index.get("billing_class"))
        service_id = make_id("service", hospital_name, code_type, code or description)
        services.append(
            {
                "service_id": service_id,
                "code_type": code_type,
                "code": code,
                "description": description,
                "category": _category_for(description, code_type, setting),
                "source_url": source_url,
            }
        )
        for (payer_name, plan_name), indexes in payer_columns.items():
            negotiated_rate = _as_float(_row_value(row, indexes.get("negotiated_dollar")))
            if negotiated_rate is None:
                continue
            payer_id = make_id("payer", payer_name)
            plan_id = make_id("plan", payer_name, plan_name)
            payers.append(
                {
                    "payer_id": payer_id,
                    "payer_name": payer_name,
                    "source_url": source_url,
                    "source_type": source_name,
                    "confidence": default_confidence,
                    "notes": f"Derived from {hospital_name} payer-specific negotiated charges.",
                }
            )
            plans.append(
                {
                    "plan_id": plan_id,
                    "payer_id": payer_id,
                    "plan_name": plan_name,
                    "plan_type": plan_name,
                    "market_segment": "hospital_contract",
                    "geography": geography,
                    "network_name": hospital_name,
                    "source_url": source_url,
                    "confidence": default_confidence,
                    "notes": f"Observed in {hospital_name} standard charges.",
                }
            )
            methodology = _row_value(row, indexes.get("methodology"))
            percentile_count = _row_value(row, indexes.get("count"))
            confidence = default_confidence
            if percentile_count in {"", "0"}:
                confidence = round(default_confidence * 0.85, 2)
            rate_id = make_id("rate", hospital_name, payer_name, plan_name, service_id, str(row_num))
            rates.append(
                {
                    "rate_id": rate_id,
                    "payer_id": payer_id,
                    "plan_id": plan_id,
                    "service_id": service_id,
                    "provider_name": hospital_name,
                    "provider_npi": meta.get("type_2_npi", ""),
                    "facility_name": hospital_name,
                    "negotiated_rate": negotiated_rate,
                    "allowed_amount": _as_float(_row_value(row, indexes.get("median_amount"))),
                    "cash_price": _as_float(_row_value(row, header_index.get("standard_charge|discounted_cash"))),
                    "billing_class": billing_class,
                    "setting": setting,
                    "geography": geography,
                    "effective_date": meta.get("last_updated_on", ""),
                    "source_url": source_url,
                    "raw_reference": f"{source_name}:row_{row_num}",
                    "confidence": confidence,
                    "methodology": methodology,
                    "percentile_count": percentile_count,
                    "gross_charge": _as_float(_row_value(row, header_index.get("standard_charge|gross"))),
                }
            )
        kept_services += 1
        for keyword in matched:
            keyword_hits[keyword] += 1
        all_quotas_met = not keyword_hits or all(count >= quota_per_keyword for count in keyword_hits.values())
        if max_services and kept_services >= max_services and all_quotas_met:
            break

    if not rates:
        issues.append(f"No negotiated rate rows were found for {source_name}.")
    return {
        "payers": unique_rows(payers, "payer_id"),
        "plans": unique_rows(plans, "plan_id"),
        "services": unique_rows(services, "service_id"),
        "rates": rates,
        "issues": issues,
    }


def parse_shoppable_services_filters_text(
    text: str,
    *,
    source_name: str,
    source_url: str,
    geography: str,
    default_confidence: float = 0.85,
) -> dict[str, list[dict] | list[str]]:
    payload = json.loads(text)
    services: list[dict] = []
    issues: list[str] = []
    seen: set[tuple[str, str]] = set()

    def visit(node: object) -> None:
        if isinstance(node, dict):
            code = str(node.get("value") or node.get("code") or node.get("id") or "").strip()
            description = str(node.get("label") or node.get("name") or node.get("description") or "").strip()
            if description:
                key = (code, description.lower())
                if key not in seen:
                    seen.add(key)
                    code_type = "CPT" if code.isdigit() else "shoppable_service"
                    services.append(
                        {
                            "service_id": make_id("service", source_name, code_type, code or description),
                            "code_type": code_type,
                            "code": code,
                            "description": description,
                            "category": _category_for(description, code_type, "outpatient"),
                            "source_url": source_url,
                        }
                    )
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(payload)
    if not services:
        issues.append(f"No shoppable service rows were found for {source_name}.")
    return {"payers": [], "plans": [], "services": services, "rates": [], "issues": issues}


def ingest_sources(
    sources: list[dict],
    *,
    service_keywords: Iterable[str] | None = DEFAULT_SERVICE_KEYWORDS,
    max_services_per_source: int = 18,
    offline_fallback: bool = True,
) -> dict[str, list[dict] | list[str]]:
    merged = {"payers": [], "plans": [], "services": [], "rates": [], "issues": [], "manifest": []}
    fallback_used = False
    for source in sources:
        manifest_row = dict(source)
        manifest_row["accessed_or_ingested_date"] = manifest_row.get("accessed_or_ingested_date") or source_now()
        merged["manifest"].append(manifest_row)
        if source["source_type"] not in {"standard_charges_csv", "duke_standard_charges_csv", "synthetic_fixture", "shoppable_services_filters_json", "insurance_reference_page"}:
            continue
        try:
            if source["source_type"] == "synthetic_fixture":
                fixture_variant = str(source.get("fixture_variant", "") or "").strip().lower()
                if not fixture_variant:
                    source_name_low = str(source.get("source_name", "") or "").lower()
                    source_geo_low = str(source.get("geography", "") or "").lower()
                    source_url = str(source.get("source_url_or_path", "") or "").lower()
                    if "duke" in source_name_low or "duke" in source_url or "durham" in source_geo_low:
                        fixture_variant = "duke_sample"
                    else:
                        fixture_variant = "imaging_demo"
                synthetic_text = SAMPLE_DUKE_STANDARD_CHARGES
                if fixture_variant == "imaging_demo":
                    geography = str(source.get("geography", "Fairfax, VA") or "Fairfax, VA")
                    parts = [item.strip() for item in geography.split(",", 1)]
                    city = parts[0] if parts else "Fairfax"
                    state = parts[1] if len(parts) > 1 else "VA"
                    facility_name = str(source.get("facility_name", "") or f"{city} Imaging Center").strip()
                    synthetic_text = build_synthetic_imaging_standard_charges(
                        facility_name=facility_name,
                        city=city,
                        state=state,
                    )
                bundle = parse_duke_standard_charges_text(
                    synthetic_text,
                    source_name=source["source_name"],
                    source_url=source["source_url_or_path"],
                    geography=source.get("geography", "Durham, NC"),
                    default_confidence=source.get("confidence", 0.45),
                    service_keywords=service_keywords,
                    max_services=max_services_per_source,
                )
            elif source["source_type"] == "shoppable_services_filters_json":
                if str(source["source_url_or_path"]).startswith(("http://", "https://")):
                    with closing(_remote_text_stream(source["source_url_or_path"])) as handle:
                        bundle = parse_shoppable_services_filters_text(
                            handle.read(),
                            source_name=source["source_name"],
                            source_url=source["source_url_or_path"],
                            geography=source.get("geography", "Durham, NC"),
                            default_confidence=source.get("confidence", 0.85),
                        )
                else:
                    bundle = parse_shoppable_services_filters_text(
                        Path(source["source_url_or_path"]).read_text(encoding="utf-8"),
                        source_name=source["source_name"],
                        source_url=str(source["source_url_or_path"]),
                        geography=source.get("geography", "Durham, NC"),
                        default_confidence=source.get("confidence", 0.85),
                    )
            elif source["source_type"] == "insurance_reference_page":
                if str(source["source_url_or_path"]).startswith(("http://", "https://")):
                    with closing(_remote_text_stream(source["source_url_or_path"])) as handle:
                        bundle = parse_insurance_reference_page_text(
                            handle.read(),
                            source_name=source["source_name"],
                            source_url=source["source_url_or_path"],
                            geography=source.get("geography", "Fairfax, VA"),
                            default_confidence=source.get("confidence", 0.8),
                        )
                else:
                    bundle = parse_insurance_reference_page_text(
                        Path(source["source_url_or_path"]).read_text(encoding="utf-8"),
                        source_name=source["source_name"],
                        source_url=str(source["source_url_or_path"]),
                        geography=source.get("geography", "Fairfax, VA"),
                        default_confidence=source.get("confidence", 0.8),
                    )
            elif str(source["source_url_or_path"]).startswith(("http://", "https://")):
                with closing(_remote_text_stream(source["source_url_or_path"])) as handle:
                    reader = csv.reader(handle)
                    bundle = _parse_duke_standard_charges_reader(
                        reader,
                        source_name=source["source_name"],
                        source_url=source["source_url_or_path"],
                        geography=source.get("geography", "Durham, NC"),
                        default_confidence=source.get("confidence", 0.9),
                        service_keywords=service_keywords,
                        max_services=max_services_per_source,
                    )
            else:
                with closing(_local_text_stream(source["source_url_or_path"])) as handle:
                    reader = csv.reader(handle)
                    bundle = _parse_duke_standard_charges_reader(
                        reader,
                        source_name=source["source_name"],
                        source_url=str(source["source_url_or_path"]),
                        geography=source.get("geography", "Durham, NC"),
                        default_confidence=source.get("confidence", 0.9),
                        service_keywords=service_keywords,
                        max_services=max_services_per_source,
                    )
        except Exception as exc:
            merged["issues"].append(f"{source['source_name']}: {exc}")
            if not offline_fallback:
                continue
            if fallback_used:
                continue
            bundle = parse_duke_standard_charges_text(
                SAMPLE_DUKE_STANDARD_CHARGES,
                source_name=f"{source['source_name']} (offline fallback)",
                source_url="sample://duke_standard_charges",
                geography=source.get("geography", "Durham, NC"),
                default_confidence=0.45,
                service_keywords=service_keywords,
                max_services=max_services_per_source,
            )
            fallback_used = True
        for key in ("payers", "plans", "services", "rates", "issues"):
            merged[key].extend(bundle[key])
    merged["payers"] = unique_rows(merged["payers"], "payer_id")
    merged["plans"] = unique_rows(merged["plans"], "plan_id")
    merged["services"] = unique_rows(merged["services"], "service_id")
    return merged


def write_table(path: str | Path, rows: list[dict], fieldnames: list[str]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    return target


def write_ingestion_outputs(
    workspace: str | Path,
    bundle: dict[str, list[dict] | list[str]],
    *,
    geography_label: str = "",
) -> dict[str, Path]:
    root = Path(workspace)
    normalized_dir = root / "normalized"
    artifacts_dir = root / "artifacts"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        "payers_csv": write_table(
            normalized_dir / "payers.csv",
            bundle["payers"],
            ["payer_id", "payer_name", "source_url", "source_type", "confidence", "notes"],
        ),
        "plans_csv": write_table(
            normalized_dir / "plans.csv",
            bundle["plans"],
            [
                "plan_id",
                "payer_id",
                "plan_name",
                "plan_type",
                "market_segment",
                "geography",
                "network_name",
                "source_url",
                "confidence",
                "notes",
            ],
        ),
        "services_csv": write_table(
            normalized_dir / "services.csv",
            bundle["services"],
            ["service_id", "code_type", "code", "description", "category", "source_url"],
        ),
        "rates_csv": write_table(
            normalized_dir / "rates.csv",
            bundle["rates"],
            [
                "rate_id",
                "payer_id",
                "plan_id",
                "service_id",
                "provider_name",
                "provider_npi",
                "facility_name",
                "negotiated_rate",
                "allowed_amount",
                "cash_price",
                "billing_class",
                "setting",
                "geography",
                "effective_date",
                "source_url",
                "raw_reference",
                "confidence",
                "methodology",
                "percentile_count",
                "gross_charge",
            ],
        ),
        "source_manifest_csv": write_table(
            artifacts_dir / "source_manifest.csv",
            bundle["manifest"],
            ["source_name", "source_type", "source_url_or_path", "accessed_or_ingested_date", "geography", "notes", "confidence"],
        ),
    }
    report_path = artifacts_dir / "data_quality_report.md"
    issue_lines = "\n".join(f"- {issue}" for issue in bundle["issues"]) or "- No ingestion issues recorded."
    report_path.write_text(
        (
            "# Data Quality Report\n\n"
            f"- Generated: {source_now()}\n"
            + (f"- Geography: {geography_label}\n" if geography_label else "")
            + f"- Payers: {len(bundle['payers'])}\n"
            + f"- Plans: {len(bundle['plans'])}\n"
            + f"- Services: {len(bundle['services'])}\n"
            + f"- Rates: {len(bundle['rates'])}\n\n"
            + "## Issues\n\n"
            + f"{issue_lines}\n"
        ),
        encoding="utf-8",
    )
    outputs["data_quality_report_md"] = report_path
    return outputs
