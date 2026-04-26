from __future__ import annotations

from textwrap import dedent


SAMPLE_DUKE_STANDARD_CHARGES = dedent(
    """
    "hospital_name","last_updated_on","version","location_name","hospital_address","license_number|NC","type_2_npi","attestation","attester_name"
    "DUKE UNIVERSITY HEALTH SYSTEM, INC.  Doing Business As: DUKE UNIVERSITY HOSPITAL ","04/01/2026","3.0.0","Duke University Hospital","2301 Erwin Rd, Durham, NC 27710","H0015|NC","1992703540","TRUE","Lisa Goodlett"
    description,code|1,code|1|type,billing_class,setting,standard_charge|gross,standard_charge|discounted_cash,standard_charge|Aetna|Commercial/HMO/PPO/POS|negotiated_dollar,standard_charge|BCBS|Commercial/HMO/PPO/Select|negotiated_dollar,standard_charge|Cigna|Commercial/HMO/PPO|negotiated_dollar,standard_charge|United Healthcare|Commercial/EPO/PPO|negotiated_dollar,standard_charge|Wellcare|Managed Medicaid|negotiated_dollar
    "MRI brain without contrast","70551","CPT","professional","outpatient","2500","1850","1425","1660","1510","1895","980"
    "CT abdomen and pelvis with contrast","74177","CPT","professional","outpatient","4300","3025","2210","2580","2475","3095","1660"
    "Colonoscopy diagnostic","45378","CPT","professional","outpatient","5200","3495","2450","2840","2695","3395","1875"
    "Emergency department visit high severity","99285","CPT","professional","outpatient","1950","1210","840","1095","990","1325","615"
    "Acute major eye infections without cc/mcc","122","MS-DRG","facility","inpatient","13271.14","3583.21","4237","5837.19","5295.22","6341.25","3234.24"
    """
).strip()


def sample_source_manifest() -> list[dict]:
    return [
        {
            "source_name": "Duke University Hospital standard charges sample",
            "source_type": "synthetic_fixture",
            "source_url_or_path": "sample://duke_standard_charges",
            "accessed_or_ingested_date": "",
            "geography": "Durham, NC",
            "notes": (
                "Synthetic fixture shaped like the Duke public standard-charge export. "
                "Use real public source URLs for production analysis."
            ),
            "confidence": 0.45,
        }
    ]
