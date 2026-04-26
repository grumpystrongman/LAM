# Durham Payer RAG

This workflow builds a Durham, NC payer-plan pricing review from public provider transparency files, shoppable-service references, and payer transparency reference pages.

## UI Usage

Open the OpenLAMb UI and ask:

```text
Review Durham, NC payer pricing, build a RAG index, create the stakeholder workbook, and show me which plans need outreach.
```

The workflow now creates a geography-specific run folder and validates that final artifacts match the current request before completion.

Follow-up questions can stay in chat:

```text
Which plans are most expensive for MRI in Durham?
Show evidence for why United Healthcare was flagged.
What data sources support this recommendation?
```

## Commands

Initialize a manifest if you want to edit sources:

```powershell
python -m lam.main payer-init-manifest --path data/payer_rag/source_manifest.json
```

Run the end-to-end build:

```powershell
python -m lam.main payer-build --workspace data/payer_rag_live --max-services-per-source 12 --service-keywords "mri,ct,colonoscopy,emergency,ultrasound,x-ray,office visit,endoscopy,heart,transplant"
```

Ask a question:

```powershell
python -m lam.main payer-ask --workspace data/payer_rag_live --question "Which plans need outreach?"
```

## Outputs

Generated artifacts land in `data/payer_rag_live/artifacts/`:

- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_payer_outreach_candidates.xlsx`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_payer_dashboard.html`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_summary_report.md`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_source_manifest.csv`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_data_quality_report.md`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_contract_validation_queue.csv`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_validation_checklist.md`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/rag_index/payer_rag.db`
- `data/payer_rag_runs/<geography>_<timestamp>/artifacts/<geography>_geography_validation.md`
- `data/payer_rag_runs/<geography>_<timestamp>/task_contract.json`

Normalized tables land in `data/payer_rag_live/normalized/`.

## Data Notes

- Uses public Duke Health, WakeMed, and regional payer/provider reference sources where available.
- Keeps Duke, UNC, WakeMed, and payer transparency pages in the source manifest for human review even when automated ingestion is limited.
- Uses cautious language. Flags are potential pricing outliers that require contract and claim validation before outreach.
- If remote source retrieval fails, the pipeline can fall back to a clearly labeled synthetic fixture.
- If the requested geography does not match the available source set, the geography consistency gate blocks completion instead of returning stale artifacts from another market.
