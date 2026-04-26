## Durham Payer RAG

This module builds a Durham, NC payer-plan pricing review from public Duke Health standard-charge files.

Primary commands:

- `python -m lam.main payer-init-manifest --path data/payer_rag/source_manifest.json`
- `python -m lam.main payer-build --workspace data/payer_rag`
- `python -m lam.main payer-ask --workspace data/payer_rag --question "Which plans need outreach?"`

Outputs land in `data/payer_rag/artifacts/`.
