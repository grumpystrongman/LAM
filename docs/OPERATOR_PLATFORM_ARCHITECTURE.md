# OpenLAMb Operator Platform Architecture

## Current Architecture Map

- Entry points: `lam/main.py`
- Frontend/UI: `lam/interface/web_ui.py`
- Backend/server: `lam/services/api_server.py`, `lam/interface/web_ui.py`
- Planner logic: `lam/interface/search_agent.py`, `lam/interface/domain_playbooks.py`, `lam/interface/operator_contract.py`
- Executor/tool logic: `lam/interface/search_agent.py`, `lam/interface/desktop_sequence.py`, `lam/adapters/*`
- Verification logic: `lam/interface/operator_contract.py`, `lam/interface/human_judgment.py`
- Playbooks/flows: `lam/interface/domain_playbooks.py`, `lam/payer_rag/workflow.py`, `lam/deep_workbench/workflow.py`
- Artifact generation: `lam/interface/search_agent.py`, `lam/payer_rag/export.py`
- Tests: `tests/unit/*`, `tests/integration/*`, `tests/security/*`
- Memory/state: `lam/interface/session_manager.py`, `lam/interface/user_defaults.py`, `lam/interface/world_model.py`
- Browser automation: `lam/interface/browser_worker.py`, `lam/adapters/playwright_adapter.py`
- Desktop automation: `lam/interface/desktop_sequence.py`, `lam/adapters/uia_adapter.py`
- Data/spreadsheet/report: `lam/payer_rag/*`, `lam/adapters/excel_adapter.py`
- RAG/vector: `lam/interface/local_vector_store.py`, `lam/payer_rag/rag.py`
- Logging/tracing: `lam/governance/audit_logger.py`, `lam/interface/operator_contract.py`
- Configuration/dependencies: `config/*`, `pyproject.toml`, `requirements.txt`

## Current Failure Mode

The codebase has a real planner/executor spine, but the behavior is still dominated by intent-specific branches inside `search_agent.py`. It is not yet a general capability platform because:

- task understanding is spread across heuristics rather than a shared contract engine
- capability selection is implicit in hardcoded flows
- critics exist, but only a narrow subset is reusable across domains
- stale-output invalidation is strong in payer review but not generalized
- memory exists for sessions and app knowledge, not as a reusable operator memory layer
- artifact metadata is not centralized
- data science, storytelling, presentation, and UI generation are not generalized capability modules

## Added Foundation

New package: `lam/operator_platform/`

- `task_contract_engine.py`
  - Responsibility: normalize user request into a reusable task contract
  - Input: instruction text, optional context
  - Output: `TaskContract`
  - Tests: `tests/unit/test_operator_platform.py::test_task_contract_extraction`

- `world_model.py`
  - Responsibility: reusable environment/world state object
  - Input: session snapshot, artifacts, summary, task contract
  - Output: `WorldModel`
  - Integration: attached under `result.world_model.capability_context`

- `capability_registry.py`
  - Responsibility: register reusable capabilities with metadata
  - Input: capability specs
  - Output: registry lookup/list
  - Tests: `test_capability_registry_lookup_and_planner`

- `capability_planner.py`
  - Responsibility: compose capabilities into an execution graph from a task contract
  - Input: `TaskContract`
  - Output: `ExecutionGraph`
  - Integration: attached to every finalized result as `capability_execution_graph`

- `execution_graph.py`
  - Responsibility: represent reusable capability graph
  - Input: capabilities and dependencies
  - Output: serializable graph structure

- `tool_runtime.py`
  - Responsibility: describe high-level runtime tool families available
  - Input: runtime flags
  - Output: tool-family inventory

- `critics.py`
  - Responsibility: reusable source/data/story/ui/presentation/completion critics
  - Output schema: `passed`, `score`, `reason`, `required_fix`, `severity`
  - Integration: attached under `result.critics.platform`

- `memory_store.py`
  - Responsibility: durable operator memory for preferences, artifacts, stale/rejected outputs
  - Storage: SQLite
  - Integration: finalized runs now record created and rejected artifacts

- `artifact_factory.py`
  - Responsibility: centralized artifact manifest generation and validation
  - Output metadata: `task_id`, `created_at`, `geography`, `domain`, `source_data`, `generated_by_capabilities`, `validation_status`
  - Integration: finalized runs now get `artifact_manifest_json`

- `human_style_reporter.py`
  - Responsibility: concise human-facing run summary
  - Integration: attached as `result.human_report`

- `data_science.py`
  - Responsibility: profiling, missingness, descriptive stats, outliers, correlation, trend, cohort comparison, regression, chart recommendation/spec, insights
  - Tests: `test_data_science_functions`

- `data_storytelling.py`
  - Responsibility: executive-summary and action-oriented narrative package
  - Tests: `test_storytelling_presentation_and_ui_build`

- `presentation_build.py`
  - Responsibility: executive slide outline and speaker-note scaffold
  - Tests: `test_storytelling_presentation_and_ui_build`

- `ui_build.py`
  - Responsibility: reusable commercial UI spec scaffold
  - Tests: `test_storytelling_presentation_and_ui_build`

## Integration Strategy

The existing domain flows remain intact. The new platform layer is attached in `lam/interface/search_agent.py::_finalize_operator_result` so every run now gets:

- `task_contract`
- `capability_execution_graph`
- `artifact_manifest_json`
- platform critic results
- human-style report
- memory-store writes
- capability-oriented world-model context

This moves the repo toward reusable capability composition without breaking working flows.

## Runtime Upgrade

The next vertical slice is now implemented:

- `lam/operator_platform/runtime.py`
  - `ExecutionGraphRuntime` executes capability graphs in dependency order
  - emits runtime events
  - runs critics per node
  - performs bounded revision loops
  - writes artifact manifests

- `lam/operator_platform/executors.py`
  - executable capability layer for:
    - `deep_research`
    - `source_evaluation`
    - `file_inspection`
    - `data_cleaning`
    - `statistical_analysis`
    - `data_visualization`
    - `rag_build`
    - `rag_query`
    - `code_write`
    - `code_test`
    - `code_fix`
    - `data_storytelling`
    - `report_build`
    - `stakeholder_summary`
    - `presentation_build`
    - `spreadsheet_build`
    - `ui_build`
    - `artifact_export`
    - `approval_gate`

- `lam/operator_platform/ui_cards.py`
  - first-class UI payload builder for:
    - task contract card
    - artifact manifest card
    - critic results card
    - execution graph card
    - memory context card

- `lam/interface/search_agent.py`
  - now routes true UI-architecture prompts through the graph runtime
  - preserves existing job, competitor, shopping, payer, and code-workbench routes

- `lam/interface/web_ui.py`
  - renders platform cards in the main summary and canvas instead of leaving them hidden in raw JSON
