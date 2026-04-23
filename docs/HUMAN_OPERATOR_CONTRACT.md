# OpenLAMb Human-Operator Contract

OpenLAMb must behave as a human-quality computer operator across browser, desktop apps, terminal, files, and web apps.

## Core Rules
- Observe environment first.
- Reuse existing state (tabs, sessions, open files, partial outputs) before restarting.
- Pick tool family by task domain, not habit.
- Use concrete targets; never placeholders.
- Detect loops quickly and change strategy.
- Recover using grounded alternatives before declaring blockers.
- Provide evidence tied to real outputs.
- Pause for approval on irreversible/high-risk actions.

## Completion Standard
A task is complete only when requested real-world outputs exist and are inspectable.

## Rubric
Scoring categories and weighted model are implemented in:
- `lam/interface/human_operator_benchmark.py`

Scenario definitions are in:
- `config/human_operator_scenarios.json`

Use these assets to run release-to-release behavior regression checks.

