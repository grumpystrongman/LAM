# CLI Guide

OpenLAMb exposes the same product surface through the CLI.
Use the CLI when you want scriptability, local JSON output, or reproducible automation from a terminal.

## Top-Level Help

```powershell
python -m lam.main --help
```

## Start the UI

Foreground:

```powershell
python -m lam.main ui
```

Detached background launch:

```powershell
python -m lam.main ui --background --port 8814
```

Disable auto-open browser:

```powershell
python -m lam.main ui --no-open-browser
```

## Topic Mastery

Help:

```powershell
python -m lam.main topic-learn --help
```

Seed-video example:

```powershell
python -m lam.main topic-learn ^
  --instruction "Learn how to build a Power BI KPI dashboard" ^
  --seed-url "https://youtube.com/example" ^
  --output json
```

Topic-only example:

```powershell
python -m lam.main topic-learn ^
  --instruction "Learn how to create a grant proposal budget narrative and build a reusable playbook" ^
  --topic "grant proposal budget narrative"
```

## Learned Skills

List skills:

```powershell
python -m lam.main skill-list --output json
```

Show one skill:

```powershell
python -m lam.main skill-show --skill-id skill_power_bi_kpi_dashboard --output json
```

Diff two versions:

```powershell
python -m lam.main skill-diff --skill-id skill_power_bi_kpi_dashboard --right-version 1.1 --left-version 1.0
```

Preview practice:

```powershell
python -m lam.main skill-practice-preview --skill-id skill_power_bi_kpi_dashboard --output json
```

Run safe practice:

```powershell
python -m lam.main skill-practice-run --skill-id skill_power_bi_kpi_dashboard --output json
```

Refresh a skill:

```powershell
python -m lam.main skill-refresh --skill-id skill_power_bi_kpi_dashboard --version 1.0 --output json
```

## Code Workbench

```powershell
python -m lam.main workbench-create --instruction "Create a new VS Code workspace, write analysis code, and leave me a runnable scaffold."
```

## Governance and Platform Commands

Control plane API:

```powershell
python -m lam.main serve-control-plane --help
```

Audit validation:

```powershell
python -m lam.main validate-audit --help
```

## Output Modes

Many commands support:
- `--output text`
- `--output json`

Use `json` when integrating with scripts or other tools.
Use `text` when working interactively.

## Practical CLI Patterns

### Launch the product locally and keep it running

```powershell
python -m lam.main ui --background
```

### Learn a topic, inspect the learned skill, then preview practice

```powershell
python -m lam.main topic-learn --instruction "Learn how to build a Power BI KPI dashboard" --seed-url "https://youtube.com/example"
python -m lam.main skill-list --output json
python -m lam.main skill-practice-preview --skill-id skill_power_bi_kpi_dashboard --output json
```

### Refresh a version-sensitive skill after product changes

```powershell
python -m lam.main skill-refresh --skill-id skill_power_bi_kpi_dashboard --version 1.0 --reason "new_power_bi_release"
```
