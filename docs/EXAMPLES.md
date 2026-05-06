# Examples Library

This page is organized by real product use, not internal modules.

## Operator Examples

### Desktop automation

```text
open notepad app then type "OpenLAMb is live" then press enter
```

### Browser research + artifacts

```text
Research the market for AI desktop agents and build an executive briefing with recommendations.
```

### Code workbench

```text
Create a new VS Code workspace for this task, write analysis code, add smoke tests, and leave me a runnable scaffold.
```

## Teach Mode Examples

### Internal workflow capture

```text
Teach OpenLAMb how I update weekly pipeline status in Salesforce.
```

### Repeated desktop process

```text
Teach this expense categorization workflow and save it as a reusable automation.
```

## Topic Mastery Examples

### Analytics skill

```text
Learn how to build a Power BI KPI dashboard from this YouTube tutorial and create a reusable skill.
```

### Grant-writing skill

```text
Learn how to create a grant proposal budget narrative. Watch this video, find related tutorials, and build me a reusable playbook.
```

### UI implementation skill

```text
Learn how to build a React chat/canvas UI for OpenLAMb. Use videos and supporting sources, then create an implementation skill.
```

## Professional Work Product Examples

### Career package

```text
Find 10 VP / Head of Data / Chief Data & AI / Analytics executive roles that fit me and produce the top-3 application package.
```

### Executive brief

```text
Research the market for AI desktop agents and build an executive briefing with recommendations.
```

### Data story

```text
Analyze this dataset, find the story, build charts, and create an executive summary.
```

## CLI Examples

### Start UI in the background

```powershell
python -m lam.main ui --background
```

### Learn a topic and inspect the skill

```powershell
python -m lam.main topic-learn --instruction "Learn how to build a Power BI KPI dashboard" --seed-url "https://youtube.com/example"
python -m lam.main skill-list --output json
python -m lam.main skill-show --skill-id skill_power_bi_kpi_dashboard --output json
```

### Preview safe practice

```powershell
python -m lam.main skill-practice-preview --skill-id skill_power_bi_kpi_dashboard --output json
```
