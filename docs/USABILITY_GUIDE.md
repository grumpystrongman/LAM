# OpenLAMb Usability Guide

## What OpenLAMb Is Best At

- Repetitive desktop workflows across browser and installed apps
- Fast setup with natural language and teach recording
- Human-in-the-loop execution where confidence and control matter
- Local-first operation with practical privacy defaults

## First 10 Minutes

1. Start UI: `python -m lam.main serve-ui --host 127.0.0.1 --port 8795`
2. Open `http://127.0.0.1:8795`
3. Click `Accept Control`
4. Run a simple prompt:
   - `open notepad app then type "OpenLAMb is live" then press enter`
5. Try `Preview` on a longer prompt before `Run`
6. Save it as an automation
7. Create an interval schedule and watch it run

## Main UI Areas

- Sidebar (collapsible): new task, task history, quick status
- Chat Workspace (default focus):
  - task instruction + run controls
  - assistant feed with concise progress and completion updates
  - feedback chips (`thumbs up/down`, `too slow`, `wrong path`, `not human-like`, `great result`)
- Canvas / Workbench (opens on demand):
  - browser frame, terminal-like progress, artifacts/outputs
  - developer details section for world model, run summary, and raw JSON
- Auth Recovery Wizard:
  - appears only when auth loop or credential pause is detected
  - recommends `local` vs `docker` worker mode based on failure code

## Best Prompt Patterns

- Deterministic macro style:
  - `open excel app then focus claims workbook then type "ready" then press enter`
- Research style:
  - `Research X across Y sources and create spreadsheet report and dashboard`
- Credential-assisted:
  - `open chatgpt app then login with chatgpt`

## Teach Mode Workflow

1. Click `Start Teach`
2. Perform actions manually or add steps with teach controls
3. Click `Stop + Generate`
4. Review generated instruction
5. Save as named automation
6. Add schedule if needed

Compression modes:

- `aggressive`: shortest generated flows
- `normal`: balanced default
- `strict`: preserve more explicit user actions

## Password Vault Workflow

1. Open vault panel
2. Save entry with `service + username + password`
3. Use `Generate Strong Password` for new accounts
4. Use `Autofill Active Window` to fill current login form
5. Export encrypted backup periodically

Notes:

- Vault is local-only and encrypted
- `data/` is excluded from git by default
- Autofill requires `Accept Control`

## Example Use Cases

### 1) Daily Job Market Sweep

Prompt:

`Search LinkedIn and other job sites for Data and AI roles in US and Ireland and build spreadsheet, report, dashboard with links`

Output:

- `data/reports/job_search/<timestamp>/jobs.csv`
- `data/reports/job_search/<timestamp>/report.md`
- `data/reports/job_search/<timestamp>/dashboard.html`

### 2) Inbox and Ticket Triage

Teach once, then schedule:

- Open mailbox
- Filter unread
- Copy ticket IDs
- Open ticketing app
- Update status fields

### 3) Credentialed App Login + Follow-up Steps

Prompt:

`open salesforce app then login with salesforce then click Opportunities then type Q2 pipeline summary`

## Troubleshooting

- If nothing runs:
  - confirm `Accept Control` is on
- If execution pauses:
  - complete login/MFA and click `Resume`
- If auth loops between sessions:
  - use `Auth Recovery Wizard` and apply the recommended mode (`local` or `docker`)
- If selector fails:
  - capture selector again with picker or use teach mode
- If OCR-based find fails:
  - verify `tesseract.exe` is on PATH and restart app session

## Operator Tips

- Use `Preview` for any high-impact request
- Keep automations named with clear intent and scope
- Use schedule events for safe manual triggering in production
- Export history for review and incident analysis
