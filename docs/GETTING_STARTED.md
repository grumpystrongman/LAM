# Getting Started

## What You Need

Minimum:
- Windows
- Python 3.11+
- local browser access for web automation

Recommended optional packages:

```powershell
pip install pywinauto pynput pyautogui opencv-python pillow pytesseract
```

## Install

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

## Launch the UI

Standard:

```powershell
python -m lam.main ui
```

Detached background launch:

```powershell
python -m lam.main ui --background --port 8814
```

Open the local URL printed by the CLI, usually:

```text
http://127.0.0.1:8795
```

## Your First 10 Minutes

1. Click `Accept Control`
2. Run a safe task:

```text
open notepad app then type "OpenLAMb is live" then press enter
```

3. Open Canvas and inspect the result
4. Try a research prompt:

```text
Research the market for AI desktop agents and build an executive briefing with recommendations.
```

5. Open `Skill Studio` and inspect learned-topic outputs if they already exist

## First Useful Prompts

### Desktop automation

```text
open excel app then type "weekly review ready" then press enter
```

### Research and artifact generation

```text
Find 10 healthcare analytics and AI leadership roles and build a ranked tracker with fit notes.
```

### Topic Mastery

```text
Learn how to build a Power BI KPI dashboard from this YouTube tutorial and create a reusable skill.
```

### Deep workbench

```text
Create a new VS Code workspace for this task, write analysis code, add smoke tests, and leave me a runnable scaffold.
```

## Where To Go Next

- [UI Guide](UI_GUIDE.md)
- [CLI Guide](CLI_GUIDE.md)
- [Teach Mode Guide](TEACH_MODE_GUIDE.md)
- [Topic Mastery Guide](TOPIC_MASTERY_GUIDE.md)
