# Teach Mode Guide

Teach Mode turns repeated workflows into reusable operator behavior.

It is not just a macro recorder.
OpenLAMb now supports:
- learned recipe families
- alternate workflow variants
- branch health and decay
- checkpoint-aware replay
- re-teach guidance when a workflow degrades

## When To Use Teach Mode

Use Teach Mode when:
- you already know the task well
- the workflow repeats often
- the target app is stable enough for replay
- the task benefits from UI grounding more than broad research

Examples:
- CRM updates
- spreadsheet cleanup
- internal tool navigation
- support-ticket workflows
- credentialed browser tasks with human-auth checkpoints

## Basic Flow

1. Open Teach Mode
2. Start recording
3. perform the workflow
4. stop and generate
5. review the learned recipe
6. preview replay
7. save and reuse

## What OpenLAMb Learns

The teach stack stores more than raw clicks:
- semantic segments
- expected states
- fallback selectors
- checkpoint names
- branch variants
- replay outcomes over time

## Replay Safety

Replay uses:
- state checks
- selector fallback
- branch ranking from prior success/failure history
- reassign-to-another-branch when the current variant fails post-state validation

## Recipe Families

A single task can have multiple successful demonstrations.
OpenLAMb groups them into one family and tracks:
- which branches succeed most often
- which branches are stale
- which checkpoints are failing repeatedly
- when re-teach is recommended

## Re-Teach Guidance

When multiple surviving branches fail at the same checkpoint, OpenLAMb now surfaces:
- the failing checkpoint
- a suggested base variant
- branch timeline and checkpoint heat
- one-click targeted re-teach entry points in the UI

## Good Teach Mode Targets

Good:
- structured internal tools
- repeated browser navigation
- stable desktop forms
- high-frequency operator tasks

Bad:
- chaotic exploratory work
- tasks requiring open-ended research first
- destructive steps without clear review gates
- one-off tasks with no reuse value

## Relationship to Topic Mastery

Use Teach Mode when the operator should learn by demonstration.
Use Topic Mastery when the operator should learn by studying sources first.

The best systems use both:
- Topic Mastery for conceptual/procedural understanding
- Teach Mode for app-specific execution grounding
