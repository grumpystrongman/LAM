# Topic Mastery Guide

Topic Mastery Learn Mode is for cases where OpenLAMb needs to become well-versed before acting.

It supports two entry modes:
- topic-only
- seed video or source URL

## What It Produces

A normal Topic Mastery run can produce:
- `source_manifest.csv`
- `video_analysis_notes.md`
- `topic_model.md`
- `consensus_workflow.md`
- `learned_skill.json`
- `mastery_guide.md`
- `practice_plan.md`
- `critic_results.md`

## What It Actually Does

The pipeline is:

`seed/topic -> source discovery -> transcript/audio/visual analysis -> synthesis -> consensus workflow -> learned skill -> mastery guide -> validation -> memory`

## When To Use It

Use Topic Mastery when the task requires:
- procedural learning from multiple sources
- synthesis, not just extraction
- reusable skill creation
- topic refresh over time

Examples:
- Power BI KPI dashboards
- grant budget narratives
- React chat/canvas UI design
- Excel modeling workflows
- product-specific admin procedures

## CLI Example

```powershell
python -m lam.main topic-learn --instruction "Learn how to build a Power BI KPI dashboard" --seed-url "https://youtube.com/example" --output json
```

## Output Quality Model

Topic Mastery uses:
- source ranking
- transcript coverage checks
- synthesis and contradiction detection
- skill validation
- safe-practice constraints

The product should not claim fully executable mastery when the evidence is weak.

## Learned Skill Lifecycle

After a run, the skill can be:
- loaded in Skill Studio
- diffed across versions
- previewed in safe practice mode
- practiced through checkpointed runtime
- refreshed when the topic changes

## Topic Refresh

Use refresh when the topic is version-sensitive.

Example:

```powershell
python -m lam.main skill-refresh --skill-id skill_power_bi_kpi_dashboard --version 1.0 --reason "new_power_bi_release"
```

## Practical Pattern

1. Learn the topic
2. inspect the mastery guide
3. review the learned skill
4. preview safe practice
5. run safe practice if the skill is grounded enough
6. refresh the skill when tools or versions change

## Limits

Topic Mastery is not a video downloader or content copier.
It stores:
- attribution
- summaries
- procedural steps
- comparisons
- best practices
- learned skills

It should not store or redistribute full copyrighted video content as a final output.
