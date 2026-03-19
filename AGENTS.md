# AGENTS.md

## Purpose Source Of Truth

The source of truth for repository purpose is `docs/PROJECT_INTENT.md`.

If README language, sprint notes, issue text, or ad hoc operator framing conflict with that document, follow `docs/PROJECT_INTENT.md`.

ShadowScope exists to support FOIA-target generation from public records. Its technical pipeline and its investigative objective are both first-class and must be preserved together.

## Core Orientation

All work in this repository should support one or more of these outcomes:

- ingesting and preserving public procurement/spending records
- normalizing them into stable, queryable events and entities
- tagging them with deterministic ontology logic
- building explainable correlations and candidate joins
- generating reviewable, traceable leads for FOIA targeting

Do not optimize only for pipeline mechanics while losing the investigative purpose.
Do not optimize only for narrative framing while losing determinism, explainability, or source traceability.

## Investigator Guardrails

- Do not complete the narrative.
- Do not upgrade weak signals into strong claims.
- Do not collapse candidate relationships into confirmed identities.
- Do not present lead outputs as proof of hidden or classified activity.
- Do not imply that a contractor, facility, office, or program is part of a sensitive effort unless the public records themselves directly support that statement.
- Prefer under-claiming over over-claiming.
- Maintain operator trust through clear evidence, plain diagnostics, and explicit uncertainty.

## What ShadowScope Is

ShadowScope is a public-record investigative pipeline for:

- data ingestion
- normalization
- ontology tagging
- entity linking
- correlation building
- candidate join generation
- lead scoring, ranking, snapshotting, and export

It is also an investigator support system for:

- FOIA target list building
- identifying programs, contractors, facilities, and supporting entities worth review
- surfacing support-footprint signals that may justify follow-on collection
- packaging evidence so a reviewer can decide what to request next

## What ShadowScope Is Not

- not a classified-program proof engine
- not a speculative storytelling tool
- not a system for asserting hidden-program existence as fact
- not a place to turn candidate joins into merged identities without evidence
- not a place to trade explainability for cleverness

## Current Repo Priorities

Current operating priorities, reflected in `README.md`, `ROADMAP.md`, `docs/STATE.md`, and `docs/RUNBOOK.md`:

- SAM.gov is the active calibration and diagnostics focus.
- SAM smoke and validation workflows are the main operator trust surfaces.
- Deterministic, offline, fixture-based tests are preferred.
- Candidate joins must remain explainable and capped.
- USAspending is still important, but this sprint treats it as maintenance mode compared with SAM-focused hardening.
- Diagnostics, bundle quality, and operator trust matter as much as raw feature count.

## Repo Map

Primary code and docs are organized as follows:

- `backend/connectors/`: source ingestion and normalization adapters such as SAM.gov and USAspending
- `backend/db/`: SQLAlchemy models and migrations
- `backend/analysis/`: ontology parsing, tagging, and scoring logic
- `backend/correlate/`: deterministic correlation and candidate join logic
- `backend/services/`: workflow orchestration, exports, diagnostics, leads, reporting, and bundle logic
- `backend/api/`: FastAPI routes and API surfaces
- `shadowscope/`: CLI entrypoints
- `examples/`: ontology profiles, seed terms, and operator setup helpers
- `docs/`: runbooks, roadmap/state snapshots, audit notes, and operational guidance
- `tests/` and `backend/tests/`: offline regression coverage
- `tools/`: operator scripts and diagnostics helpers
- `ui/`: lightweight interface helpers

## Operating Principles For Changes

- Preserve deterministic behavior wherever possible.
- Keep outputs explainable.
- Preserve traceability from lead back to source record.
- Favor additive changes over schema-breaking or workflow-breaking changes.
- Keep existing operators unbroken unless a deliberate migration is part of the task.
- Do not fabricate fields or fill missing evidence with guessed values.
- If evidence is partial, label it as partial.
- If a relationship is candidate-grade, keep it candidate-grade.
- If a field is absent, return `null` rather than inventing a fallback story.
- Keep scoring changes explicit and bounded; do not redesign scoring casually.
- Treat diagnostics and review artifacts as product surfaces, not afterthoughts.

## Required Behavioral Constraints For Codex And Other Agents

- Read relevant docs and impacted code before editing.
- Keep both the technical pipeline function and the FOIA investigation objective in view.
- Do not rewrite the project into a generic procurement analytics tool that forgets FOIA targeting.
- Do not rewrite the project into a speculation engine that forgets source-grounded evidence.
- Maintain operator trust via clear diagnostics, explicit evidence, and conservative language.
- When in doubt, choose the interpretation that is more testable, more explainable, and less speculative.

## Source Interpretation Rules

- Public records are evidence.
- Correlations are investigative aids.
- Candidate joins are hypotheses for review.
- Leads are triage outputs.
- None of the above should be presented as final conclusions without direct supporting evidence.

## Change Rules

Before making changes:

- Read `README.md`, `ROADMAP.md`, `docs/STATE.md`, and `docs/RUNBOOK.md`.
- Inspect the code paths you are about to touch.
- Check whether the affected feature is part of the SAM trust/calibration surface.

When making changes:

- Keep naming and behavior backward compatible unless the task explicitly allows breaking change.
- Update tests with deterministic fixtures.
- Update docs when operator behavior or commands change.
- Preserve existing explainability fields and evidence trails.
- Keep bundle/report/export contracts stable unless the task explicitly asks to change them.
- Ensure outputs remain useful without raw DB inspection when working on lead/review surfaces.

When touching correlations or joins:

- Keep evidence visible.
- Keep scores explainable.
- Do not silently convert candidate joins into asserted entity merges.
- Preserve lane semantics and uncertainty labels.

When touching lead generation or exports:

- Keep source identifiers, agencies, vendors, dates, and rationale visible.
- Ensure a reviewer can understand why a lead exists.
- Preserve clear mapping back to source records.

When touching diagnostics:

- Prefer actionable hints over vague failure states.
- Explain what failed, why it matters, and what command to run next.
- Protect operator trust by being explicit about degraded or partial outcomes.

## Validation Steps

Use the smallest deterministic validation set that proves the change.

Common checks:

- `ss doctor status`
- `ss doctor status --source "SAM.gov" --days 30 --json`
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

Preferred offline test commands for current repo priorities:

- `.\.venv\Scripts\pytest.exe -q tests/test_workflow_wrapper.py tests/test_doctor_status_source_hints.py`
- `.\.venv\Scripts\pytest.exe -q tests/test_example_ontologies.py tests/test_workflow_cli_flags.py tests/test_samgov_ontology_tuning.py tests/test_leads_foia_matrix.py backend/tests/test_tagger.py`

If you change a narrower surface, run the targeted tests for that surface in addition to or instead of the broader set.

## Documentation Rules

If you change project purpose, operator framing, or investigation language:

- update `docs/PROJECT_INTENT.md` first
- make downstream docs align to it
- do not let sprint notes drift into speculative framing

If you change commands, workflows, bundle contracts, or diagnostics:

- update the relevant runbook or README section
- keep examples accurate
- prefer exact commands over vague prose

## Review Standard

A strong ShadowScope change does all of the following:

- improves public-record ingestion, normalization, ontology, correlation, lead generation, or review usability
- strengthens FOIA target generation value
- preserves determinism and explainability
- keeps uncertainty visible
- improves operator trust

A weak ShadowScope change does one or more of the following:

- adds speculative framing
- hides evidence behind opaque logic
- weakens reproducibility
- breaks diagnostics
- implies more certainty than the data supports

## Final Reminder

ShadowScope should help an investigator answer:

- what happened
- who is involved
- what public-record evidence connects these records
- why this lead might justify a FOIA request or follow-on collection step

It should not try to answer, without evidence:

- whether a hidden program definitely exists
- whether two entities are definitely the same
- whether a sensitive activity is confirmed fact

Build for disciplined investigation, not narrative completion.
