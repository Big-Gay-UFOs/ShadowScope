# ShadowScope State Snapshot (FOIA sprint)

## What exists now
- USAspending ingest with `--keyword` / `--recipient`
- FOIA ontology sandbox: `ontology.foia.json`
- Correlation lanes: same_entity, same_uei, same_keyword, kw_pair
- Lead snapshots default to v2 scoring (pair_strength + noise penalties)
- DOE weapons complex pivots: SRS, Y-12, Pantex, KCNSC, CNS, SRNS
- One-command runbook: `tools/runbook.ps1`

## How to run
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\runbook.ps1`

## Why it’s structured this way
- Prefer co-term evidence via `kw_pair` rather than overly broad single keywords
- Add explicit “noise” rules and penalize them in v2 scoring
- Keep FOIA ontology in `ontology.foia.json` to iterate safely

