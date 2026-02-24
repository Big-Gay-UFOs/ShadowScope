# ShadowScope

ShadowScope is a **batch investigative OSINT pipeline** for surfacing “support footprints” of sensitive programs inside **public procurement data** (starting with USAspending; SAM.gov planned).

It is designed for repeatable investigator runs:

1) ingest a time window (seeded searches)  
2) normalize + persist to Postgres (idempotent)  
3) tag with ontology signals (keywords + clause hits)  
4) score/rank leads (**v2** scoring by default)  
5) snapshot leads (repeatability + deltas)  
6) cluster related records (entity / UEI / keyword / keyword-pair)

## Runbook
One command:
- `tools/runbook.ps1`

Windows execution policy (one-time):
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\runbook.ps1`

## Key FOIA sprint additions
- Seeded ingest: `--keyword`, `--recipient`
- FOIA ontology: `ontology.foia.json` (apply via `--path`)
- Correlation lanes include `kw_pair` (co-term clustering)
- Default lead snapshots are **v2**
- Operational noise handling (HRP/DACTS, NASA sponsoring agreement)
- DOE/NNSA weapons complex pivots (SRS, Y-12, Pantex, KCNSC, CNS, SRNS)

## Resume points
- `docs/STATE.md` (current state snapshot)
- `ROADMAP.md` (milestones + next steps)
- `ontology.foia.json` (FOIA ontology)
- `tools/runbook.ps1` (repeatable run)

