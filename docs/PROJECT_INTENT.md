# Project Intent

ShadowScope is an investigative analysis system built on public U.S. government procurement and spending records. Its purpose is to turn large volumes of public contracting data into traceable, reviewable leads that help a human operator decide where to aim follow-on research and FOIA requests.

## System Function

Technically, ShadowScope is a deterministic pipeline that:

- ingests public source data such as SAM.gov opportunities and USAspending award records
- preserves raw source snapshots for auditability and debugging
- normalizes source records into queryable `Event` and `Entity` records
- applies ontology-driven tagging to surface relevant themes, terms, and rule matches
- builds explainable correlation lanes and candidate joins across records
- scores and ranks leads without hiding the evidence trail
- snapshots and exports review artifacts for analyst use

The system is therefore both a data pipeline and an investigator support surface. Its technical function exists to support its investigative purpose.

## Mission

ShadowScope exists to analyze public U.S. government procurement and spending data to identify patterns, entities, and relationships that can inform FOIA requests.

## Investigative Objective

- The system is used to build FOIA target lists.
- Targets include programs, contractors, facilities, and supporting entities.
- The focus includes identifying signals that may suggest the existence of hidden or sensitive programs.
- Those signals may appear in auxiliary, support, logistics, facilities, materials, engineering, security, testing, or sustainment contracts rather than in direct program disclosure.
- Example domains of interest may include exotic materials, advanced aerospace, UAP-related research, crash retrieval, reverse engineering contexts, specialized test infrastructure, unusual sustainment footprints, and related support ecosystems.
- The goal is not to prove any hidden program exists. The goal is to identify public-record leads worth structured follow-up.

## Method Constraints

- All outputs must be grounded in public records.
- The system produces leads, not conclusions.
- Correlations must remain explainable and evidence-based.
- Candidate joins must not be treated as confirmed identity matches.
- Weak signals must remain weak signals.
- Missing data must stay missing; the system should not fabricate context.
- Narrative construction must not outrun the underlying evidence.
- Operators must be able to trace a lead back to source records, identifiers, timestamps, and rule/correlation evidence.

## Output Philosophy

- Prioritize FOIA utility over speculation.
- Outputs should help a human investigator decide what to request next.
- Every lead should preserve source traceability.
- Exports should make the reason a lead exists visible without requiring raw database inspection.
- Explainability is a feature, not a nice-to-have.
- Under-claiming is preferred to over-claiming when evidence is incomplete or ambiguous.

## Non-Goals

- ShadowScope is not a system for proving hidden programs exist.
- It is not a speculative inference engine.
- It is not a tool for asserting classified activities as fact.
- It is not a replacement for human investigative judgment.
- It is not a license to collapse candidate relationships into confirmed identities.
- It is not a narrative generator for sensational claims unsupported by source evidence.

## Practical Interpretation

A good ShadowScope output does the following:

- identifies a potentially useful procurement or spending lead
- shows the source records and identifiers behind that lead
- explains the ontology hits, correlations, or candidate joins that made it noteworthy
- gives an investigator enough grounded context to decide whether a FOIA request, deeper review, or collection task is warranted

A bad ShadowScope output does the following:

- treats correlations as proof
- upgrades candidate joins into identity assertions
- implies hidden-program conclusions without source support
- optimizes for intrigue instead of traceability and investigator utility
