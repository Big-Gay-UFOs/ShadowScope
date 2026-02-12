# Phase 0 — Architecture & Scope

## Clarified goal (batch investigative mode)

ShadowScope is operated as a **batch investigative analysis tool** (not a continuous alerting system). Each run ingests a defined time window of public procurement data, tags events using a configurable keyword ontology, scores events, and produces + persists **anomaly clusters** so future runs can attach new events and report deltas.

## System architecture (high-level)

```mermaid
graph TD
  subgraph Ingestion
    A[Batch Runner (manual / cron)]
    B[Connector Workers]
    C[Parser & Clause Miner]
  end
  subgraph DataPlatform
    D[(Postgres)]
    E[(OpenSearch)]
    F[Blob Storage (S3-compatible)]
    G[Entity Resolver]
  end
  subgraph Analytics
    H[Correlation Engine]
    I[Scoring & Anti-Confirmation]
  end
  subgraph Interfaces
    J[FastAPI Service]
    K[CLI]
  end

  A --> B --> C
  C --> D
  C --> E
  C --> F
  D --> G --> D
  D --> H
  E --> H
  H --> I --> J
  J --> K
