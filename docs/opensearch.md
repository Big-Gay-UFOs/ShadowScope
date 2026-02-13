# OpenSearch in ShadowScope

## Purpose
OpenSearch is the search/index layer for investigative queries (fast text search, filters, and scoring).
Postgres remains the source of truth; OpenSearch is a derived index built from Postgres events.

## Current workflow
1. Ingest into Postgres using ss ingest ...
2. Build/refresh the OpenSearch index using 	ools/opensearch_reindex.py
   - Full rebuild: --recreate
   - Incremental: default (indexes only new events by id)

## Index name + versioning
The index name is configured via OPENSEARCH_INDEX / --index.

Versioning rule:
- When the mapping changes in a non-backwards-compatible way, bump the index name (e.g. shadowscope-events-v2)
  and run a full rebuild with --recreate.

## Mapping (v1)
The reindex tool creates the index with the following key fields:

- hash (keyword) — stable unique identifier (used as document _id)
- event_id (integer) — Postgres events.id (used for incremental indexing)
- category (keyword)
- source (keyword)
- doc_id (keyword)
- source_url (keyword)
- occurred_at (date)
- created_at (date)
- place_text (text + keyword subfield)
- snippet (text)
- keywords (keyword)

## Verification commands
Mapping:
  GET http://127.0.0.1:9200/<index>/_mapping?pretty

Count:
  GET http://127.0.0.1:9200/<index>/_count?pretty