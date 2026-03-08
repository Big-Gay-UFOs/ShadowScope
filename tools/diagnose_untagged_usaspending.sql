-- Diagnose recent untagged USAspending events using JSON-safe keyword length checks.
--
-- Usage (PowerShell):
--   psql -U postgres -d shadowscope -v window_days=30 -v row_limit=50 -f .\tools\diagnose_untagged_usaspending.sql
--
-- Notes:
--   - events.keywords is JSON in this schema (not JSONB).
--   - This script uses json_typeof/json_array_length and avoids jsonb_* functions.

\if :{?window_days}
\else
\set window_days 30
\endif

\if :{?row_limit}
\else
\set row_limit 50
\endif

WITH recent AS (
  SELECT
    e.id,
    e.created_at,
    e.occurred_at,
    e.doc_id,
    e.source_url,
    e.snippet,
    e.raw_json,
    CASE
      WHEN e.keywords IS NULL THEN 0
      WHEN json_typeof(e.keywords) = 'array' THEN json_array_length(e.keywords)
      ELSE 0
    END AS keyword_count
  FROM events e
  WHERE e.source = 'USAspending'
    AND COALESCE(e.occurred_at, e.created_at) >= NOW() - (:window_days || ' days')::interval
)
SELECT
  COUNT(*) AS events_window,
  SUM(CASE WHEN keyword_count > 0 THEN 1 ELSE 0 END) AS events_with_keywords,
  SUM(CASE WHEN keyword_count = 0 THEN 1 ELSE 0 END) AS events_without_keywords,
  ROUND(
    100.0 * SUM(CASE WHEN keyword_count > 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0),
    1
  ) AS keyword_coverage_pct
FROM recent;

WITH recent AS (
  SELECT
    e.id,
    e.created_at,
    e.occurred_at,
    e.doc_id,
    e.source_url,
    e.snippet,
    e.raw_json,
    CASE
      WHEN e.keywords IS NULL THEN 0
      WHEN json_typeof(e.keywords) = 'array' THEN json_array_length(e.keywords)
      ELSE 0
    END AS keyword_count
  FROM events e
  WHERE e.source = 'USAspending'
    AND COALESCE(e.occurred_at, e.created_at) >= NOW() - (:window_days || ' days')::interval
)
SELECT
  id,
  created_at,
  occurred_at,
  COALESCE(doc_id, raw_json->>'Award ID', raw_json->>'generated_unique_award_id') AS award_id,
  raw_json->>'Recipient Name' AS recipient_name,
  keyword_count,
  LEFT(
    COALESCE(
      NULLIF(TRIM(snippet), ''),
      NULLIF(TRIM(raw_json->>'Description'), ''),
      NULLIF(TRIM(raw_json->>'description'), ''),
      '<no description>'
    ),
    240
  ) AS description_preview,
  source_url
FROM recent
WHERE keyword_count = 0
ORDER BY COALESCE(occurred_at, created_at) DESC, id DESC
LIMIT :row_limit;
