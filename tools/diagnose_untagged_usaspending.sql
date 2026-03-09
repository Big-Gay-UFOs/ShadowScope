-- Diagnose recent untagged USAspending events using JSON-safe keyword length checks.
--
-- Usage (PowerShell):
--   psql -U postgres -d shadowscope -v window_days=30 -v row_limit=50 -f .\tools\diagnose_untagged_usaspending.sql
--
-- Notes:
--   - events.keywords is JSON in this schema (not JSONB).
--   - This script uses json_typeof/json_array_length and avoids jsonb_* functions.
--
-- Tuning loop:
--   1) Run workflow + doctor for USAspending
--   2) Run this diagnostic to inspect untagged prevalence and samples
--   3) Tune ontology rules for recurring, high-precision patterns
--   4) Re-run workflow/correlations and compare doctor metrics

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
    COALESCE(
      NULLIF(TRIM(e.snippet), ''),
      NULLIF(TRIM(e.raw_json->>'Description'), ''),
      NULLIF(TRIM(e.raw_json->>'description'), ''),
      ''
    ) AS description_text,
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
    LOWER(
      COALESCE(
        NULLIF(TRIM(e.snippet), ''),
        NULLIF(TRIM(e.raw_json->>'Description'), ''),
        NULLIF(TRIM(e.raw_json->>'description'), ''),
        ''
      )
    ) AS description_lc,
    CASE
      WHEN e.keywords IS NULL THEN 0
      WHEN json_typeof(e.keywords) = 'array' THEN json_array_length(e.keywords)
      ELSE 0
    END AS keyword_count
  FROM events e
  WHERE e.source = 'USAspending'
    AND COALESCE(e.occurred_at, e.created_at) >= NOW() - (:window_days || ' days')::interval
),
untagged AS (
  SELECT *
  FROM recent
  WHERE keyword_count = 0
)
SELECT
  COUNT(*) AS untagged_window,
  SUM(CASE WHEN description_lc ~ '\mservice(s)?\M' THEN 1 ELSE 0 END) AS has_service,
  SUM(CASE WHEN description_lc ~ '\msupport\M' THEN 1 ELSE 0 END) AS has_support,
  SUM(CASE WHEN description_lc ~ '\mmaintenance\M' THEN 1 ELSE 0 END) AS has_maintenance,
  SUM(CASE WHEN description_lc ~ '\mtraining\M' THEN 1 ELSE 0 END) AS has_training,
  SUM(CASE WHEN description_lc ~ '\msecurity\M' THEN 1 ELSE 0 END) AS has_security,
  SUM(CASE WHEN description_lc ~ '\mlicen[cs]e(s)?\M' THEN 1 ELSE 0 END) AS has_license,
  SUM(CASE WHEN description_lc ~ '\mcloud\M' THEN 1 ELSE 0 END) AS has_cloud,
  SUM(CASE WHEN description_lc ~ '\msoftware\M' THEN 1 ELSE 0 END) AS has_software,
  SUM(CASE WHEN description_lc ~ '\msoftware\M' AND description_lc ~ '\mmaintenance\M' THEN 1 ELSE 0 END) AS software_and_maintenance,
  SUM(CASE WHEN description_lc ~ '\msoftware\M' AND description_lc ~ '\mlicen[cs]e(s)?\M' THEN 1 ELSE 0 END) AS software_and_license,
  SUM(CASE WHEN description_lc ~ '\mcloud\M' AND description_lc ~ '\msupport\M' THEN 1 ELSE 0 END) AS cloud_and_support,
  SUM(CASE WHEN description_lc ~ '\msecurity\M' AND description_lc ~ '\mtraining\M' THEN 1 ELSE 0 END) AS security_and_training
FROM untagged;

WITH recent AS (
  SELECT
    e.id,
    e.created_at,
    e.occurred_at,
    e.doc_id,
    e.source_url,
    e.snippet,
    e.raw_json,
    COALESCE(
      NULLIF(TRIM(e.snippet), ''),
      NULLIF(TRIM(e.raw_json->>'Description'), ''),
      NULLIF(TRIM(e.raw_json->>'description'), ''),
      '<no description>'
    ) AS description_text,
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
  CASE WHEN LOWER(description_text) ~ '\mservice(s)?\M' THEN 1 ELSE 0 END AS has_service,
  CASE WHEN LOWER(description_text) ~ '\msupport\M' THEN 1 ELSE 0 END AS has_support,
  CASE WHEN LOWER(description_text) ~ '\mmaintenance\M' THEN 1 ELSE 0 END AS has_maintenance,
  CASE WHEN LOWER(description_text) ~ '\mtraining\M' THEN 1 ELSE 0 END AS has_training,
  CASE WHEN LOWER(description_text) ~ '\msecurity\M' THEN 1 ELSE 0 END AS has_security,
  CASE WHEN LOWER(description_text) ~ '\mlicen[cs]e(s)?\M' THEN 1 ELSE 0 END AS has_license,
  CASE WHEN LOWER(description_text) ~ '\mcloud\M' THEN 1 ELSE 0 END AS has_cloud,
  CASE WHEN LOWER(description_text) ~ '\msoftware\M' THEN 1 ELSE 0 END AS has_software,
  LEFT(description_text, 240) AS description_preview,
  source_url
FROM recent
WHERE keyword_count = 0
ORDER BY COALESCE(occurred_at, created_at) DESC, id DESC
LIMIT :row_limit;
