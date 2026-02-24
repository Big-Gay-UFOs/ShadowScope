param(
  [int]$Days = 3650,
  [int]$MaxRecords = 200,
  [int]$MinScore = 18,
  [int]$Limit = 75,
  [int]$ScanLimit = 40000
)

Write-Host "ShadowScope runbook starting..." -ForegroundColor Cyan

# Seed pack (moderate volume; duplicates simply insert 0)
$seeds = @(
  'National Nuclear Security Administration',
  'Office of Secure Transportation',
  'Savannah River Site',
  'Y-12 National Security Complex',
  'Pantex Plant',
  'Kansas City National Security Campus',
  'Consolidated Nuclear Security',
  'Savannah River Nuclear Solutions',
  'Nevada National Security Site',
  'Nevada Test and Training Range'
)

foreach ($k in $seeds) {
  Write-Host "Ingest: $k"
  docker compose exec backend ss ingest usaspending --days $Days --pages 2 --page-size 100 --max-records $MaxRecords --keyword "$k"
}

Write-Host "Applying ontology..."
docker compose exec backend ss ontology apply --source USAspending --days $Days --path ontology.foia.json

Write-Host "Linking entities..."
docker compose exec backend ss entities link --source USAspending --days $Days

Write-Host "Rebuilding correlations..."
docker compose exec backend ss correlate rebuild --window-days $Days --source USAspending --min-events 2
docker compose exec backend ss correlate rebuild-uei --window-days $Days --source USAspending --min-events 2
docker compose exec backend ss correlate rebuild-keywords --window-days $Days --source USAspending --min-events 3 --max-events 200
docker compose exec backend ss correlate rebuild-keyword-pairs --window-days $Days --source USAspending --min-events 2 --max-events 200 --max-keywords-per-event 10

$aid = (docker compose exec -T db psql -U postgres -d shadowscope -t -A -c "select max(id) from analysis_runs where analysis_type='ontology_apply' and source='USAspending';").Trim()
Write-Host "analysis_run_id=$aid"

Write-Host "Creating lead snapshot (default scoring v2)..."
docker compose exec backend ss leads snapshot --analysis-run-id $aid --source USAspending --min-score $MinScore --limit $Limit --scan-limit $ScanLimit --notes "runbook snapshot"

Write-Host "Top 15 leads:"
docker compose exec -T db psql -U postgres -d shadowscope -P pager=off -c "
with latest as (select max(id) as sid from lead_snapshots)
select
  i.rank, i.score,
  coalesce(i.score_details->>'has_noise','') as has_noise,
  coalesce(i.score_details->>'noise_penalty','') as noise_penalty,
  e.doc_id,
  left(coalesce(e.snippet,''), 120) as snippet_120
from lead_snapshot_items i
join latest l on i.snapshot_id=l.sid
join events e on e.id=i.event_id
order by i.rank
limit 15;
"

Write-Host "Runbook complete." -ForegroundColor Green
