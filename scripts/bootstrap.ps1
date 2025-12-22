<#!
    ShadowScope Windows bootstrap script.
!>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Allow script execution for this session
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force | Out-Null

$RepoRoot = (Resolve-Path "$PSScriptRoot\..")
Set-Location $RepoRoot

Write-Host "ShadowScope bootstrap starting..." -ForegroundColor Cyan

$venvPath = Join-Path $RepoRoot ".venv"

function Resolve-Python313 {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return "py -3.13"
    }
    if (Get-Command python3.13 -ErrorAction SilentlyContinue) {
        return "python3.13"
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return "python"
    }
    throw "Python 3.13 executable not found. Install Python 3.13 from https://www.python.org/downloads/"
}

$pythonCmd = Resolve-Python313

if (-not (Test-Path $venvPath)) {
    Write-Host "Creating virtual environment at $venvPath" -ForegroundColor Yellow
    & $pythonCmd -m venv $venvPath
}

$activate = Join-Path $venvPath "Scripts/Activate.ps1"
. $activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .

$envPath = Join-Path $RepoRoot ".env"
if (-not (Test-Path $envPath)) {
    Copy-Item (Join-Path $RepoRoot ".env.example") $envPath
    Write-Host "Created .env from template" -ForegroundColor Green
}

Write-Host "Initializing database..." -ForegroundColor Cyan
ss db init

Write-Host "Running tests..." -ForegroundColor Cyan
ss test

Write-Host "Ingesting USAspending sample..." -ForegroundColor Cyan
ss ingest usaspending --days 3 --limit 25 --pages 1

Write-Host "Exporting events..." -ForegroundColor Cyan
$exportOutput = ss export events
$exportOutput | ForEach-Object { Write-Host $_ }

$logs = @(
    (Join-Path $RepoRoot "logs/app.log"),
    (Join-Path $RepoRoot "logs/ingest.log")
)
Write-Host "Log files:" -ForegroundColor Cyan
foreach ($log in $logs) {
    Write-Host " - $log"
}

Write-Host "Starting API server on http://127.0.0.1:8000" -ForegroundColor Cyan
ss serve --host 127.0.0.1 --port 8000
