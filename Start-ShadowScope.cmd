@echo off
setlocal
cd /d "%~dp0"

REM Default DB (override by setting DATABASE_URL before running)
if not defined DATABASE_URL set "DATABASE_URL=sqlite:///./dev.db"

REM Logging level (debug/info/warning/error)
if not defined UVICORN_LOG_LEVEL set "UVICORN_LOG_LEVEL=info"

REM Pick venv folder
set "VENV_DIR=.venv"
if exist ".venv313\Scripts\python.exe" set "VENV_DIR=.venv313"

set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "SS_EXE=%VENV_DIR%\Scripts\ss.exe"

if not exist "%VENV_PY%" (
  echo ERROR: Cannot find %VENV_PY%
  echo Run setup/bootstrap once to create the venv.
  pause
  exit /b 1
)

echo Initializing DB...
"%SS_EXE%" db init

echo Seeding entities (safe to re-run)...
"%VENV_PY%" seed_entities.py

echo Opening docs...
start "" "http://127.0.0.1:8000/docs"

echo Starting API server...
"%VENV_PY%" -X dev -m uvicorn backend.app:app --host 127.0.0.1 --port 8000 --log-level %UVICORN_LOG_LEVEL%

echo(
echo Uvicorn exited with code %ERRORLEVEL%
pause
