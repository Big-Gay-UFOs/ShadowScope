@echo off
setlocal
cd /d "%~dp0"
set DATABASE_URL=sqlite:///./dev.db
set UVICORN_LOG_LEVEL=debug
".venv311\Scripts\python.exe" -X dev -m uvicorn backend.app:app --host 127.0.0.1 --port 8000
echo(
echo Uvicorn exited with code %ERRORLEVEL%
pause
