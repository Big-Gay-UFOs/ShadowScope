@echo off
setlocal
cd /d "%~dp0"

set "DATABASE_URL=%DATABASE_URL%"
if not defined DATABASE_URL set "DATABASE_URL=sqlite:///./dev.db"
set "UVICORN_LOG_LEVEL=%UVICORN_LOG_LEVEL%"
if not defined UVICORN_LOG_LEVEL set "UVICORN_LOG_LEVEL=debug"

set "VENV_PY=.venv\Scripts\python.exe"
if exist ".venv311\Scripts\python.exe" set "VENV_PY=.venv311\Scripts\python.exe"

if not exist "%VENV_PY%" (
    echo Unable to locate a virtual environment python interpreter.
    echo Run scripts\bootstrap.ps1 to create the .venv folder and try again.
    goto :exit
)

"%VENV_PY%" -X dev -m uvicorn backend.app:app --host 127.0.0.1 --port 8000

:exit
echo(
echo Uvicorn exited with code %ERRORLEVEL%
pause
