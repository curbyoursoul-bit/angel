@echo off
setlocal ENABLEDELAYEDEXPANSION

REM ==== resolve repo root ====
set "ROOT=%~dp0"
pushd "%ROOT%"

echo [info] Repo: "%ROOT%"

REM ==== ensure virtualenv exists ====
if not exist "%ROOT%\.venv\Scripts\python.exe" (
  echo [.setup] Creating .venv ...
  py -3 -m venv "%ROOT%\.venv"
)

REM ==== activate ====
if not exist "%ROOT%\.venv\Scripts\activate.bat" (
  echo [ERROR] Expected "%ROOT%\.venv\Scripts\activate.bat" but it wasn't found.
  echo        Delete the ".venv" folder and re-run this script.
  exit /b 1
)

call "%ROOT%\.venv\Scripts\activate.bat"
if errorlevel 1 (
  echo [ERROR] Failed to activate virtualenv.
  exit /b 1
)

for /f "tokens=* usebackq" %%v in (`python -V`) do set "PYVER=%%v"
echo [.venv] %PYVER%

REM ==== install requirements (idempotent) ====
if exist "%ROOT%\requirements.txt" (
  echo [.setup] Installing requirements ...
  python -m pip install -r "%ROOT%\requirements.txt"
)

REM ==== pick entrypoint ====
set "ENTRY="
if exist "%ROOT%\core\engine.py" (
  set "ENTRY=python -m core.engine"
) else if exist "%ROOT%\main.py" (
  set "ENTRY=python main.py"
) else (
  echo [ERROR] Could not find core\engine.py or main.py
  exit /b 1
)

echo [.run] Starting trader → %ENTRY%
%ENTRY%

popd
endlocal
