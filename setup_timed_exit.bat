@echo off
setlocal

REM ===== configurable =====
set TASKNAME=TimedExit
REM default time (HH:MM, 24h local time). Override by passing as 1st arg.
set TIME=15:20
REM pass FORCE to append --force to the runner (optional 2nd arg)
set FORCEFLAG=
REM ========================

if not "%~1"=="" set TIME=%~1
if /I "%~2"=="FORCE" set FORCEFLAG= --force

REM Resolve repo dir (this script’s folder)
set "REPO=%~dp0"
REM Trim trailing backslash if present
if "%REPO:~-1%"=="\" set "REPO=%REPO:~0,-1%"

REM Runner path
set "RUNNER=%REPO%\exit_task_runner.bat"

REM Ensure runner exists
if not exist "%RUNNER%" (
  echo [ERROR] Not found: %RUNNER%
  exit /b 1
)

REM Delete any existing task silently
schtasks /Delete /TN "%TASKNAME%" /F >nul 2>nul

REM Create new task: run as SYSTEM, highest privileges, daily at TIME
schtasks /Create ^
  /TN "%TASKNAME%" ^
  /SC DAILY ^
  /ST %TIME% ^
  /RL HIGHEST ^
  /RU SYSTEM ^
  /TR "\"%RUNNER%\"%FORCEFLAG%"

set RC=%ERRORLEVEL%
if not %RC%==0 (
  echo [ERROR] Failed to create task (code %RC%). Try from an **elevated** Command Prompt.
  exit /b %RC%
)

echo [OK] Task "%TASKNAME%" scheduled daily at %TIME% as SYSTEM.
echo [INFO] To verify:
echo   schtasks /Query /TN "%TASKNAME%" /V /FO LIST
echo [INFO] To run now:
echo   schtasks /Run /TN "%TASKNAME%"

endlocal
@echo off
setlocal

REM ===== configurable =====
set TASKNAME=TimedExit
REM default time (HH:MM, 24h local time). Override by passing as 1st arg.
set TIME=15:20
REM pass FORCE to append --force to the runner (optional 2nd arg)
set FORCEFLAG=
REM ========================

if not "%~1"=="" set TIME=%~1
if /I "%~2"=="FORCE" set FORCEFLAG= --force

REM Resolve repo dir (this script’s folder)
set "REPO=%~dp0"
REM Trim trailing backslash if present
if "%REPO:~-1%"=="\" set "REPO=%REPO:~0,-1%"

REM Runner path
set "RUNNER=%REPO%\exit_task_runner.bat"

REM Ensure runner exists
if not exist "%RUNNER%" (
  echo [ERROR] Not found: %RUNNER%
  exit /b 1
)

REM Delete any existing task silently
schtasks /Delete /TN "%TASKNAME%" /F >nul 2>nul

REM Create new task: run as SYSTEM, highest privileges, daily at TIME
schtasks /Create ^
  /TN "%TASKNAME%" ^
  /SC DAILY ^
  /ST %TIME% ^
  /RL HIGHEST ^
  /RU SYSTEM ^
  /TR "\"%RUNNER%\"%FORCEFLAG%"

set RC=%ERRORLEVEL%
if not %RC%==0 (
  echo [ERROR] Failed to create task (code %RC%). Try from an **elevated** Command Prompt.
  exit /b %RC%
)

echo [OK] Task "%TASKNAME%" scheduled daily at %TIME% as SYSTEM.
echo [INFO] To verify:
echo   schtasks /Query /TN "%TASKNAME%" /V /FO LIST
echo [INFO] To run now:
echo   schtasks /Run /TN "%TASKNAME%"

endlocal
