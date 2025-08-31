@echo off
setlocal
REM Always run from this script’s folder
cd /d "%~dp0"

REM Ensure logs folder exists
if not exist "logs" mkdir "logs"

set "LOG=logs\exit_task.log"
echo [START %date% %time%] args=%* >> "%LOG%"

REM Call your venv Python; append ALL output to the log
"%CD%\.venv\Scripts\python.exe" -m scripts.exit_at_time %* >> "%LOG%" 2>&1

echo [END   %date% %time%] exitcode=%errorlevel% >> "%LOG%"
endlocal
