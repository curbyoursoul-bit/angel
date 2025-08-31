@echo off
REM ===== Dump all code files into one text =====
cd /d %~dp0

REM delete old dump if exists
if exist project_code_dump.txt del project_code_dump.txt

REM loop through all .py files and append with headers
for /r %%f in (*.py) do (
    echo ============================== >> project_code_dump.txt
    echo FILE: %%~nxf >> project_code_dump.txt
    echo PATH: %%f >> project_code_dump.txt
    echo ============================== >> project_code_dump.txt
    type "%%f" >> project_code_dump.txt
    echo. >> project_code_dump.txt
)

echo.
echo Full code dump saved to: %cd%\project_code_dump.txt
pause
