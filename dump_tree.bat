@echo off
REM ===== Dump Angel Auto Trader project tree =====

cd /d C:\dev\angel_auto_trader

REM generate tree with files, ASCII chars, save to file
tree /f /a > project_tree_dump.txt

echo Project tree saved to: %cd%\project_tree_dump.txt
pause
