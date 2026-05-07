@echo off
rem Windows 用起動スクリプト。ダブルクリックで起動できます。
cd /d "%~dp0"

where python >nul 2>nul
if %errorlevel% neq 0 (
  echo Python が見つかりません。https://www.python.org/ からインストールしてください。
  pause
  exit /b 1
)

python main.py
if %errorlevel% neq 0 pause
