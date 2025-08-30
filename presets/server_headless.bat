@echo off
REM Usage: presets\server_headless.bat <keyword> [collection] [user_data_dir]
set KEYWORD=%1
if "%KEYWORD%"=="" (
  echo Usage: presets\server_headless.bat ^<keyword^> [collection] [user_data_dir]
  exit /b 1
)
set COLLECTION=%2
if "%COLLECTION%"=="" set COLLECTION=%KEYWORD%
set USERDIR=%3

if "%USERDIR%"=="" (
  python main.py --preset server_headless --keyword "%KEYWORD%" --collection "%COLLECTION%"
) else (
  python main.py --preset server_headless --keyword "%KEYWORD%" --collection "%COLLECTION%" --user-data-dir "%USERDIR%"
)
