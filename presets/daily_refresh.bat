@echo off
REM Usage: presets\daily_refresh.bat <keyword> [collection]
set KEYWORD=%1
if "%KEYWORD%"=="" (
  echo Usage: presets\daily_refresh.bat ^<keyword^> [collection]
  exit /b 1
)
set COLLECTION=%2
if "%COLLECTION%"=="" set COLLECTION=%KEYWORD%

python main.py --preset daily_refresh --keyword "%KEYWORD%" --collection "%COLLECTION%"
