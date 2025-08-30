@echo off
REM Usage: presets\db_only_analysis.bat <keyword> [collection] [since] [until] [max_tweets]
set KEYWORD=%1
if "%KEYWORD%"=="" (
  echo Usage: presets\db_only_analysis.bat ^<keyword^> [collection] [since] [until] [max_tweets]
  exit /b 1
)
set COLLECTION=%2
if "%COLLECTION%"=="" set COLLECTION=%KEYWORD%
set SINCE=%3
set UNTIL=%4
set MAXT=%5
if "%MAXT%"=="" set MAXT=1000

set EXTRA=
if not "%SINCE%"=="" set EXTRA=%EXTRA% --since "%SINCE%"
if not "%UNTIL%"=="" set EXTRA=%EXTRA% --until "%UNTIL%"

python main.py --preset db_only --keyword "%KEYWORD%" --collection "%COLLECTION%" --max-tweets %MAXT% %EXTRA%
