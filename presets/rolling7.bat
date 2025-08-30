@echo off
REM Usage: presets\rolling7.bat <keyword> [collection] [max_tweets]
set KEYWORD=%1
if "%KEYWORD%"=="" (
  echo Usage: presets\rolling7.bat ^<keyword^> [collection] [max_tweets]
  exit /b 1
)
set COLLECTION=%2
if "%COLLECTION%"=="" set COLLECTION=%KEYWORD%
set MAXT=%3
if "%MAXT%"=="" set MAXT=500

python main.py --preset rolling7 --keyword "%KEYWORD%" --collection "%COLLECTION%" --max-tweets %MAXT%
