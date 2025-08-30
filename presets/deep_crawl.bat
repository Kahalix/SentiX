@echo off
REM Usage: presets\deep_crawl.bat <keyword> <since> <until> [collection] [max_tweets]
set KEYWORD=%1
set SINCE=%2
set UNTIL=%3
if "%KEYWORD%"=="" goto :usage
if "%SINCE%"=="" goto :usage
if "%UNTIL%"=="" goto :usage
set COLLECTION=%4
if "%COLLECTION%"=="" set COLLECTION=%KEYWORD%
set MAXT=%5
if "%MAXT%"=="" set MAXT=5000

python main.py --preset deep_crawl --keyword "%KEYWORD%" --since "%SINCE%" --until "%UNTIL%" --collection "%COLLECTION%" --max-tweets %MAXT%
exit /b 0

:usage
echo Usage: presets\deep_crawl.bat ^<keyword^> ^<since^> ^<until^> [collection] [max_tweets]
exit /b 1
