@echo off
cd /d "%~dp0"

color 0b

set PROJ_DIR=%~dp0
set CONDA_PATH=%USERPROFILE%\miniconda3\Scripts\activate.bat

echo [1/2] Activating Conda Environment: mmean...
call %CONDA_PATH% mmean

echo [2/2] Moving to Project Directory...
cd /d "%PROJ_DIR%"

if "%~1"=="" goto AUTO

set DATES=

:PARSE
if "%~1"=="" goto RUNLOOP
set ARG=%~1
set YEAR=20%ARG:~0,2%
set MON=%ARG:~2,2%
set DAY=%ARG:~4,2%
if "%DATES%"=="" (
    set DATES=%YEAR%-%MON%-%DAY%
) else (
    set DATES=%DATES%,%YEAR%-%MON%-%DAY%
)
shift
goto PARSE

:RUNLOOP
echo.
echo [PULSE SIM] dates=%DATES%
echo.
python sim_opt/pulse_sim.py --dates %DATES% --random 300 --bayes 150
python sim_opt/pulse_sim.py --top 10
goto RUNLOOP

:AUTO
echo.
echo [PULSE SIM] auto loop - Ctrl+C to stop
echo.
python sim_opt/pulse_sim.py --auto --random 300 --bayes 150 --sleep 300
