@echo off
cd /d "%~dp0"

:MENU
cls
echo.
echo ================================================================
echo   MMEAN - Daily Session Pattern Classifier (day_pattern.py)
echo ================================================================
echo.
echo   Pattern Types:
echo     TREND_UP / TREND_DOWN  : Directional trend day
echo     REVERSAL_UP / DOWN     : Reversal day
echo     V_SHAPE                : Sharp drop then sharp recovery
echo     MORNING_SPIKE          : Early spike then range
echo     CLOSE_RUSH             : Late session direction surge
echo     RANGE_TIGHT / WIDE     : Range-bound day
echo     CHOPPY                 : No clear direction
echo.
echo ----------------------------------------------------------------
echo  [1] Auto-classify missing dates  (skip already classified)
echo  [2] Rebuild all                  (re-classify all dates)
echo  [3] Classify specific date
echo  [4] Show all stored patterns
echo  [5] Show pattern for specific date
echo  [0] Exit
echo.
set /p CHOICE="Select: "

if "%CHOICE%"=="1" goto AUTO
if "%CHOICE%"=="2" goto REBUILD
if "%CHOICE%"=="3" goto BY_DATE
if "%CHOICE%"=="4" goto SHOW_ALL
if "%CHOICE%"=="5" goto SHOW_DATE
if "%CHOICE%"=="0" goto END

echo Invalid choice.
timeout /t 1 >nul
goto MENU

:AUTO
echo.
echo [1] Auto-classifying missing dates...
echo.
python day_pattern.py
goto DONE

:REBUILD
echo.
echo  WARNING: This will delete all stored patterns and re-classify from scratch.
set /p CONFIRM="Type Y to continue: "
if /i not "%CONFIRM%"=="Y" goto MENU
echo.
python day_pattern.py --rebuild
goto DONE

:BY_DATE
echo.
set /p DATE="Enter date (e.g. 2026-03-20): "
if "%DATE%"=="" goto MENU
echo.
python day_pattern.py --date %DATE%
goto DONE

:SHOW_ALL
echo.
echo [4] Showing all stored patterns...
echo.
python day_pattern.py --show
goto DONE

:SHOW_DATE
echo.
set /p DATE="Enter date (e.g. 2026-03-20): "
if "%DATE%"=="" goto MENU
echo.
python day_pattern.py --show --date %DATE%
goto DONE

:DONE
echo.
if errorlevel 1 (
    echo [ERROR] Execution failed. Exit code: %errorlevel%
) else (
    echo [DONE]
)
echo.
set /p AGAIN="Return to menu? (y/n): "
if /i "%AGAIN%"=="y" goto MENU

:END
exit /b
