@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: PYTHON: 콘다 환경이 활성화되어 있으면 그냥 python, 아니면 전체 경로
::   conda activate mmean 후 실행하면 python 만으로 충분
set PYTHON=%USERPROFILE%\miniconda3\envs\mmean\python.exe
set SCRIPT=%~dp0db\db_archive.py
set KEEP_DAYS=7

:SET_DAYS
cls
echo.
echo ════════════════════════════════════════════════════
echo   DB Archive  C Drive -^> F Drive
echo ════════════════════════════════════════════════════
echo.
echo   C 드라이브에 유지할 최근 일수를 입력하세요.
echo   (이 일수보다 오래된 데이터를 F 드라이브로 이전합니다)
echo.
set /p INPUT_DAYS=  유지 일수 [현재: %KEEP_DAYS%일] (엔터=유지):
if not "!INPUT_DAYS!"=="" set KEEP_DAYS=!INPUT_DAYS!

:MENU
cls
echo.
echo ════════════════════════════════════════════════════
echo   DB Archive  C Drive -^> F Drive
echo   현재 설정: 최근 [%KEEP_DAYS%일] C드라이브 유지
echo ════════════════════════════════════════════════════
echo.
echo   [1] 용량 현황 확인
echo   [2] DRY-RUN   (변경 없이 이전 대상 건수만 확인)
echo   [3] 전체 실행 (sim.db + mmean.db)
echo   [4] sim.db    만 실행
echo   [5] mmean.db  만 실행
echo   [6] 유지 일수 변경  (현재: %KEEP_DAYS%일)
echo   [7] F드라이브 중복 데이터 정리 (dedup)
echo   [8] F드라이브 중복 확인만 (dedup dry-run)
echo   [0] 종료
echo.
echo ════════════════════════════════════════════════════
set /p CHOICE=  번호 선택:

if "%CHOICE%"=="0" goto END
if "%CHOICE%"=="1" goto STATUS
if "%CHOICE%"=="2" goto DRYRUN
if "%CHOICE%"=="3" goto FULL
if "%CHOICE%"=="4" goto SIM_ONLY
if "%CHOICE%"=="5" goto MMEAN_ONLY
if "%CHOICE%"=="6" goto SET_DAYS
if "%CHOICE%"=="7" goto DEDUP
if "%CHOICE%"=="8" goto DEDUP_DRY

echo   잘못된 번호입니다.
pause
goto MENU

:: ─────────────────────────────────────────
:STATUS
cls
echo.
echo   [용량 현황 확인]
echo.
%PYTHON% %SCRIPT% --status
echo.
pause
goto MENU

:: ─────────────────────────────────────────
:DRYRUN
cls
echo.
echo   [DRY-RUN]  유지 일수: %KEEP_DAYS%일  -- 실제 변경 없음
echo.
%PYTHON% %SCRIPT% --dry-run --keep-days %KEEP_DAYS%
echo.
pause
goto MENU

:: ─────────────────────────────────────────
:FULL
cls
echo.
echo   [전체 실행]  sim.db + mmean.db
echo   %KEEP_DAYS%일 이전 데이터를 F드라이브로 이전합니다.
echo.
set /p CONFIRM=  계속하시겠습니까? (y/n):
if /i not "%CONFIRM%"=="y" goto MENU
echo.
%PYTHON% %SCRIPT% --keep-days %KEEP_DAYS%
echo.
pause
goto MENU

:: ─────────────────────────────────────────
:SIM_ONLY
cls
echo.
echo   [sim.db 만 실행]
echo   %KEEP_DAYS%일 이전 데이터를 F드라이브로 이전합니다.
echo.
set /p CONFIRM=  계속하시겠습니까? (y/n):
if /i not "%CONFIRM%"=="y" goto MENU
echo.
%PYTHON% %SCRIPT% --sim-only --keep-days %KEEP_DAYS%
echo.
pause
goto MENU

:: ─────────────────────────────────────────
:MMEAN_ONLY
cls
echo.
echo   [mmean.db 만 실행]
echo   %KEEP_DAYS%일 이전 데이터를 F드라이브로 이전합니다.
echo.
set /p CONFIRM=  계속하시겠습니까? (y/n):
if /i not "%CONFIRM%"=="y" goto MENU
echo.
%PYTHON% %SCRIPT% --mmean-only --keep-days %KEEP_DAYS%
echo.
pause
goto MENU

:: ─────────────────────────────────────────
:DEDUP
cls
echo.
echo   [F드라이브 중복 데이터 정리]
echo   F:\db\sim.db, F:\db\mmean.db 중복 행을 삭제합니다.
echo.
set /p CONFIRM=  계속하시겠습니까? (y/n):
if /i not "%CONFIRM%"=="y" goto MENU
echo.
%PYTHON% %SCRIPT% --dedup
echo.
pause
goto MENU

:: ─────────────────────────────────────────
:DEDUP_DRY
cls
echo.
echo   [F드라이브 중복 확인 - DRY-RUN]
echo   실제 삭제 없이 중복 건수만 확인합니다.
echo.
%PYTHON% %SCRIPT% --dedup --dry-run
echo.
pause
goto MENU

:: ─────────────────────────────────────────
:END
echo.
echo   종료합니다.
echo.
endlocal
exit /b 0
