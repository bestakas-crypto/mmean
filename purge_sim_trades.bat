@echo off
cd /d "%~dp0"
chcp 65001 > nul

echo.
echo ================================================================
echo   MMEAN — sim_trades 정리 (검증 완료 데이터 용량 회수)
echo ================================================================
echo.
echo  [1] 현황 확인 (삭제 없음)
echo  [2] 삭제 예정 건수만 확인 (dry-run)
echo  [3] 전체 정리 (done + 집계완료 run 전부)
echo  [4] 최근 3일 제외하고 정리
echo  [5] validation run만 정리
echo  [6] 직접 입력 (고급)
echo  [0] 취소
echo.
set /p CHOICE="번호 선택: "

if "%CHOICE%"=="1" (
    python db\purge_sim_trades.py --status
    goto END
)
if "%CHOICE%"=="2" (
    python db\purge_sim_trades.py --dry-run
    goto END
)
if "%CHOICE%"=="3" (
    echo.
    echo  경고: done 상태이고 집계 완료된 모든 run의 sim_trades를 삭제합니다.
    set /p CONFIRM="계속하려면 Y 입력: "
    if /i "%CONFIRM%"=="Y" (
        python db\purge_sim_trades.py
    ) else (
        echo 취소되었습니다.
    )
    goto END
)
if "%CHOICE%"=="4" (
    python db\purge_sim_trades.py --keep-days 3
    goto END
)
if "%CHOICE%"=="5" (
    python db\purge_sim_trades.py --run-type validation
    goto END
)
if "%CHOICE%"=="6" (
    echo.
    echo  옵션 예시:
    echo    --keep-days 7          최근 7일 제외 정리
    echo    --run-type validation  validation run만
    echo    --run-ids 10 11 12     특정 run_id만
    echo    --dry-run              건수만 확인
    echo.
    set /p OPTS="옵션 입력: "
    python db\purge_sim_trades.py %OPTS%
    goto END
)
if "%CHOICE%"=="0" (
    echo 취소되었습니다.
    goto END
)

echo 잘못된 선택입니다.

:END
echo.
pause
