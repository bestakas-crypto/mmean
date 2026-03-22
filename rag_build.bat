@echo off
cd /d "%~dp0"
title RAG Builder - MMEAN
color 0a

:MENU
cls
echo ============================================================
echo   RAG Builder - MMEAN Pattern Docs
echo ============================================================
echo.
echo   [1] Phase 1       rag_documents 생성 (regime_tags 포함)
echo   [2] Phase 2A      pattern_docs 생성 (VS / PC / FC)
echo   [3] Phase 1+2A    전체 생성
echo   [4] 재생성        pattern_docs 전체 삭제 후 재빌드
echo   [5] 검증          현재 pattern_docs 상태 점검
echo   [6] 종료
echo.
echo ============================================================
set /p CHOICE=  선택 (1~6):

if "%CHOICE%"=="1" goto PHASE1
if "%CHOICE%"=="2" goto PHASE2
if "%CHOICE%"=="3" goto ALL
if "%CHOICE%"=="4" goto REBUILD
if "%CHOICE%"=="5" goto VERIFY
if "%CHOICE%"=="6" goto END

echo.
echo [오류] 잘못된 입력입니다.
timeout /t 2 >nul
goto MENU

:PHASE1
echo.
echo [Phase 1] rag_documents 생성 중...
echo.
python rag/sim_rag_builder.py
if errorlevel 1 (
    echo.
    echo [오류] 실행 실패. 종료 코드: %errorlevel%
)
goto DONE

:PHASE2
echo.
echo [Phase 2A] pattern_docs 생성 중...
echo.
python rag/sim_rag_builder.py --pattern-docs
if errorlevel 1 (
    echo.
    echo [오류] 실행 실패. 종료 코드: %errorlevel%
)
goto DONE

:ALL
echo.
echo [Phase 1+2A] 전체 생성 중...
echo.
python rag/sim_rag_builder.py --all
if errorlevel 1 (
    echo.
    echo [오류] 실행 실패. 종료 코드: %errorlevel%
)
goto DONE

:REBUILD
echo.
echo [경고] pattern_docs 전체 삭제 후 재생성합니다.
set /p CONFIRM=  계속하려면 Y 입력:
if /i not "%CONFIRM%"=="Y" goto MENU
echo.
python rag/sim_rag_builder.py --pattern-docs --rebuild
if errorlevel 1 (
    echo.
    echo [오류] 실행 실패. 종료 코드: %errorlevel%
)
goto DONE

:VERIFY
echo.
echo [검증] pattern_docs 상태 점검 중...
echo.
python rag/sim_rag_builder.py --verify
if errorlevel 1 (
    echo.
    echo [오류] 실행 실패. 종료 코드: %errorlevel%
)
goto DONE

:DONE
echo.
echo ============================================================
pause
goto MENU

:END
exit
