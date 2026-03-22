@echo off
cd /d "%~dp0"
cls

:MENU
echo.
echo ============================================================
echo   RAG Builder Menu
echo ============================================================
echo.
echo   1. pattern_docs 생성       (신규/갱신)
echo   2. pattern_docs 재생성     (전체 삭제 후 재빌드)
echo   3. 전체 빌드               (rag_documents + pattern_docs)
echo   4. 검증                    (현재 상태 점검표 출력)
echo   5. Phase 1 only            (rag_documents만)
echo.
echo   0. 종료
echo.
set /p CHOICE=번호 선택:

if "%CHOICE%"=="1" goto BUILD
if "%CHOICE%"=="2" goto REBUILD
if "%CHOICE%"=="3" goto ALL
if "%CHOICE%"=="4" goto VERIFY
if "%CHOICE%"=="5" goto PHASE1
if "%CHOICE%"=="0" goto END

echo 잘못된 번호입니다.
goto MENU

:BUILD
echo.
echo [1] pattern_docs 생성 중...
python sim_rag_builder.py --pattern-docs
goto DONE

:REBUILD
echo.
echo [2] pattern_docs 전체 재생성 중...
python sim_rag_builder.py --pattern-docs --rebuild
goto DONE

:ALL
echo.
echo [3] 전체 빌드 중 (rag_documents + pattern_docs)...
python sim_rag_builder.py --all
goto DONE

:VERIFY
echo.
echo [4] 검증 중...
python sim_rag_builder.py --verify
goto DONE

:PHASE1
echo.
echo [5] Phase 1 (rag_documents) 빌드 중...
python sim_rag_builder.py
goto DONE

:DONE
echo.
if errorlevel 1 (
    echo [오류] 실행 실패. 종료 코드: %errorlevel%
) else (
    echo [완료]
)
echo.
set /p AGAIN=메뉴로 돌아가기? (y/n):
if /i "%AGAIN%"=="y" goto MENU

:END
exit /b
