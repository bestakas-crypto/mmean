@echo off
setlocal
cd /d "%~dp0"
cls
echo.
echo ================================================================
echo   MMEAN - RAG 사전작업 (rag_prep.py)
echo ================================================================
echo.
echo   step1: regime_ticks  -^> market_features 변환  [step2의 전제]
echo   step2: market_features + trades -^> pattern_memory 집계
echo   step3: LLM 평가 점수 계산
echo   step4: judgment_events -^> failure_patterns 집계
echo   step0: 전체 실행 (1+2+3+4)
echo.
echo   * step1+2+4 는 엔진 시작 시 자동 실행됨 (6시간 이상 경과 시)
echo   * 수동 실행이 필요한 경우에만 사용
echo.

:MENU
echo --------------------------------
echo  [1] step1        - market_features 변환 (regime_ticks 필요)
echo  [2] step2        - pattern_memory 재집계
echo  [3] step4        - failure_patterns 재집계
echo  [4] step1+2+4    - 경고 패턴 전체 재집계  [권장]
echo  [5] step0        - 전체 (1+2+3+4)
echo  [0] 종료
echo --------------------------------
set /p CHOICE="선택 (0~5): "

if "%CHOICE%"=="1" goto STEP1
if "%CHOICE%"=="2" goto STEP2
if "%CHOICE%"=="3" goto STEP4
if "%CHOICE%"=="4" goto STEP124
if "%CHOICE%"=="5" goto STEP0
if "%CHOICE%"=="0" goto END
echo 잘못된 입력. 다시 선택하세요.
goto MENU

:STEP1
echo.
echo [step1] market_features 변환 중...
python rag\rag_prep.py --step 1
goto DONE

:STEP2
echo.
echo [step2] pattern_memory 집계 중...
python rag\rag_prep.py --step 2
goto DONE

:STEP4
echo.
echo [step4] failure_patterns 집계 중...
python rag\rag_prep.py --step 4
goto DONE

:STEP124
echo.
echo [step1] market_features 변환 중...
python rag\rag_prep.py --step 1
echo.
echo [step2] pattern_memory 집계 중...
python rag\rag_prep.py --step 2
echo.
echo [step4] failure_patterns 집계 중...
python rag\rag_prep.py --step 4
goto DONE

:STEP0
echo.
echo [step0] 전체 RAG 사전작업 실행 중...
python rag\rag_prep.py --step 0
goto DONE

:DONE
echo.
echo 완료.
pause
goto MENU

:END
echo 종료합니다.
endlocal
