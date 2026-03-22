@echo off
setlocal enabledelayedexpansion

REM =========================================================
REM [MMEAN] setup_conda_and_freeze.bat
REM 기능:
REM 1) conda 초기화 확인
REM 2) conda 환경 생성
REM 3) 환경 활성화
REM 4) pip / setuptools / wheel 업그레이드
REM 5) requirements.txt가 있으면 설치
REM 6) environment.yml export
REM 7) pip freeze 저장
REM =========================================================

cd /d "%~dp0"

REM ===== 사용자 설정 =====
set ENV_NAME=mmean
set PYTHON_VERSION=3.11
set BASE_REQ=requirements.txt
set LOCK_FILE=requirements.lock.txt
set ENV_YML=environment.yml
REM ======================

echo.
echo ========================================
echo MMEAN Miniconda env setup + freeze start
echo Project Root : %cd%
echo Env Name     : %ENV_NAME%
echo Python Ver   : %PYTHON_VERSION%
echo ========================================
echo.

REM ---------------------------------------------------------
REM 1) conda 명령 확인
REM ---------------------------------------------------------
where conda >nul 2>nul
if errorlevel 1 (
    echo [ERROR] conda 명령을 찾을 수 없습니다.
    echo Miniconda Prompt에서 실행하거나, conda PATH 확인이 필요합니다.
    pause
    exit /b 1
)

REM ---------------------------------------------------------
REM 2) conda hook 로드
REM ---------------------------------------------------------
for /f "delims=" %%i in ('conda shell.bat hook') do %%i
if errorlevel 1 (
    echo [ERROR] conda shell hook 로드 실패
    echo 일반 CMD 대신 Miniconda Prompt에서 실행해 보세요.
    pause
    exit /b 1
)

REM ---------------------------------------------------------
REM 3) 환경 존재 여부 확인
REM ---------------------------------------------------------
set ENV_EXISTS=0
for /f "tokens=1" %%a in ('conda env list ^| findstr /r /c:"^%ENV_NAME% " /c:"^%ENV_NAME%\*"') do (
    set ENV_EXISTS=1
)

if "%ENV_EXISTS%"=="1" (
    echo [INFO] 기존 conda 환경이 이미 존재합니다: %ENV_NAME%
) else (
    echo [INFO] conda 환경 생성 중...
    call conda create -n %ENV_NAME% python=%PYTHON_VERSION% -y
    if errorlevel 1 (
        echo [ERROR] conda 환경 생성 실패
        pause
        exit /b 1
    )
    echo [OK] conda 환경 생성 완료
)

REM ---------------------------------------------------------
REM 4) 환경 활성화
REM ---------------------------------------------------------
echo [INFO] conda 환경 활성화...
call conda activate %ENV_NAME%
if errorlevel 1 (
    echo [ERROR] conda 환경 활성화 실패
    pause
    exit /b 1
)

REM ---------------------------------------------------------
REM 5) 기본 도구 업그레이드
REM ---------------------------------------------------------
echo [INFO] pip / setuptools / wheel 업그레이드...
python -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
    echo [ERROR] pip 업그레이드 실패
    pause
    exit /b 1
)

REM ---------------------------------------------------------
REM 6) requirements.txt 있으면 설치
REM ---------------------------------------------------------
if exist "%BASE_REQ%" (
    echo [INFO] %BASE_REQ% 발견 - 패키지 설치 진행...
    pip install -r "%BASE_REQ%"
    if errorlevel 1 (
        echo [ERROR] requirements 설치 실패
        pause
        exit /b 1
    )
    echo [OK] requirements 설치 완료
) else (
    echo [INFO] %BASE_REQ% 없음 - 설치 단계는 건너뜁니다.
)

REM ---------------------------------------------------------
REM 7) conda 환경 export
REM ---------------------------------------------------------
echo [INFO] conda environment export 저장 중...
call conda env export --name %ENV_NAME% > "%ENV_YML%"
if errorlevel 1 (
    echo [ERROR] conda env export 실패
    pause
    exit /b 1
)
echo [OK] export 완료: %ENV_YML%

REM ---------------------------------------------------------
REM 8) pip freeze 저장
REM ---------------------------------------------------------
echo [INFO] pip freeze 저장 중...
pip freeze > "%LOCK_FILE%"
if errorlevel 1 (
    echo [ERROR] pip freeze 실패
    pause
    exit /b 1
)
echo [OK] freeze 완료: %LOCK_FILE%

REM ---------------------------------------------------------
REM 9) 활성 환경 정보 출력
REM ---------------------------------------------------------
echo.
echo [INFO] 현재 python / pip 위치
where python
where pip

echo.
echo [INFO] conda env list
conda env list

echo.
echo ========================================
echo 모든 작업이 완료되었습니다.
echo - Conda export : %ENV_YML%
echo - Pip freeze   : %LOCK_FILE%
echo ========================================
echo.

pause
exit /b 0