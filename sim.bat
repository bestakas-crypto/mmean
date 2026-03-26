@echo off

title MMEAN Simulation 
color 0b

:: 이 bat 파일이 있는 디렉토리를 프로젝트 루트로 사용 (절대경로 불필요)
set PROJ_DIR=%~dp0

:: 가상환경 활성화 경로 설정 (미니콘다 기준)
set CONDA_PATH=%USERPROFILE%\miniconda3\Scripts\activate.bat

echo ======================================================
echo  MMEAN Simulation
echo ======================================================

::  가상환경 mmean 활성화
echo [1/3] Activating Conda Environment: mmean...
call %CONDA_PATH% mmean

:: 프로젝트 위치로 이동 (bat 파일 위치 기준 — 절대경로 불필요)
echo [2/3] Moving to Project Directory...
cd /d "%PROJ_DIR%"
cd /d "%~dp0"
python sim.py %*
if errorlevel 1 (
    echo.
    echo [ERROR] sim.py failed. Exit code: %errorlevel%
    pause
    exit /b 1
)

echo.
echo [RAG] Checking sim_adoptions...
python -c "import sqlite3,sys; c=sqlite3.connect('storage/sim.db'); n=c.execute('SELECT COUNT(*) FROM sim_adoptions').fetchone()[0]; c.close(); print(f'[RAG] sim_adoptions={n}'); sys.exit(0 if n>0 else 1)"
if errorlevel 1 (
    echo [RAG] No adoptions yet - skipping rebuild.
) else (
    echo [RAG] Applying DB migrations...
    python db\db_setup_sim.py storage\sim.db
    echo [RAG] Rebuilding RAG documents...
    python rag\sim_rag_builder.py --rebuild
    if errorlevel 1 (
        echo [RAG] Build failed - check logs above
    ) else (
        echo [RAG] Done
    )
)
echo.

echo [SHADOW] Running paper_shadow evaluation...
python -c "import sqlite3,sys; c=sqlite3.connect('storage/sim.db'); n=c.execute(\"SELECT COUNT(*) FROM sim_adoptions WHERE status='paper_shadow'\").fetchone()[0]; c.close(); print(f'[SHADOW] paper_shadow count={n}'); sys.exit(0 if n>0 else 1)"
if errorlevel 1 (
    echo [SHADOW] No paper_shadow candidates - skipping.
) else (
    python sim_opt\shadow_evaluator.py
    if errorlevel 1 (
        echo [SHADOW] Evaluation failed - check logs above
    ) else (
        echo [SHADOW] Done
    )
)
echo.
