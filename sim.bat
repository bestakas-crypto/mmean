@echo off
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
