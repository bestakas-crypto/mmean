@echo off
title MMEAN Analyzer Engine Server
color 0b

:: 이 bat 파일이 있는 디렉토리를 프로젝트 루트로 사용 (절대경로 불필요)
set PROJ_DIR=%~dp0

:: 가상환경 활성화 경로 설정 (미니콘다 기준)
set CONDA_PATH=%USERPROFILE%\miniconda3\Scripts\activate.bat

echo ======================================================
echo  MMEAN Market Brain Project - Analyzer Starting...
echo ======================================================

::  가상환경 mmean 활성화
echo [1/3] Activating Conda Environment: mmean...
call %CONDA_PATH% mmean

:: 프로젝트 위치로 이동 (bat 파일 위치 기준 — 절대경로 불필요)
echo [2/3] Moving to Project Directory...
cd /d "%PROJ_DIR%"

echo [3/3] Launching MMEAN Engine on Port 5001...
echo.
echo Dashboard: http://localhost:5001
echo.

python analyzer_app.py
pause