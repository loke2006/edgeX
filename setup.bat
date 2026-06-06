@echo off
REM =============================================================================
REM EdgeCloudX — Windows Setup Script
REM =============================================================================
REM Run this once to set up the complete development environment
REM =============================================================================

echo.
echo ============================================================
echo   EdgeCloudX — Setup Script
echo ============================================================
echo.

REM --- Check Docker ---
echo [1/6] Checking Docker...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not installed or not in PATH!
    echo Please install Docker Desktop from https://www.docker.com/products/docker-desktop/
    exit /b 1
)
echo       Docker found!

REM --- Check Python ---
echo [2/6] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH!
    exit /b 1
)
echo       Python found!

REM --- Create Virtual Environment ---
echo [3/6] Creating virtual environment...
if not exist "venv" (
    python -m venv venv
    echo       Created venv/
) else (
    echo       venv/ already exists, skipping
)

REM --- Activate and Install Dependencies ---
echo [4/6] Installing Python dependencies...
call venv\Scripts\activate.bat
python -m pip install --upgrade pip --quiet
python -m pip install -r requirements-dev.txt --quiet
echo       Dependencies installed!

REM --- Copy Environment File ---
echo [5/6] Setting up environment...
if not exist ".env" (
    copy .env.example .env >nul
    echo       Created .env from .env.example
) else (
    echo       .env already exists, skipping
)

REM --- Pull Docker Images ---
echo [6/6] Pulling Docker images (this may take a few minutes)...
docker compose pull
echo       Docker images pulled!

echo.
echo ============================================================
echo   Setup Complete!
echo ============================================================
echo.
echo   To start the full stack:
echo     docker compose up -d --build
echo.
echo   To run tests:
echo     venv\Scripts\activate.bat
echo     pytest tests\unit\ -v
echo.
echo   Service URLs (after docker compose up):
echo     Traffic:    http://localhost:8001/docs
echo     Routing:    http://localhost:8002/docs
echo     Analytics:  http://localhost:8003/docs
echo     Alerts:     http://localhost:8004/docs
echo     Auth:       http://localhost:8005/docs
echo.
echo ============================================================
