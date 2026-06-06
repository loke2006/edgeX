@echo off
REM =============================================================================
REM EdgeCloudX — Run Commands Helper
REM =============================================================================
REM Usage: run.bat [command]
REM =============================================================================

if "%1"=="" goto help

if "%1"=="up" goto up
if "%1"=="down" goto down
if "%1"=="build" goto build
if "%1"=="logs" goto logs
if "%1"=="test" goto test
if "%1"=="seed" goto seed
if "%1"=="health" goto health
if "%1"=="infra" goto infra
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="clean" goto clean
goto help

:up
echo Starting EdgeCloudX full stack...
docker compose up -d --build
goto end

:down
echo Stopping all services...
docker compose down
goto end

:build
echo Rebuilding all services...
docker compose build --no-cache
goto end

:logs
if "%2"=="" (
    docker compose logs -f --tail=50
) else (
    docker compose logs -f --tail=50 %2
)
goto end

:test
echo Running unit tests...
call venv\Scripts\activate.bat
pytest tests\unit\ -v --tb=short
goto end

:seed
echo Seeding test data...
call venv\Scripts\activate.bat
python scripts\seed_data.py
goto end

:health
echo Checking service health...
echo.
echo Traffic Service (8001):
curl -s http://localhost:8001/health 2>nul || echo   [OFFLINE]
echo.
echo Routing Service (8002):
curl -s http://localhost:8002/health 2>nul || echo   [OFFLINE]
echo.
echo Analytics Service (8003):
curl -s http://localhost:8003/health 2>nul || echo   [OFFLINE]
echo.
echo Alert Service (8004):
curl -s http://localhost:8004/health 2>nul || echo   [OFFLINE]
echo.
echo Auth Service (8005):
curl -s http://localhost:8005/health 2>nul || echo   [OFFLINE]
echo.
goto end

:infra
echo Starting infrastructure only (Kafka, Redis, PostgreSQL)...
docker compose up -d kafka redis postgres kafka-init
goto end

:stop
echo Stopping all containers...
docker compose stop
goto end

:restart
echo Restarting all services...
docker compose restart
goto end

:clean
echo Stopping and removing all containers, volumes, networks...
docker compose down -v --remove-orphans
goto end

:help
echo.
echo ============================================================
echo   EdgeCloudX — Run Commands
echo ============================================================
echo.
echo   run.bat up        Start full stack (build + run)
echo   run.bat down      Stop all services
echo   run.bat build     Rebuild all images (no cache)
echo   run.bat logs      Follow all logs (or: run.bat logs kafka)
echo   run.bat test      Run unit tests
echo   run.bat seed      Seed test data to Kafka
echo   run.bat health    Check all service health endpoints
echo   run.bat infra     Start infrastructure only
echo   run.bat stop      Stop containers (keep data)
echo   run.bat restart   Restart all services
echo   run.bat clean     Remove everything (containers + volumes)
echo.
echo ============================================================

:end
