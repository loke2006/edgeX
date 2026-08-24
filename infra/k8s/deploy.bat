@echo off
REM =============================================================================
REM EdgeCloudX — Kubernetes Deployment Script
REM =============================================================================
REM Usage: deploy.bat [command]
REM =============================================================================

if "%1"=="" goto help
if "%1"=="up" goto up
if "%1"=="down" goto down
if "%1"=="status" goto status
if "%1"=="logs" goto logs
if "%1"=="build" goto build
if "%1"=="dry-run" goto dryrun
if "%1"=="port-forward" goto portforward
goto help

:build
echo Building Docker images for Kubernetes...
echo.
docker compose build
echo.
echo Images built. They are available to Docker Desktop Kubernetes automatically.
goto end

:dryrun
echo Validating Kubernetes manifests (dry-run)...
echo.
kubectl apply --dry-run=client -k infra\k8s\overlays\dev\
echo.
echo Dry-run complete. No resources were created.
goto end

:up
echo ============================================================
echo   EdgeCloudX — Deploying to Kubernetes (dev overlay)
echo ============================================================
echo.

echo [1/4] Building Docker images...
docker compose build
echo.

echo [2/4] Applying Kubernetes manifests...
kubectl apply -k infra\k8s\overlays\dev\
echo.

echo [3/4] Waiting for infrastructure pods...
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis -n edgecloudx --timeout=120s 2>nul
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgres -n edgecloudx --timeout=120s 2>nul
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n edgecloudx --timeout=180s 2>nul
echo.

echo [4/4] Waiting for application pods...
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=api -n edgecloudx --timeout=120s 2>nul
echo.

echo ============================================================
echo   EdgeCloudX is running on Kubernetes!
echo ============================================================
echo.
echo   Dashboard:  http://localhost:30080
echo   Grafana:    http://localhost:30030
echo   Prometheus: http://localhost:30090
echo   Jaeger:     http://localhost:30686
echo.
echo   Run: deploy.bat status   to check pod status
echo   Run: deploy.bat logs     to follow logs
echo   Run: deploy.bat down     to tear down
echo ============================================================
goto end

:down
echo Tearing down EdgeCloudX from Kubernetes...
kubectl delete -k infra\k8s\overlays\dev\
echo.
echo EdgeCloudX removed from Kubernetes.
echo Note: PersistentVolumeClaims are NOT deleted. Run 'kubectl delete pvc --all -n edgecloudx' to remove data.
goto end

:status
echo EdgeCloudX Pod Status:
echo.
kubectl get pods -n edgecloudx -o wide
echo.
echo Services:
kubectl get svc -n edgecloudx
echo.
echo HPAs:
kubectl get hpa -n edgecloudx
goto end

:logs
if "%2"=="" (
    echo Following all EdgeCloudX logs...
    kubectl logs -f -l app.kubernetes.io/part-of=edgecloudx -n edgecloudx --max-log-requests=20 --tail=50
) else (
    echo Following logs for %2...
    kubectl logs -f -l app.kubernetes.io/name=%2 -n edgecloudx --tail=50
)
goto end

:portforward
echo Starting port-forwards for all UIs...
echo.
echo Dashboard  -> http://localhost:8000
echo Grafana    -> http://localhost:3000
echo Prometheus -> http://localhost:9090
echo Jaeger     -> http://localhost:16686
echo.
echo Press Ctrl+C to stop.
start /b kubectl port-forward svc/dashboard 8000:8000 -n edgecloudx
start /b kubectl port-forward svc/grafana 3000:3000 -n edgecloudx
start /b kubectl port-forward svc/prometheus 9090:9090 -n edgecloudx
start /b kubectl port-forward svc/jaeger 16686:16686 -n edgecloudx
echo Port-forwards started in background.
goto end

:help
echo.
echo ============================================================
echo   EdgeCloudX — Kubernetes Deploy Commands
echo ============================================================
echo.
echo   deploy.bat build         Build Docker images
echo   deploy.bat dry-run       Validate manifests (no apply)
echo   deploy.bat up            Deploy full stack to K8s
echo   deploy.bat down          Tear down from K8s
echo   deploy.bat status        Show pod/svc/hpa status
echo   deploy.bat logs          Follow all logs (or: deploy.bat logs traffic-service)
echo   deploy.bat port-forward  Port-forward all UIs
echo.
echo ============================================================

:end
