"""
EdgeCloudX Routing Service — Main Application
==============================================
FastAPI microservice for EV pathfinding and route optimization.
Uses A* algorithm with real-time congestion data from Redis.

Enhanced with: structured logging, security headers, Prometheus metrics.
"""

import os
import sys

# Add shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from shared.logging import setup_logging  # noqa: E402

setup_logging("routing-service")

import logging  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from prometheus_fastapi_instrumentator import Instrumentator  # noqa: E402
from shared.metrics import SERVICE_INFO  # noqa: E402
from shared.middleware import add_security_headers  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.routers import routing  # noqa: E402

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Routing Service starting")

    # Store auth service URL for RBAC
    app.state.auth_service_url = os.environ.get(
        "AUTH_SERVICE_URL", "http://auth-service:8000"
    )

    SERVICE_INFO.info({"service": "routing-service", "version": "0.2.0"})

    yield
    logger.info("Routing Service stopped")


app = FastAPI(
    title="EdgeCloudX Routing Service",
    description="EV pathfinding and route optimization with real-time congestion awareness",
    version="0.2.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

add_security_headers(app)
Instrumentator().instrument(app).expose(app, include_in_schema=False)
app.include_router(routing.router)


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.2.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Routing Service", "version": "0.2.0", "docs": "/docs"}
