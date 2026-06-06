"""
EdgeCloudX Routing Service — Main Application
==============================================
FastAPI microservice for EV pathfinding and route optimization.
Uses A* algorithm with real-time congestion data from Redis.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import get_settings
from app.routers import routing

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("EdgeCloudX Routing Service — Starting")
    yield
    logger.info("Routing Service stopped")


app = FastAPI(
    title="EdgeCloudX Routing Service",
    description="EV pathfinding and route optimization with real-time congestion awareness",
    version="0.1.0",
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

Instrumentator().instrument(app).expose(app, include_in_schema=False)
app.include_router(routing.router)


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.1.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Routing Service", "version": "0.1.0", "docs": "/docs"}
