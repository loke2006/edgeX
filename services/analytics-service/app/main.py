"""
EdgeCloudX Analytics Service — Main Application
=================================================
FastAPI microservice for congestion analytics and historical data.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from functools import lru_cache

import redis.asyncio as aioredis
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from pydantic_settings import BaseSettings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    service_name: str = "analytics-service"
    debug: bool = False
    kafka_bootstrap_servers: str = "kafka:9092"
    redis_url: str = "redis://redis:6379/0"
    database_url: str = "postgresql+asyncpg://edgecloudx:edgecloudx_secret@postgres:5432/edgecloudx"
    redis_intersection_prefix: str = "intersection:"
    grid_rows: int = 4
    grid_cols: int = 4

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("EdgeCloudX Analytics Service — Starting")
    yield
    logger.info("Analytics Service stopped")


app = FastAPI(
    title="EdgeCloudX Analytics Service",
    description="Congestion analytics, historical data, and traffic intelligence",
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


class CongestionReport(BaseModel):
    grid_rows: int
    grid_cols: int
    intersections: list[dict]
    hotspots: list[dict]
    avg_congestion: float
    peak_congestion: float
    total_vehicles: int
    timestamp: str


@app.get("/analytics/congestion", response_model=CongestionReport)
async def get_congestion_analytics():
    """Get real-time congestion analytics from Redis state."""
    intersections = []
    hotspots = []
    total_vehicles = 0
    congestion_scores = []

    try:
        r = aioredis.from_url(settings.redis_url)
        for row in range(settings.grid_rows):
            for col in range(settings.grid_cols):
                key = f"{settings.redis_intersection_prefix}int-{row}-{col}"
                data = await r.hgetall(key)
                if data:
                    score = float(data.get(b"congestion_score", b"0.0"))
                    vehicles = int(data.get(b"vehicle_count", b"0"))
                    entry = {
                        "intersection_id": f"int-{row}-{col}",
                        "row": row,
                        "col": col,
                        "congestion_score": score,
                        "vehicle_count": vehicles,
                        "congestion_level": data.get(b"congestion_level", b"low").decode(),
                        "signal_state": data.get(b"signal_state", b"red").decode(),
                    }
                    intersections.append(entry)
                    congestion_scores.append(score)
                    total_vehicles += vehicles

                    if score >= 0.7:
                        hotspots.append(entry)
                else:
                    intersections.append({
                        "intersection_id": f"int-{row}-{col}",
                        "row": row,
                        "col": col,
                        "congestion_score": 0.0,
                        "vehicle_count": 0,
                        "congestion_level": "low",
                        "signal_state": "red",
                    })
                    congestion_scores.append(0.0)

        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")

    avg = sum(congestion_scores) / len(congestion_scores) if congestion_scores else 0.0
    peak = max(congestion_scores) if congestion_scores else 0.0

    return CongestionReport(
        grid_rows=settings.grid_rows,
        grid_cols=settings.grid_cols,
        intersections=intersections,
        hotspots=hotspots,
        avg_congestion=round(avg, 3),
        peak_congestion=round(peak, 3),
        total_vehicles=total_vehicles,
        timestamp=datetime.utcnow().isoformat(),
    )


@app.get("/analytics/heatmap")
async def get_heatmap():
    """Get congestion heatmap data as a 2D matrix."""
    heatmap = []
    try:
        r = aioredis.from_url(settings.redis_url)
        for row in range(settings.grid_rows):
            row_data = []
            for col in range(settings.grid_cols):
                key = f"{settings.redis_intersection_prefix}int-{row}-{col}"
                data = await r.hgetall(key)
                score = float(data.get(b"congestion_score", b"0.0")) if data else 0.0
                row_data.append(round(score, 3))
            heatmap.append(row_data)
        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        heatmap = [[0.0] * settings.grid_cols for _ in range(settings.grid_rows)]

    return {
        "grid_rows": settings.grid_rows,
        "grid_cols": settings.grid_cols,
        "heatmap": heatmap,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/analytics/history")
async def get_history(
    hours: int = Query(24, ge=1, le=168, description="Hours of history to retrieve"),
):
    """Get historical analytics summary (placeholder — requires DB integration)."""
    return {
        "period_hours": hours,
        "message": "Historical data will be populated once traffic events are flowing",
        "avg_congestion": 0.0,
        "peak_congestion": 0.0,
        "total_events": 0,
        "anomalies_detected": 0,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.1.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Analytics Service", "version": "0.1.0", "docs": "/docs"}
