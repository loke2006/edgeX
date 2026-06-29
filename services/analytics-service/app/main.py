"""
EdgeCloudX Analytics Service — Main Application
=================================================
FastAPI microservice for congestion analytics, historical data, and trends.

Enhanced with:
- Historical analytics from PostgreSQL (hourly, daily, trends)
- Background aggregation job
- Audit log query endpoint
- Structured JSON logging
- Security headers
"""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache

# Add shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "shared"))

from shared.logging import setup_logging  # noqa: E402

setup_logging("analytics-service")

import logging  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402
from fastapi import FastAPI, Query  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from prometheus_fastapi_instrumentator import Instrumentator  # noqa: E402
from pydantic import BaseModel  # noqa: E402
from pydantic_settings import BaseSettings  # noqa: E402
from shared.audit import AuditLogger  # noqa: E402
from shared.metrics import SERVICE_INFO  # noqa: E402
from shared.middleware import add_security_headers  # noqa: E402
from sqlalchemy import (  # noqa: E402
    Column,
    DateTime,
    Float,
    Integer,
    String,
    func,
    select,
    text,
)
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase  # noqa: E402

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
    aggregation_interval_minutes: int = 5

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

# Database
engine = create_async_engine(settings.database_url, pool_size=5)
analytics_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class AnalyticsBase(DeclarativeBase):
    pass


class HourlyAggregation(AnalyticsBase):
    """Pre-computed hourly statistics per intersection."""

    __tablename__ = "hourly_aggregations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    intersection_id = Column(String(50), nullable=False, index=True)
    hour_start = Column(DateTime, nullable=False, index=True)
    avg_congestion = Column(Float, default=0.0)
    max_congestion = Column(Float, default=0.0)
    min_congestion = Column(Float, default=0.0)
    total_vehicles = Column(Integer, default=0)
    event_count = Column(Integer, default=0)
    anomaly_count = Column(Integer, default=0)


class DailyAggregation(AnalyticsBase):
    """Daily summary statistics per intersection."""

    __tablename__ = "daily_aggregations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    intersection_id = Column(String(50), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    avg_congestion = Column(Float, default=0.0)
    peak_congestion = Column(Float, default=0.0)
    total_vehicles = Column(Integer, default=0)
    total_events = Column(Integer, default=0)
    total_anomalies = Column(Integer, default=0)
    peak_hour = Column(Integer, default=0)  # 0-23


# ── Background aggregation ──


async def run_aggregation_loop():
    """Periodically aggregate traffic_events into hourly/daily tables."""
    while True:
        try:
            await _run_hourly_aggregation()
        except Exception as e:
            logger.error(f"Aggregation error: {e}", exc_info=True)
        await asyncio.sleep(settings.aggregation_interval_minutes * 60)


async def _run_hourly_aggregation():
    """Aggregate recent traffic events into hourly stats."""
    async with analytics_session() as session:
        try:
            # Aggregate last hour of traffic events into hourly_aggregations
            now = datetime.now(timezone.utc)
            hour_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

            result = await session.execute(
                text("""
                    INSERT INTO hourly_aggregations
                        (intersection_id, hour_start, avg_congestion, max_congestion,
                         min_congestion, total_vehicles, event_count, anomaly_count)
                    SELECT
                        intersection_id,
                        date_trunc('hour', event_timestamp) as hour_start,
                        AVG(congestion_score),
                        MAX(congestion_score),
                        MIN(congestion_score),
                        SUM(vehicle_count),
                        COUNT(*),
                        SUM(CASE WHEN anomaly_detected THEN 1 ELSE 0 END)
                    FROM traffic_events
                    WHERE event_timestamp >= :hour_start
                      AND event_timestamp < :hour_end
                    GROUP BY intersection_id, date_trunc('hour', event_timestamp)
                    ON CONFLICT DO NOTHING
                """),
                {"hour_start": hour_start, "hour_end": hour_start + timedelta(hours=1)},
            )
            await session.commit()
            logger.info(
                "Hourly aggregation complete",
                extra={"hour": hour_start.isoformat()},
            )
        except Exception as e:
            logger.warning(f"Hourly aggregation failed (table may not exist yet): {e}")
            await session.rollback()


# ── Schemas ──


class CongestionReport(BaseModel):
    grid_rows: int
    grid_cols: int
    intersections: list[dict]
    hotspots: list[dict]
    avg_congestion: float
    peak_congestion: float
    total_vehicles: int
    timestamp: str


class HistoricalDataPoint(BaseModel):
    intersection_id: str
    period_start: str
    avg_congestion: float
    max_congestion: float
    total_vehicles: int
    event_count: int
    anomaly_count: int


# ── App ──


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Analytics Service starting")

    # Create analytics tables
    async with engine.begin() as conn:
        await conn.run_sync(AnalyticsBase.metadata.create_all)
    logger.info("Analytics tables initialized")

    # Initialize audit logger (for query endpoint)
    audit = AuditLogger(settings.database_url)
    await audit.init()
    app.state.audit = audit

    # Prometheus info
    SERVICE_INFO.info({"service": "analytics-service", "version": "0.2.0"})

    # Start background aggregation
    agg_task = asyncio.create_task(run_aggregation_loop())
    logger.info("Background aggregation started")

    yield

    agg_task.cancel()
    try:
        await agg_task
    except asyncio.CancelledError:
        pass
    logger.info("Analytics Service stopped")


app = FastAPI(
    title="EdgeCloudX Analytics Service",
    description="Congestion analytics, historical data, trends, and audit log queries",
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
        timestamp=datetime.now(timezone.utc).isoformat(),
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/analytics/history/hourly")
async def get_hourly_history(
    intersection_id: str = Query(None, description="Filter by intersection"),
    hours: int = Query(24, ge=1, le=168, description="Hours of history"),
):
    """Get hourly historical analytics."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with analytics_session() as session:
        stmt = select(HourlyAggregation).where(
            HourlyAggregation.hour_start >= since
        ).order_by(HourlyAggregation.hour_start.desc())

        if intersection_id:
            stmt = stmt.where(HourlyAggregation.intersection_id == intersection_id)

        stmt = stmt.limit(500)
        result = await session.execute(stmt)
        rows = result.scalars().all()

        return {
            "period_hours": hours,
            "count": len(rows),
            "data": [
                {
                    "intersection_id": r.intersection_id,
                    "hour_start": r.hour_start.isoformat() if r.hour_start else None,
                    "avg_congestion": round(r.avg_congestion, 3),
                    "max_congestion": round(r.max_congestion, 3),
                    "total_vehicles": r.total_vehicles,
                    "event_count": r.event_count,
                    "anomaly_count": r.anomaly_count,
                }
                for r in rows
            ],
        }


@app.get("/analytics/history/daily")
async def get_daily_history(
    days: int = Query(7, ge=1, le=30, description="Days of history"),
):
    """Get daily historical analytics."""
    since = datetime.now(timezone.utc) - timedelta(days=days)

    async with analytics_session() as session:
        stmt = select(DailyAggregation).where(
            DailyAggregation.date >= since
        ).order_by(DailyAggregation.date.desc()).limit(200)

        result = await session.execute(stmt)
        rows = result.scalars().all()

        return {
            "period_days": days,
            "count": len(rows),
            "data": [
                {
                    "intersection_id": r.intersection_id,
                    "date": r.date.isoformat() if r.date else None,
                    "avg_congestion": round(r.avg_congestion, 3),
                    "peak_congestion": round(r.peak_congestion, 3),
                    "total_vehicles": r.total_vehicles,
                    "total_events": r.total_events,
                    "total_anomalies": r.total_anomalies,
                    "peak_hour": r.peak_hour,
                }
                for r in rows
            ],
        }


@app.get("/analytics/trends")
async def get_trends():
    """Get congestion trend analysis (improving/worsening/stable per intersection)."""
    async with analytics_session() as session:
        # Compare last 6 hours avg vs previous 6 hours avg
        now = datetime.now(timezone.utc)
        recent_start = now - timedelta(hours=6)
        prev_start = now - timedelta(hours=12)

        trends = []
        for row in range(settings.grid_rows):
            for col in range(settings.grid_cols):
                iid = f"int-{row}-{col}"

                # Recent average
                result = await session.execute(
                    select(func.avg(HourlyAggregation.avg_congestion)).where(
                        HourlyAggregation.intersection_id == iid,
                        HourlyAggregation.hour_start >= recent_start,
                    )
                )
                recent_avg = result.scalar() or 0.0

                # Previous average
                result = await session.execute(
                    select(func.avg(HourlyAggregation.avg_congestion)).where(
                        HourlyAggregation.intersection_id == iid,
                        HourlyAggregation.hour_start >= prev_start,
                        HourlyAggregation.hour_start < recent_start,
                    )
                )
                prev_avg = result.scalar() or 0.0

                # Determine trend
                if prev_avg == 0:
                    trend = "stable"
                elif recent_avg > prev_avg * 1.1:
                    trend = "worsening"
                elif recent_avg < prev_avg * 0.9:
                    trend = "improving"
                else:
                    trend = "stable"

                trends.append({
                    "intersection_id": iid,
                    "recent_avg": round(recent_avg, 3),
                    "previous_avg": round(prev_avg, 3),
                    "trend": trend,
                })

        return {
            "analysis_window_hours": 12,
            "trends": trends,
            "timestamp": now.isoformat(),
        }


@app.get("/analytics/peak-hours")
async def get_peak_hours():
    """Get busiest hours of the day based on historical data."""
    async with analytics_session() as session:
        # Get hourly averages across all intersections
        result = await session.execute(
            text("""
                SELECT
                    EXTRACT(HOUR FROM hour_start) as hour_of_day,
                    AVG(avg_congestion) as avg_congestion,
                    SUM(total_vehicles) as total_vehicles
                FROM hourly_aggregations
                WHERE hour_start >= NOW() - INTERVAL '7 days'
                GROUP BY EXTRACT(HOUR FROM hour_start)
                ORDER BY avg_congestion DESC
            """)
        )
        rows = result.fetchall()

        return {
            "period": "last_7_days",
            "peak_hours": [
                {
                    "hour": int(r[0]),
                    "avg_congestion": round(float(r[1]), 3),
                    "total_vehicles": int(r[2]),
                }
                for r in rows
            ],
        }


@app.get("/analytics/history")
async def get_history(
    hours: int = Query(24, ge=1, le=168, description="Hours of history to retrieve"),
):
    """Get historical analytics summary."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with analytics_session() as session:
        result = await session.execute(
            select(
                func.avg(HourlyAggregation.avg_congestion),
                func.max(HourlyAggregation.max_congestion),
                func.sum(HourlyAggregation.event_count),
                func.sum(HourlyAggregation.anomaly_count),
            ).where(HourlyAggregation.hour_start >= since)
        )
        row = result.one_or_none()

        return {
            "period_hours": hours,
            "avg_congestion": round(float(row[0] or 0), 3),
            "peak_congestion": round(float(row[1] or 0), 3),
            "total_events": int(row[2] or 0),
            "anomalies_detected": int(row[3] or 0),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


@app.get("/audit/logs")
async def get_audit_logs(
    action: str = Query(None, description="Filter by action type"),
    actor: str = Query(None, description="Filter by actor"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Query audit logs."""
    if hasattr(app.state, "audit"):
        logs = await app.state.audit.query(action=action, actor=actor, limit=limit)
        return {"count": len(logs), "logs": logs}
    return {"count": 0, "logs": []}


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.2.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Analytics Service", "version": "0.2.0", "docs": "/docs"}
