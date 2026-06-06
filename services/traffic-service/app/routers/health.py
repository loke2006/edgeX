"""
EdgeCloudX Traffic Service — Health Check Router
"""

from datetime import datetime

import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import APIRouter
from sqlalchemy import text

from app.config import get_settings
from app.models.database import async_session
from app.schemas.traffic import HealthResponse

router = APIRouter(tags=["health"])
settings = get_settings()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check for the traffic service."""

    kafka_ok = False
    redis_ok = False
    db_ok = False

    # Check Kafka
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            request_timeout_ms=3000,
        )
        await producer.start()
        await producer.stop()
        kafka_ok = True
    except Exception:
        pass

    # Check Redis
    try:
        r = aioredis.from_url(settings.redis_url, socket_connect_timeout=3)
        pong = await r.ping()
        redis_ok = bool(pong)
        await r.aclose()
    except Exception:
        pass

    # Check PostgreSQL
    try:
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass

    all_ok = kafka_ok and redis_ok and db_ok

    return HealthResponse(
        service=settings.service_name,
        status="healthy" if all_ok else "degraded",
        kafka_connected=kafka_ok,
        redis_connected=redis_ok,
        db_connected=db_ok,
        timestamp=datetime.utcnow(),
    )


@router.get("/health/liveness")
async def liveness():
    """Simple liveness probe for Kubernetes."""
    return {"status": "alive"}


@router.get("/health/readiness")
async def readiness():
    """Readiness probe — checks if service can handle traffic."""
    try:
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception:
        return {"status": "not_ready"}, 503
