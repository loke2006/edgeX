"""
EdgeCloudX Alert Service — Main Application
=============================================
FastAPI microservice for emergency alert management.
Consumes emergency-alerts from Kafka, triggers green corridors,
and notifies the dashboard via Redis Pub/Sub.
"""

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from functools import lru_cache
from typing import Optional

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    service_name: str = "alert-service"
    debug: bool = False
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_emergency_topic: str = "emergency-alerts"
    kafka_consumer_group: str = "alert-service-group"
    redis_url: str = "redis://redis:6379/0"
    redis_alert_channel: str = "alerts:emergency"

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

# In-memory alert store (would use DB in production)
active_alerts: dict[str, dict] = {}


class EmergencyAlert(BaseModel):
    alert_type: str = Field(..., description="Type: ambulance, fire, accident, breakdown")
    intersection_id: str = Field(..., description="Intersection where emergency occurred")
    severity: str = Field("high", description="Severity: low, medium, high, critical")
    description: Optional[str] = None
    source_node_id: Optional[str] = None


class AlertResponse(BaseModel):
    alert_id: str
    alert_type: str
    intersection_id: str
    severity: str
    description: Optional[str]
    status: str
    created_at: str


class AlertConsumer:
    """Kafka consumer for emergency-alerts topic."""

    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            settings.kafka_emergency_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        for attempt in range(10):
            try:
                await self.consumer.start()
                self._running = True
                logger.info(
                    f"Alert Kafka consumer started — topic: {settings.kafka_emergency_topic}"
                )
                return
            except Exception as e:
                wait = min(2 ** attempt, 30)
                logger.warning(f"Kafka attempt {attempt + 1}/10 failed: {e}. Retry in {wait}s...")
                await asyncio.sleep(wait)
        logger.error("Failed to connect alert consumer to Kafka")

    async def stop(self):
        self._running = False
        if self.consumer:
            await self.consumer.stop()

    async def consume(self):
        if not self.consumer or not self._running:
            return
        try:
            async for message in self.consumer:
                try:
                    await _process_emergency(message.value)
                except Exception as e:
                    logger.error(f"Error processing alert: {e}")
        except Exception as e:
            logger.error(f"Alert consumer error: {e}")


alert_consumer = AlertConsumer()


async def _process_emergency(data: dict):
    """Process an incoming emergency alert."""
    alert_id = str(uuid.uuid4())[:8]
    alert = {
        "alert_id": alert_id,
        "alert_type": data.get("alert_type", "unknown"),
        "intersection_id": data.get("intersection_id", "unknown"),
        "severity": data.get("severity", "high"),
        "description": data.get("description", ""),
        "source_node_id": data.get("source_node_id", "unknown"),
        "status": "active",
        "created_at": datetime.utcnow().isoformat(),
    }
    active_alerts[alert_id] = alert

    # Publish to Redis for dashboard
    try:
        r = aioredis.from_url(settings.redis_url)
        await r.publish(settings.redis_alert_channel, json.dumps(alert))

        # Set emergency flag on intersection
        key = f"intersection:{alert['intersection_id']}"
        await r.hset(key, "is_emergency_active", "True")

        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis publish failed: {e}")

    logger.warning(
        f"🚨 EMERGENCY ALERT: {alert['alert_type']} "
        f"at {alert['intersection_id']} (severity: {alert['severity']})"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("EdgeCloudX Alert Service — Starting")

    async def _start_consumer():
        await alert_consumer.start()
        await alert_consumer.consume()

    consumer_task = asyncio.create_task(_start_consumer())
    yield
    await alert_consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    logger.info("Alert Service stopped")


app = FastAPI(
    title="EdgeCloudX Alert Service",
    description="Emergency alert management and green corridor triggering",
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


@app.post("/alerts/emergency", response_model=AlertResponse)
async def create_emergency_alert(alert: EmergencyAlert):
    """Manually create an emergency alert."""
    alert_id = str(uuid.uuid4())[:8]
    entry = {
        "alert_id": alert_id,
        "alert_type": alert.alert_type,
        "intersection_id": alert.intersection_id,
        "severity": alert.severity,
        "description": alert.description,
        "status": "active",
        "created_at": datetime.utcnow().isoformat(),
    }
    active_alerts[alert_id] = entry

    # Publish to Redis
    try:
        r = aioredis.from_url(settings.redis_url)
        await r.publish(settings.redis_alert_channel, json.dumps(entry))
        key = f"intersection:{alert.intersection_id}"
        await r.hset(key, "is_emergency_active", "True")
        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis publish failed: {e}")

    logger.warning(f"🚨 Manual alert: {alert.alert_type} at {alert.intersection_id}")
    return AlertResponse(**entry)


@app.get("/alerts/active", response_model=list[AlertResponse])
async def get_active_alerts():
    """Get all active emergency alerts."""
    return [AlertResponse(**a) for a in active_alerts.values() if a["status"] == "active"]


@app.post("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    """Resolve an active emergency alert."""
    if alert_id not in active_alerts:
        raise HTTPException(status_code=404, detail="Alert not found")

    active_alerts[alert_id]["status"] = "resolved"

    # Clear emergency flag on intersection
    try:
        r = aioredis.from_url(settings.redis_url)
        key = f"intersection:{active_alerts[alert_id]['intersection_id']}"
        await r.hset(key, "is_emergency_active", "False")
        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis update failed: {e}")

    return {"alert_id": alert_id, "status": "resolved"}


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.1.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Alert Service", "version": "0.1.0", "docs": "/docs"}
