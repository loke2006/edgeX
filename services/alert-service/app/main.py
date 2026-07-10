"""
EdgeCloudX Alert Service — Main Application
=============================================
FastAPI microservice for emergency alert management.
Consumes emergency-alerts from Kafka, triggers green corridors,
and notifies the dashboard via Redis Pub/Sub.

Enhanced with:
- Structured JSON logging with trace ID
- Dead-letter queue for failed alert processing
- RBAC-protected endpoints
- Audit logging for emergency events
- Custom Prometheus metrics
- Security headers
"""

import asyncio
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import lru_cache
from typing import Optional

# Add shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from shared.logging import setup_logging  # noqa: E402

setup_logging("alert-service")

import logging  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402
from aiokafka import AIOKafkaConsumer  # noqa: E402
from fastapi import Depends, FastAPI, HTTPException  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from prometheus_fastapi_instrumentator import Instrumentator  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402
from pydantic_settings import BaseSettings  # noqa: E402
from shared.audit import AuditLogger  # noqa: E402
from shared.dlq import DeadLetterPublisher, retry_with_dlq  # noqa: E402
from shared.metrics import (  # noqa: E402
    ACTIVE_EMERGENCIES,
    EVENTS_TOTAL,
    SERVICE_INFO,
)
from shared.middleware import add_security_headers, require_role  # noqa: E402

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    service_name: str = "alert-service"
    debug: bool = False
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_emergency_topic: str = "emergency-alerts"
    kafka_consumer_group: str = "alert-service-group"
    redis_url: str = "redis://redis:6379/0"
    redis_alert_channel: str = "alerts:emergency"
    database_url: str = "postgresql+asyncpg://edgecloudx:edgecloudx_secret@postgres:5432/edgecloudx"

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
    trace_id: Optional[str] = None


class AlertConsumer:
    """Kafka consumer for emergency-alerts topic."""

    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
        self.dlq: Optional[DeadLetterPublisher] = None
        self.audit: Optional[AuditLogger] = None

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
                    "Alert Kafka consumer started",
                    extra={"topic": settings.kafka_emergency_topic},
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

        while self._running:
            try:
                async for message in self.consumer:
                    if not self._running:
                        break
                    try:
                        if self.dlq:
                            await retry_with_dlq(
                                _process_emergency,
                                message.value,
                                dlq=self.dlq,
                                topic=settings.kafka_emergency_topic,
                                service="alert-service",
                                max_retries=3,
                            )
                        else:
                            await _process_emergency(message.value)

                        EVENTS_TOTAL.labels(
                            service="alert-service",
                            topic="emergency-alerts",
                            status="ok",
                        ).inc()
                    except Exception as e:
                        logger.error(f"Error processing alert: {e}", exc_info=True)
                        EVENTS_TOTAL.labels(
                            service="alert-service",
                            topic="emergency-alerts",
                            status="error",
                        ).inc()
            except Exception as e:
                logger.error(f"Alert consumer loop error: {e}. Restarting in 5s...", exc_info=True)
                await asyncio.sleep(5)


alert_consumer = AlertConsumer()


async def _process_emergency(data: dict):
    """Process an incoming emergency alert."""
    alert_id = str(uuid.uuid4())[:8]
    trace_id = data.get("trace_id", "")

    alert = {
        "alert_id": alert_id,
        "alert_type": data.get("alert_type", "unknown"),
        "intersection_id": data.get("intersection_id", "unknown"),
        "severity": data.get("severity", "high"),
        "description": data.get("description", ""),
        "source_node_id": data.get("source_node_id", "unknown"),
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "trace_id": trace_id,
    }
    active_alerts[alert_id] = alert
    ACTIVE_EMERGENCIES.set(
        sum(1 for a in active_alerts.values() if a["status"] == "active")
    )

    # Publish to Redis for dashboard
    try:
        r = aioredis.from_url(settings.redis_url)
        await r.publish(settings.redis_alert_channel, json.dumps(alert))

        # Set emergency flag on intersection
        key = f"intersection:{alert['intersection_id']}"
        await r.hset(key, "is_emergency_active", "True")

        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis publish failed: {e}", extra={"trace_id": trace_id})

    # Audit log
    if alert_consumer.audit:
        await alert_consumer.audit.log(
            "emergency_activated",
            actor=data.get("source_node_id", "system"),
            resource=alert["intersection_id"],
            details={"alert_id": alert_id, "type": alert["alert_type"], "severity": alert["severity"]},
            trace_id=trace_id,
            service="alert-service",
        )

    logger.warning(
        "EMERGENCY ALERT",
        extra={
            "trace_id": trace_id,
            "alert_id": alert_id,
            "type": alert["alert_type"],
            "intersection": alert["intersection_id"],
            "severity": alert["severity"],
        },
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Alert Service starting")

    # Initialize audit logger
    audit = AuditLogger(settings.database_url)
    await audit.init()
    app.state.audit = audit
    alert_consumer.audit = audit

    # Initialize DLQ
    dlq_pub = DeadLetterPublisher(settings.kafka_bootstrap_servers)
    await dlq_pub.start()
    alert_consumer.dlq = dlq_pub

    # Store auth service URL for RBAC
    app.state.auth_service_url = os.environ.get(
        "AUTH_SERVICE_URL", "http://auth-service:8000"
    )

    # Prometheus service info
    SERVICE_INFO.info({"service": "alert-service", "version": "0.2.0"})

    async def _start_consumer():
        await alert_consumer.start()
        await alert_consumer.consume()

    consumer_task = asyncio.create_task(_start_consumer())
    yield

    await alert_consumer.stop()
    await dlq_pub.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    logger.info("Alert Service stopped")


app = FastAPI(
    title="EdgeCloudX Alert Service",
    description="Emergency alert management and green corridor triggering",
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


@app.post("/alerts/emergency", response_model=AlertResponse)
async def create_emergency_alert(
    alert: EmergencyAlert,
    user: dict = Depends(require_role("operator", "admin")),
):
    """Manually create an emergency alert (requires operator or admin role)."""
    alert_id = str(uuid.uuid4())[:8]
    entry = {
        "alert_id": alert_id,
        "alert_type": alert.alert_type,
        "intersection_id": alert.intersection_id,
        "severity": alert.severity,
        "description": alert.description,
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "trace_id": None,
    }
    active_alerts[alert_id] = entry
    ACTIVE_EMERGENCIES.set(
        sum(1 for a in active_alerts.values() if a["status"] == "active")
    )

    # Publish to Redis
    try:
        r = aioredis.from_url(settings.redis_url)
        await r.publish(settings.redis_alert_channel, json.dumps(entry))
        key = f"intersection:{alert.intersection_id}"
        await r.hset(key, "is_emergency_active", "True")
        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis publish failed: {e}")

    # Audit
    if hasattr(app.state, "audit"):
        await app.state.audit.log(
            "emergency_activated",
            actor=user.get("username", "unknown"),
            resource=alert.intersection_id,
            details={"alert_id": alert_id, "type": alert.alert_type, "manual": True},
            service="alert-service",
        )

    logger.warning(
        "Manual alert created",
        extra={"alert_id": alert_id, "type": alert.alert_type, "intersection": alert.intersection_id},
    )
    return AlertResponse(**entry)


@app.get("/alerts/active", response_model=list[AlertResponse])
async def get_active_alerts(
    user: dict = Depends(require_role("viewer", "operator", "admin")),
):
    """Get all active emergency alerts (requires viewer or above)."""
    return [AlertResponse(**a) for a in active_alerts.values() if a["status"] == "active"]


@app.post("/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: str,
    user: dict = Depends(require_role("operator", "admin")),
):
    """Resolve an active emergency alert (requires operator or admin)."""
    if alert_id not in active_alerts:
        raise HTTPException(status_code=404, detail="Alert not found")

    active_alerts[alert_id]["status"] = "resolved"
    ACTIVE_EMERGENCIES.set(
        sum(1 for a in active_alerts.values() if a["status"] == "active")
    )

    # Clear emergency flag on intersection
    try:
        r = aioredis.from_url(settings.redis_url)
        key = f"intersection:{active_alerts[alert_id]['intersection_id']}"
        await r.hset(key, "is_emergency_active", "False")
        await r.aclose()
    except Exception as e:
        logger.warning(f"Redis update failed: {e}")

    # Audit
    if hasattr(app.state, "audit"):
        await app.state.audit.log(
            "emergency_resolved",
            actor=user.get("username", "unknown"),
            resource=active_alerts[alert_id]["intersection_id"],
            details={"alert_id": alert_id},
            service="alert-service",
        )

    return {"alert_id": alert_id, "status": "resolved"}


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.2.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Alert Service", "version": "0.2.0", "docs": "/docs"}
