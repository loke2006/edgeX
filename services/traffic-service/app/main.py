"""
EdgeCloudX Traffic Service — Main Application
================================================
FastAPI microservice for traffic data ingestion, processing, and querying.
Consumes traffic-density events from Kafka, stores to PostgreSQL,
and publishes real-time updates to Redis Pub/Sub.

Enhanced with:
- Structured JSON logging
- Prometheus custom metrics
- Node health monitoring
- Adaptive signal controller
- Dead-letter queue for failed messages
- RBAC + security headers
- Audit logging
"""

import asyncio
import os
import sys

# Add project root (parent of shared/) to path so `from shared.xxx` works
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from shared.logging import setup_logging  # noqa: E402

setup_logging("traffic-service")

import logging  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from prometheus_fastapi_instrumentator import Instrumentator  # noqa: E402
from shared.audit import AuditLogger  # noqa: E402
from shared.dlq import DeadLetterPublisher  # noqa: E402
from shared.metrics import SERVICE_INFO  # noqa: E402
from shared.middleware import add_security_headers  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.consumers.kafka_consumer import traffic_consumer  # noqa: E402
from app.models.database import init_db  # noqa: E402
from app.routers import health, traffic  # noqa: E402
from app.services.node_monitor import NodeMonitor  # noqa: E402
from app.services.signal_controller import AdaptiveSignalController  # noqa: E402

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle — startup and shutdown hooks."""
    logger.info("Traffic Service starting")

    # Initialize database tables
    await init_db()
    logger.info("Database initialized")

    # Initialize audit logger
    audit = AuditLogger(settings.database_url)
    await audit.init()
    app.state.audit = audit

    # Initialize DLQ publisher
    dlq = DeadLetterPublisher(settings.kafka_bootstrap_servers)
    await dlq.start()
    app.state.dlq = dlq
    traffic_consumer.dlq = dlq

    # Store auth service URL for RBAC middleware
    app.state.auth_service_url = os.environ.get(
        "AUTH_SERVICE_URL", "http://auth-service:8000"
    )

    # Prometheus service info
    SERVICE_INFO.info({
        "service": "traffic-service",
        "version": "0.2.0",
    })

    # Start Kafka consumer in background
    async def _start_consumer():
        await traffic_consumer.start()
        await traffic_consumer.consume()

    consumer_task = asyncio.create_task(_start_consumer())
    logger.info("Kafka consumer task launched")

    # Start node health monitor
    node_monitor = NodeMonitor(
        kafka_servers=settings.kafka_bootstrap_servers,
        redis_url=settings.redis_url,
    )
    monitor_task = asyncio.create_task(node_monitor.run())
    logger.info("Node health monitor launched")

    # Start adaptive signal controller
    signal_ctrl = AdaptiveSignalController(
        redis_url=settings.redis_url,
        db_url=settings.database_url,
        audit=audit,
    )
    signal_task = asyncio.create_task(signal_ctrl.run())
    logger.info("Adaptive signal controller launched")

    yield

    # Shutdown
    logger.info("Shutting down Traffic Service...")
    await traffic_consumer.stop()
    await node_monitor.stop()
    await signal_ctrl.stop()
    await dlq.stop()

    for task in [consumer_task, monitor_task, signal_task]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    logger.info("Traffic Service stopped")


# Create FastAPI application
app = FastAPI(
    title="EdgeCloudX Traffic Service",
    description="Traffic data ingestion, processing, and real-time grid management",
    version="0.2.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security headers
add_security_headers(app)

# Prometheus metrics
Instrumentator().instrument(app).expose(app, include_in_schema=False)

# Register routers
app.include_router(health.router)
app.include_router(traffic.router)


@app.get("/")
async def root():
    return {
        "service": "EdgeCloudX Traffic Service",
        "version": "0.2.0",
        "docs": "/docs",
    }
