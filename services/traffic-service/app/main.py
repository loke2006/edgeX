"""
EdgeCloudX Traffic Service — Main Application
================================================
FastAPI microservice for traffic data ingestion, processing, and querying.
Consumes traffic-density events from Kafka, stores to PostgreSQL,
and publishes real-time updates to Redis Pub/Sub.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import get_settings
from app.consumers.kafka_consumer import traffic_consumer
from app.models.database import init_db
from app.routers import health, traffic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle — startup and shutdown hooks."""
    logger.info("=" * 60)
    logger.info("  EdgeCloudX Traffic Service — Starting")
    logger.info("=" * 60)

    # Initialize database tables
    await init_db()
    logger.info("Database initialized")

    # Start Kafka consumer in background (non-blocking)
    async def _start_consumer():
        await traffic_consumer.start()
        await traffic_consumer.consume()

    consumer_task = asyncio.create_task(_start_consumer())
    logger.info("Kafka consumer task launched (non-blocking)")

    yield

    # Shutdown
    logger.info("Shutting down Traffic Service...")
    await traffic_consumer.stop()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    logger.info("Traffic Service stopped")


# Create FastAPI application
app = FastAPI(
    title="EdgeCloudX Traffic Service",
    description="Traffic data ingestion, processing, and real-time grid management",
    version="0.1.0",
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

# Prometheus metrics
Instrumentator().instrument(app).expose(app, include_in_schema=False)

# Register routers
app.include_router(health.router)
app.include_router(traffic.router)


@app.get("/")
async def root():
    return {
        "service": "EdgeCloudX Traffic Service",
        "version": "0.1.0",
        "docs": "/docs",
    }
