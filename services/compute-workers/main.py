"""
EdgeCloudX Compute Workers — Main Application
================================================
Entry point that initializes Ray, starts Kafka consumers,
and dispatches events to distributed Ray workers.

Enhanced with:
- Structured JSON logging with trace ID propagation
- Dead-letter queue for failed message processing
- Custom Prometheus metrics
- Security headers
"""

import asyncio
import os
import sys
import signal
import time

# Add project root (parent of shared/) to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from shared.logging import setup_logging  # noqa: E402

setup_logging("compute-workers")

import logging  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

import ray  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from shared.dlq import DeadLetterPublisher  # noqa: E402
from shared.metrics import (  # noqa: E402
    EVENTS_TOTAL,
    KAFKA_PROCESS_LATENCY,
    ACTIVE_EVS,
    SERVICE_INFO,
)
from shared.middleware import add_security_headers  # noqa: E402
from shared.trace import TraceContext  # noqa: E402

from config import get_settings  # noqa: E402
from consumers.kafka_consumer import compute_consumer  # noqa: E402
from workers.congestion import CongestionAnalyzer  # noqa: E402
from workers.emergency_corridor import compute_emergency_corridor  # noqa: E402
from workers.heatmap import generate_heatmap  # noqa: E402
from workers.predictor import TrafficPredictor  # noqa: E402
from workers.route_optimizer import optimize_routes  # noqa: E402

logger = logging.getLogger(__name__)
settings = get_settings()

# ── Ray actors (initialized in lifespan) ──
congestion_analyzer = None
traffic_predictor = None
dlq = None


# ── Kafka message handlers ──


async def handle_traffic_events(messages: list[dict]) -> None:
    """Handle batch of traffic-density events."""
    global congestion_analyzer, traffic_predictor

    if congestion_analyzer is None:
        return

    start = time.time()

    # Submit batch to congestion analyzer
    result_ref = congestion_analyzer.process_batch.remote(messages)

    # Record observations for prediction
    if traffic_predictor is not None:
        for msg in messages:
            iid = msg.get("intersection_id", "")
            score = float(msg.get("congestion_score", 0.0))
            traffic_predictor.record.remote(iid, score)

    # Await result (non-blocking via Ray)
    try:
        updated = await asyncio.get_event_loop().run_in_executor(
            None, lambda: ray.get(result_ref, timeout=5)
        )
        logger.debug(
            "Congestion batch processed",
            extra={
                "intersections": len(updated),
                "batch_size": len(messages),
                "trace_id": messages[0].get("trace_id", "") if messages else "",
            },
        )
        EVENTS_TOTAL.labels(
            service="compute-workers", topic="traffic-density", status="ok"
        ).inc(len(messages))
    except Exception as e:
        logger.warning(f"Congestion batch processing error: {e}")
        EVENTS_TOTAL.labels(
            service="compute-workers", topic="traffic-density", status="error"
        ).inc(len(messages))
        # Send failed messages to DLQ
        if dlq:
            for msg in messages:
                await dlq.send(
                    "traffic-density", msg, e,
                    service="compute-workers",
                    trace_id=msg.get("trace_id", ""),
                )

    KAFKA_PROCESS_LATENCY.labels(
        service="compute-workers", topic="traffic-density"
    ).observe(time.time() - start)


async def handle_ev_events(messages: list[dict]) -> None:
    """Handle batch of ev-telemetry events (store in Redis for optimizer)."""
    import redis as sync_redis

    r = sync_redis.from_url(settings.redis_url, decode_responses=True)
    for msg in messages:
        ev_id = msg.get("ev_id", "unknown")
        key = f"ev:{ev_id}"

        # Extract position (nested under 'position' key from edge node)
        position = msg.get("position", {})
        target = msg.get("target", {})

        r.hset(key, mapping={
            "ev_id": ev_id,
            "row": str(position.get("row", msg.get("row", 0))),
            "col": str(position.get("col", msg.get("col", 0))),
            "dest_row": str(target.get("row", msg.get("dest_row", settings.grid_rows - 1))),
            "dest_col": str(target.get("col", msg.get("dest_col", settings.grid_cols - 1))),
            "battery": str(msg.get("battery_level", msg.get("battery_percent", 100))),
            "speed": str(msg.get("speed_kmh", 0)),
            "status": msg.get("status", "moving"),
            "trace_id": msg.get("trace_id", ""),
        })
        r.expire(key, 60)  # 60s TTL

    ACTIVE_EVS.set(len(messages))
    EVENTS_TOTAL.labels(
        service="compute-workers", topic="ev-telemetry", status="ok"
    ).inc(len(messages))
    r.close()


async def handle_emergency_events(messages: list[dict]) -> None:
    """Handle emergency alert events — trigger corridor computation."""
    for msg in messages:
        trace_id = msg.get("trace_id", "")
        try:
            result_ref = compute_emergency_corridor.remote(msg)
            corridor = await asyncio.get_event_loop().run_in_executor(
                None, lambda: ray.get(result_ref, timeout=10)
            )
            if corridor:
                logger.info(
                    "Emergency corridor computed",
                    extra={
                        "trace_id": trace_id,
                        "emergency_id": corridor["emergency_id"],
                        "distance": corridor["distance"],
                    },
                )
            EVENTS_TOTAL.labels(
                service="compute-workers", topic="emergency-alerts", status="ok"
            ).inc()
        except Exception as e:
            logger.error(f"Emergency corridor error: {e}", extra={"trace_id": trace_id})
            EVENTS_TOTAL.labels(
                service="compute-workers", topic="emergency-alerts", status="error"
            ).inc()
            if dlq:
                await dlq.send(
                    "emergency-alerts", msg, e,
                    service="compute-workers",
                    trace_id=trace_id,
                )


# ── Periodic tasks ──


async def periodic_heatmap():
    """Periodically generate heatmap."""
    while True:
        try:
            ref = generate_heatmap.remote()
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: ray.get(ref, timeout=10)
            )
            logger.debug(
                "Heatmap generated",
                extra={"avg_congestion": round(result["avg_congestion"], 3)},
            )
        except Exception as e:
            logger.warning(f"Heatmap generation error: {e}")
        await asyncio.sleep(settings.heatmap_interval_seconds)


async def periodic_route_optimization():
    """Periodically optimize EV routes."""
    while True:
        try:
            ref = optimize_routes.remote()
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: ray.get(ref, timeout=10)
            )
            logger.debug(
                "Routes optimized",
                extra={"ev_count": len(result)},
            )
        except Exception as e:
            logger.warning(f"Route optimization error: {e}")
        await asyncio.sleep(settings.route_optimization_interval_seconds)


async def periodic_predictions():
    """Periodically generate traffic predictions."""
    global traffic_predictor
    while True:
        if traffic_predictor is not None:
            try:
                ref = traffic_predictor.predict.remote()
                preds = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: ray.get(ref, timeout=10)
                )
                logger.debug(
                    "Predictions generated",
                    extra={"intersections": len(preds)},
                )
            except Exception as e:
                logger.warning(f"Prediction error: {e}")
        await asyncio.sleep(settings.prediction_interval_seconds)


# ── FastAPI application ──


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle — startup and shutdown."""
    global congestion_analyzer, traffic_predictor, dlq

    logger.info("Compute Workers starting")

    # Initialize Ray
    try:
        ray.init(
            num_cpus=settings.ray_num_cpus,
            include_dashboard=False,
            logging_level=logging.WARNING,
        )
        logger.info("Ray initialized", extra={"cpus": settings.ray_num_cpus})
    except Exception as e:
        logger.error(f"Ray initialization failed: {e}")
        raise

    # Create Ray actors
    congestion_analyzer = CongestionAnalyzer.remote()
    traffic_predictor = TrafficPredictor.remote()
    logger.info("Ray actors created: CongestionAnalyzer, TrafficPredictor")

    # Initialize DLQ
    dlq = DeadLetterPublisher(settings.kafka_bootstrap_servers)
    await dlq.start()

    # Prometheus service info
    SERVICE_INFO.info({"service": "compute-workers", "version": "0.2.0"})

    # Register Kafka handlers
    compute_consumer.register_handler(
        settings.kafka_traffic_topic, handle_traffic_events
    )
    compute_consumer.register_handler(
        settings.kafka_ev_topic, handle_ev_events
    )
    compute_consumer.register_handler(
        settings.kafka_emergency_topic, handle_emergency_events
    )

    # Start Kafka consumer
    async def _start_consumer():
        await compute_consumer.start()
        await compute_consumer.consume()

    consumer_task = asyncio.create_task(_start_consumer())
    logger.info("Kafka consumer task launched")

    # Start periodic workers
    heatmap_task = asyncio.create_task(periodic_heatmap())
    route_task = asyncio.create_task(periodic_route_optimization())
    predict_task = asyncio.create_task(periodic_predictions())
    logger.info("Periodic workers launched: heatmap, routes, predictions")

    yield

    # Shutdown
    logger.info("Shutting down Compute Workers...")
    await compute_consumer.stop()
    await dlq.stop()

    for task in [consumer_task, heatmap_task, route_task, predict_task]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Shutdown Ray actors
    try:
        ray.get(congestion_analyzer.shutdown.remote())
        ray.get(traffic_predictor.shutdown.remote())
    except Exception:
        pass

    ray.shutdown()
    logger.info("Compute Workers stopped")


app = FastAPI(
    title="EdgeCloudX Compute Workers",
    description="Ray-based distributed compute workers for traffic analysis",
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

# Security headers
add_security_headers(app)


@app.get("/health")
async def health():
    """Health check endpoint."""
    ray_alive = ray.is_initialized()
    return {
        "service": settings.service_name,
        "status": "healthy" if ray_alive else "degraded",
        "version": "0.2.0",
        "ray_initialized": ray_alive,
    }


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {
        "service": "EdgeCloudX Compute Workers",
        "version": "0.2.0",
        "docs": "/docs",
    }
