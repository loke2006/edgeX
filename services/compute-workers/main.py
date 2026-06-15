"""
EdgeCloudX Compute Workers — Main Application
================================================
Entry point that initializes Ray, starts Kafka consumers,
and dispatches events to distributed Ray workers.
Also runs a lightweight FastAPI health endpoint.
"""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager

import ray
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from consumers.kafka_consumer import compute_consumer
from workers.congestion import CongestionAnalyzer
from workers.emergency_corridor import compute_emergency_corridor
from workers.heatmap import generate_heatmap
from workers.predictor import TrafficPredictor
from workers.route_optimizer import optimize_routes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
settings = get_settings()

# ── Ray actors (initialized in lifespan) ──
congestion_analyzer = None
traffic_predictor = None


# ── Kafka message handlers ──


async def handle_traffic_events(messages: list[dict]) -> None:
    """Handle batch of traffic-density events."""
    global congestion_analyzer, traffic_predictor

    if congestion_analyzer is None:
        return

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
            f"Congestion updated for {len(updated)} intersections"
        )
    except Exception as e:
        logger.warning(f"Congestion batch processing error: {e}")


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
        })
        r.expire(key, 60)  # 60s TTL
    r.close()


async def handle_emergency_events(messages: list[dict]) -> None:
    """Handle emergency alert events — trigger corridor computation."""
    for msg in messages:
        try:
            result_ref = compute_emergency_corridor.remote(msg)
            corridor = await asyncio.get_event_loop().run_in_executor(
                None, lambda: ray.get(result_ref, timeout=10)
            )
            if corridor:
                logger.info(
                    f"Emergency corridor: {corridor['emergency_id']} "
                    f"({corridor['distance']} steps)"
                )
        except Exception as e:
            logger.error(f"Emergency corridor error: {e}")


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
                f"Heatmap generated: avg={result['avg_congestion']:.3f}"
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
                f"Routes optimized for {len(result)} EVs"
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
                    f"Predictions generated for "
                    f"{len(preds)} intersections"
                )
            except Exception as e:
                logger.warning(f"Prediction error: {e}")
        await asyncio.sleep(settings.prediction_interval_seconds)


# ── FastAPI application ──


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle — startup and shutdown."""
    global congestion_analyzer, traffic_predictor

    logger.info("=" * 60)
    logger.info("  EdgeCloudX Compute Workers — Starting")
    logger.info("=" * 60)

    # Initialize Ray
    try:
        ray.init(
            num_cpus=settings.ray_num_cpus,
            include_dashboard=False,
            logging_level=logging.WARNING,
        )
        logger.info(
            f"Ray initialized with {settings.ray_num_cpus} CPUs"
        )
    except Exception as e:
        logger.error(f"Ray initialization failed: {e}")
        raise

    # Create Ray actors
    congestion_analyzer = CongestionAnalyzer.remote()
    traffic_predictor = TrafficPredictor.remote()
    logger.info("Ray actors created: CongestionAnalyzer, TrafficPredictor")

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


@app.get("/health")
async def health():
    """Health check endpoint."""
    ray_alive = ray.is_initialized()
    return {
        "service": settings.service_name,
        "status": "healthy" if ray_alive else "degraded",
        "version": "0.1.0",
        "ray_initialized": ray_alive,
    }


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {
        "service": "EdgeCloudX Compute Workers",
        "version": "0.1.0",
        "docs": "/docs",
    }
