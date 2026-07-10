"""
EdgeCloudX Edge Node — Main Entry Point
==========================================
Orchestrates the edge node simulation:
1. Generates synthetic traffic data (or runs YOLOv8 on video frames)
2. Publishes traffic density events to Kafka with trace IDs
3. Simulates EV telemetry and publishes to Kafka
4. Detects emergencies and publishes alerts
5. Sends periodic health heartbeats with system telemetry
"""

import asyncio
import os
import random
import sys
import time

# Add project root (parent of shared/) to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))

from shared.logging import setup_logging  # noqa: E402

setup_logging("edge-node")

import logging  # noqa: E402

from config import get_settings  # noqa: E402
from detector.frame_generator import TrafficSimulator  # noqa: E402
from detector.yolo_detector import YOLODetector  # noqa: E402
from producer.kafka_producer import EdgeKafkaProducer  # noqa: E402
from shared.trace import TraceContext, new_trace_id  # noqa: E402
from telemetry.ev_simulator import EVFleetSimulator  # noqa: E402

logger = logging.getLogger("edge-node")
settings = get_settings()


async def run_edge_node():
    """Main edge node execution loop."""

    logger.info(
        "Edge node starting",
        extra={
            "node_id": settings.edge_node_id,
            "grid": f"{settings.grid_rows}x{settings.grid_cols}",
            "density": settings.vehicle_density,
            "yolo": settings.enable_yolo,
            "ev_count": settings.ev_count,
            "interval_ms": settings.event_interval_ms,
        },
    )

    # Initialize components
    producer = EdgeKafkaProducer()
    simulator = TrafficSimulator(
        grid_rows=settings.grid_rows,
        grid_cols=settings.grid_cols,
        density=settings.vehicle_density,
    )
    ev_fleet = EVFleetSimulator(
        ev_count=settings.ev_count,
        grid_rows=settings.grid_rows,
        grid_cols=settings.grid_cols,
    )

    detector = None
    if settings.enable_yolo:
        detector = YOLODetector()
        if not detector.is_loaded:
            logger.warning("YOLOv8 failed to load, falling back to simulation")
            detector = None

    # Connect to Kafka
    connected = await producer.start()
    if not connected:
        logger.error("Cannot start edge node without Kafka connection")
        return

    tick_count = 0
    interval = settings.event_interval_ms / 1000.0
    start_time = time.time()

    try:
        while True:
            tick_count += 1
            tick_start = time.time()

            # Generate a shared trace_id for this tick
            tick_trace_id = new_trace_id()

            # --- Traffic Events ---
            traffic_events = simulator.tick_simulation()

            for event in traffic_events:
                # Create trace context per event
                ctx = TraceContext.new("edge-node", trace_id=tick_trace_id)

                # If YOLO is enabled, generate frame and run detection
                if detector:
                    frame = simulator.generate_frame()
                    detection = detector.detect(frame)
                    event["vehicle_count"] = detection["vehicle_count"]
                    event["anomaly_detected"] = detection["anomaly_detected"]
                    event["anomaly_type"] = detection["anomaly_type"]

                # Inject trace context
                event.update(ctx.as_dict())

                # Send traffic event
                await producer.send_traffic_event(event)

                # Send anomaly if detected
                if event.get("anomaly_detected"):
                    anomaly_ctx = TraceContext.child(ctx, "edge-node")
                    await producer.send_anomaly_event({
                        "intersection_id": event["intersection_id"],
                        "anomaly_type": event["anomaly_type"],
                        "vehicle_count": event["vehicle_count"],
                        "congestion_score": event["congestion_score"],
                        **anomaly_ctx.as_dict(),
                    })

            # --- EV Telemetry ---
            ev_data = ev_fleet.tick()
            for telemetry in ev_data:
                ev_ctx = TraceContext.new("edge-node", trace_id=tick_trace_id)
                telemetry.update(ev_ctx.as_dict())
                await producer.send_ev_telemetry(telemetry)

            # --- Emergency Events (probabilistic) ---
            if random.random() < settings.emergency_probability:
                emergency_types = ["ambulance", "fire_truck", "accident"]
                rand_row = random.randint(0, settings.grid_rows - 1)
                rand_col = random.randint(0, settings.grid_cols - 1)
                random_intersection = f"int-{rand_row}-{rand_col}"
                em_ctx = TraceContext.new("edge-node", trace_id=tick_trace_id)
                await producer.send_emergency_alert({
                    "alert_type": random.choice(emergency_types),
                    "intersection_id": random_intersection,
                    "severity": random.choice(["medium", "high", "critical"]),
                    "description": f"Emergency detected at {random_intersection}",
                    **em_ctx.as_dict(),
                })

            # --- Health Heartbeat (every 5 ticks) ---
            if tick_count % 5 == 0:
                elapsed = time.time() - tick_start
                uptime = time.time() - start_time

                # System telemetry
                try:
                    import psutil
                    cpu_percent = psutil.cpu_percent(interval=None)
                    memory = psutil.virtual_memory()
                    memory_percent = memory.percent
                except ImportError:
                    cpu_percent = -1.0
                    memory_percent = -1.0

                await producer.send_health({
                    "node_id": settings.edge_node_id,
                    "status": "healthy",
                    "tick_count": tick_count,
                    "events_per_tick": len(traffic_events),
                    "evs_tracked": len(ev_data),
                    "tick_duration_ms": round(elapsed * 1000, 2),
                    "yolo_enabled": detector is not None,
                    "cpu_percent": round(cpu_percent, 1),
                    "memory_percent": round(memory_percent, 1),
                    "fps": round(1.0 / max(elapsed, 0.001), 1),
                    "uptime_seconds": round(uptime, 1),
                })

            # --- Logging ---
            if tick_count % 5 == 0:
                total_vehicles = sum(
                    e["vehicle_count"] for e in traffic_events
                )
                avg_congestion = (
                    sum(e["congestion_score"] for e in traffic_events)
                    / len(traffic_events)
                )
                logger.info(
                    "Tick summary",
                    extra={
                        "trace_id": tick_trace_id,
                        "tick": tick_count,
                        "vehicles": total_vehicles,
                        "avg_congestion": round(avg_congestion, 2),
                        "evs": len(ev_data),
                        "anomalies": sum(1 for e in traffic_events if e.get("anomaly_detected")),
                    },
                )

            # Wait for next tick
            elapsed = time.time() - tick_start
            sleep_time = max(0, interval - elapsed)
            await asyncio.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Edge node shutting down (keyboard interrupt)")
    except Exception as e:
        logger.error(f"Edge node error: {e}", exc_info=True)
    finally:
        await producer.stop()
        logger.info("Edge node stopped")


if __name__ == "__main__":
    asyncio.run(run_edge_node())
