"""
EdgeCloudX Edge Node — Main Entry Point
==========================================
Orchestrates the edge node simulation:
1. Generates synthetic traffic data (or runs YOLOv8 on video frames)
2. Publishes traffic density events to Kafka
3. Simulates EV telemetry and publishes to Kafka
4. Detects emergencies and publishes alerts
5. Sends periodic health heartbeats
"""

import asyncio
import logging
import random
import time

from config import get_settings
from detector.frame_generator import TrafficSimulator
from detector.yolo_detector import YOLODetector
from producer.kafka_producer import EdgeKafkaProducer
from telemetry.ev_simulator import EVFleetSimulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("edge-node")
settings = get_settings()


async def run_edge_node():
    """Main edge node execution loop."""

    logger.info("=" * 60)
    logger.info(f"  EdgeCloudX Edge Node [{settings.edge_node_id}]")
    logger.info(f"  Grid: {settings.grid_rows}x{settings.grid_cols}")
    logger.info(f"  Density: {settings.vehicle_density}")
    logger.info(f"  YOLO: {'enabled' if settings.enable_yolo else 'simulation mode'}")
    logger.info(f"  EVs: {settings.ev_count}")
    logger.info(f"  Event interval: {settings.event_interval_ms}ms")
    logger.info("=" * 60)

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

    try:
        while True:
            tick_count += 1
            tick_start = time.time()

            # --- Traffic Events ---
            traffic_events = simulator.tick_simulation()

            for event in traffic_events:
                # If YOLO is enabled, generate frame and run detection
                if detector:
                    frame = simulator.generate_frame()
                    detection = detector.detect(frame)
                    event["vehicle_count"] = detection["vehicle_count"]
                    event["anomaly_detected"] = detection["anomaly_detected"]
                    event["anomaly_type"] = detection["anomaly_type"]

                # Send traffic event
                await producer.send_traffic_event(event)

                # Send anomaly if detected
                if event.get("anomaly_detected"):
                    await producer.send_anomaly_event({
                        "intersection_id": event["intersection_id"],
                        "anomaly_type": event["anomaly_type"],
                        "vehicle_count": event["vehicle_count"],
                        "congestion_score": event["congestion_score"],
                    })

            # --- EV Telemetry ---
            ev_data = ev_fleet.tick()
            for telemetry in ev_data:
                await producer.send_ev_telemetry(telemetry)

            # --- Emergency Events (probabilistic) ---
            if random.random() < settings.emergency_probability:
                emergency_types = ["ambulance", "fire_truck", "accident"]
                rand_row = random.randint(0, settings.grid_rows - 1)
                rand_col = random.randint(0, settings.grid_cols - 1)
                random_intersection = f"int-{rand_row}-{rand_col}"
                await producer.send_emergency_alert({
                    "alert_type": random.choice(emergency_types),
                    "intersection_id": random_intersection,
                    "severity": random.choice(["medium", "high", "critical"]),
                    "description": f"Emergency detected at {random_intersection}",
                })

            # --- Health Heartbeat (every 10 ticks) ---
            if tick_count % 10 == 0:
                elapsed = time.time() - tick_start
                await producer.send_health({
                    "status": "healthy",
                    "tick_count": tick_count,
                    "events_per_tick": len(traffic_events),
                    "evs_tracked": len(ev_data),
                    "tick_duration_ms": round(elapsed * 1000, 2),
                    "yolo_enabled": detector is not None,
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
                    f"Tick {tick_count}: "
                    f"vehicles={total_vehicles} "
                    f"avg_congestion={avg_congestion:.2f} "
                    f"EVs={len(ev_data)} "
                    f"anomalies={sum(1 for e in traffic_events if e.get('anomaly_detected'))}"
                )

            # Wait for next tick
            elapsed = time.time() - tick_start
            sleep_time = max(0, interval - elapsed)
            await asyncio.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Edge node shutting down (keyboard interrupt)...")
    except Exception as e:
        logger.error(f"Edge node error: {e}", exc_info=True)
    finally:
        await producer.stop()
        logger.info("Edge node stopped")


if __name__ == "__main__":
    asyncio.run(run_edge_node())
