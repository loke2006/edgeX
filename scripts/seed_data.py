"""
EdgeCloudX — Seed Data Script
================================
Seeds the database with initial intersection grid data.
Can also generate sample traffic events for testing.
"""

import asyncio
import json
import random
from datetime import datetime, timedelta

from aiokafka import AIOKafkaProducer


KAFKA_BOOTSTRAP = "localhost:9094"  # External port


async def seed_traffic_events(count: int = 100):
    """Send sample traffic events to Kafka for testing."""

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    print(f"Sending {count} sample traffic events...")

    for i in range(count):
        row = random.randint(0, 3)
        col = random.randint(0, 3)

        event = {
            "intersection_id": f"int-{row}-{col}",
            "edge_node_id": "seed-script",
            "vehicle_count": random.randint(0, 30),
            "congestion_score": round(random.uniform(0, 1), 3),
            "anomaly_detected": random.random() < 0.05,
            "anomaly_type": random.choice(["accident", "breakdown", None]),
            "timestamp": datetime.utcnow().isoformat(),
        }

        await producer.send_and_wait("traffic-density", event)

        if (i + 1) % 10 == 0:
            print(f"  Sent {i + 1}/{count} events")

        await asyncio.sleep(0.05)

    await producer.stop()
    print(f"✅ Done! Sent {count} traffic events to Kafka")


async def seed_emergency_alerts(count: int = 5):
    """Send sample emergency alerts."""

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    print(f"Sending {count} emergency alerts...")

    emergency_types = ["ambulance", "fire_truck", "accident"]

    for i in range(count):
        alert = {
            "alert_type": random.choice(emergency_types),
            "intersection_id": f"int-{random.randint(0, 3)}-{random.randint(0, 3)}",
            "severity": random.choice(["medium", "high", "critical"]),
            "description": f"Test emergency alert #{i + 1}",
            "source_node_id": "seed-script",
            "timestamp": datetime.utcnow().isoformat(),
        }
        await producer.send_and_wait("emergency-alerts", alert)
        print(f"  🚨 Alert: {alert['alert_type']} at {alert['intersection_id']}")
        await asyncio.sleep(0.5)

    await producer.stop()
    print(f"✅ Done! Sent {count} emergency alerts")


async def main():
    print("=" * 50)
    print("  EdgeCloudX — Data Seeder")
    print("=" * 50)
    print()

    await seed_traffic_events(100)
    print()
    await seed_emergency_alerts(5)


if __name__ == "__main__":
    asyncio.run(main())
