"""
EdgeCloudX Edge Node — Async Kafka Producer
=============================================
High-throughput async Kafka producer for edge node events.
Publishes traffic density, emergency alerts, anomalies, and node health.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from aiokafka import AIOKafkaProducer
from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class EdgeKafkaProducer:
    """Async Kafka producer for edge node telemetry."""

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self._connected = False

    async def start(self) -> bool:
        """Connect to Kafka with retry logic."""
        for attempt in range(15):
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    compression_type="gzip",
                    linger_ms=10,
                )
                await self.producer.start()
                self._connected = True
                logger.info(f"Kafka producer connected: {settings.kafka_bootstrap_servers}")
                return True
            except Exception as e:
                wait = min(2 ** attempt, 30)
                logger.warning(
                    f"Kafka connection attempt {attempt + 1}/15 failed: {e}. "
                    f"Retrying in {wait}s..."
                )
                await asyncio.sleep(wait)

        logger.error("Failed to connect to Kafka after 15 attempts")
        return False

    async def stop(self):
        """Disconnect from Kafka."""
        if self.producer:
            await self.producer.stop()
            self._connected = False
            logger.info("Kafka producer disconnected")

    async def send_traffic_event(self, event: dict) -> None:
        """Send traffic density event."""
        await self._send(settings.traffic_topic, {
            **event,
            "edge_node_id": settings.edge_node_id,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def send_ev_telemetry(self, telemetry: dict) -> None:
        """Send EV telemetry data."""
        await self._send(settings.ev_topic, {
            **telemetry,
            "edge_node_id": settings.edge_node_id,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def send_emergency_alert(self, alert: dict) -> None:
        """Send emergency alert."""
        await self._send(settings.emergency_topic, {
            **alert,
            "source_node_id": settings.edge_node_id,
            "timestamp": datetime.utcnow().isoformat(),
        })
        logger.warning(f"🚨 Emergency alert sent: {alert.get('alert_type', 'unknown')}")

    async def send_anomaly_event(self, anomaly: dict) -> None:
        """Send anomaly detection event."""
        await self._send(settings.anomaly_topic, {
            **anomaly,
            "edge_node_id": settings.edge_node_id,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def send_health(self, health: dict) -> None:
        """Send node health status."""
        await self._send(settings.health_topic, {
            **health,
            "edge_node_id": settings.edge_node_id,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def _send(self, topic: str, data: dict) -> None:
        """Internal send with error handling."""
        if not self._connected or not self.producer:
            logger.warning(f"Producer not connected, dropping message to {topic}")
            return

        try:
            await self.producer.send_and_wait(topic, data)
        except Exception as e:
            logger.error(f"Failed to send to {topic}: {e}")
