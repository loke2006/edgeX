"""
EdgeCloudX Traffic Service — Kafka Consumer
Consumes traffic-density events from edge nodes and processes them.
"""

import asyncio
import json
import logging
from datetime import datetime

from aiokafka import AIOKafkaConsumer

from app.config import get_settings
from app.models.database import async_session
from app.schemas.traffic import TrafficUpdateSchema
from app.services.traffic_service import TrafficService

logger = logging.getLogger(__name__)
settings = get_settings()


class TrafficKafkaConsumer:
    """Async Kafka consumer for traffic-density topic."""

    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self) -> None:
        """Start the Kafka consumer."""
        self.consumer = AIOKafkaConsumer(
            settings.kafka_traffic_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            retry_backoff_ms=1000,
            session_timeout_ms=30000,
        )

        # Retry connection with backoff
        for attempt in range(10):
            try:
                await self.consumer.start()
                self._running = True
                logger.info(
                    f"Kafka consumer started — subscribed to '{settings.kafka_traffic_topic}'"
                )
                return
            except Exception as e:
                wait = min(2 ** attempt, 30)
                logger.warning(
                    f"Kafka connection attempt {attempt + 1}/10 failed: {e}. "
                    f"Retrying in {wait}s..."
                )
                await asyncio.sleep(wait)

        logger.error("Failed to connect to Kafka after 10 attempts")

    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        self._running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")

    async def consume(self) -> None:
        """Main consume loop — processes messages from Kafka."""
        if not self.consumer or not self._running:
            logger.warning("Consumer not started, skipping consume loop")
            return

        try:
            async for message in self.consumer:
                try:
                    await self._process_message(message.value)
                except Exception as e:
                    logger.error(
                        f"Error processing message: {e}",
                        exc_info=True,
                    )
        except Exception as e:
            logger.error(f"Consumer loop error: {e}", exc_info=True)

    async def _process_message(self, data: dict) -> None:
        """Process a single traffic-density message."""
        try:
            update = TrafficUpdateSchema(
                intersection_id=data.get("intersection_id", "unknown"),
                edge_node_id=data.get("edge_node_id", "unknown"),
                vehicle_count=data.get("vehicle_count", 0),
                congestion_score=data.get("congestion_score", 0.0),
                anomaly_detected=data.get("anomaly_detected", False),
                anomaly_type=data.get("anomaly_type"),
                timestamp=datetime.fromisoformat(
                    data.get("timestamp", datetime.utcnow().isoformat())
                ),
            )

            async with async_session() as session:
                service = TrafficService(session)
                await service.process_traffic_update(update)
                await session.commit()

        except Exception as e:
            logger.error(f"Failed to process traffic message: {e}", exc_info=True)


# Singleton consumer instance
traffic_consumer = TrafficKafkaConsumer()
