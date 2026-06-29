"""
EdgeCloudX Traffic Service — Kafka Consumer
==============================================
Consumes traffic-density events from edge nodes and processes them.

Enhanced with:
- Trace ID extraction and propagation
- Dead-letter queue for failed messages
- Custom Prometheus metrics
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional

from aiokafka import AIOKafkaConsumer

from app.config import get_settings
from app.models.database import async_session
from app.schemas.traffic import TrafficUpdateSchema
from app.services.traffic_service import TrafficService
from shared.dlq import DeadLetterPublisher, retry_with_dlq
from shared.metrics import EVENTS_TOTAL, KAFKA_PROCESS_LATENCY

logger = logging.getLogger(__name__)
settings = get_settings()


class TrafficKafkaConsumer:
    """Async Kafka consumer for traffic-density topic."""

    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self.dlq: Optional[DeadLetterPublisher] = None

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
        """Main consume loop — processes messages from Kafka with auto-restart."""
        if not self.consumer or not self._running:
            logger.warning("Consumer not started, skipping consume loop")
            return

        while self._running:
            try:
                await self._consume_inner()
            except Exception as e:
                logger.error(f"Consumer loop crashed: {e}. Restarting in 5s...", exc_info=True)
                await asyncio.sleep(5)

    async def _consume_inner(self) -> None:
        """Inner consume loop."""
        async for message in self.consumer:
            if not self._running:
                break

            start = time.time()

            try:
                if self.dlq:
                    success = await retry_with_dlq(
                        self._process_message,
                        message.value,
                        dlq=self.dlq,
                        topic=settings.kafka_traffic_topic,
                        service="traffic-service",
                        max_retries=3,
                    )
                    status = "ok" if success else "dlq"
                else:
                    await self._process_message(message.value)
                    status = "ok"

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                status = "error"

            # Prometheus metrics
            elapsed = time.time() - start
            EVENTS_TOTAL.labels(
                service="traffic-service",
                topic=settings.kafka_traffic_topic,
                status=status,
            ).inc()
            KAFKA_PROCESS_LATENCY.labels(
                service="traffic-service",
                topic=settings.kafka_traffic_topic,
            ).observe(elapsed)

    async def _process_message(self, data: dict) -> None:
        """Process a single traffic-density message."""
        trace_id = data.get("trace_id", "")
        event_id = data.get("event_id", "")

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
            await service.process_traffic_update(
                update,
                trace_id=trace_id,
                event_id=event_id,
            )
            await session.commit()


# Singleton consumer instance
traffic_consumer = TrafficKafkaConsumer()
