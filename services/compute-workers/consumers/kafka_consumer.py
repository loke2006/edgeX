"""
EdgeCloudX Compute Workers — Multi-Topic Kafka Consumer
=========================================================
Async Kafka consumer that subscribes to traffic-density, ev-telemetry,
and emergency-alerts topics and dispatches messages to Ray workers.
"""

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ComputeKafkaConsumer:
    """
    Multi-topic Kafka consumer that dispatches events to Ray workers.
    """

    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._handlers: dict[str, callable] = {}

    def register_handler(self, topic: str, handler: callable) -> None:
        """Register an async handler function for a Kafka topic."""
        self._handlers[topic] = handler
        logger.info(f"Registered handler for topic: {topic}")

    async def start(self) -> None:
        """Start the Kafka consumer with retry logic."""
        topics = [
            settings.kafka_traffic_topic,
            settings.kafka_ev_topic,
            settings.kafka_emergency_topic,
        ]

        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            retry_backoff_ms=1000,
            session_timeout_ms=30000,
        )

        for attempt in range(10):
            try:
                await self.consumer.start()
                self._running = True
                logger.info(
                    f"Kafka consumer started — subscribed to: "
                    f"{', '.join(topics)}"
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
        """Main consume loop — routes messages to registered handlers."""
        if not self.consumer or not self._running:
            logger.warning("Consumer not started, skipping consume loop")
            return

        batch: dict[str, list[dict]] = {}
        batch_size = 10
        batch_timeout = 2.0  # seconds
        last_flush = asyncio.get_event_loop().time()

        try:
            async for message in self.consumer:
                topic = message.topic
                data = message.value

                # Accumulate batch
                if topic not in batch:
                    batch[topic] = []
                batch[topic].append(data)

                now = asyncio.get_event_loop().time()
                total_msgs = sum(len(v) for v in batch.values())

                # Flush if batch is full or timeout reached
                if total_msgs >= batch_size or (now - last_flush) >= batch_timeout:
                    await self._flush_batch(batch)
                    batch = {}
                    last_flush = now

        except Exception as e:
            logger.error(f"Consumer loop error: {e}", exc_info=True)
        finally:
            # Flush remaining
            if batch:
                await self._flush_batch(batch)

    async def _flush_batch(self, batch: dict[str, list[dict]]) -> None:
        """Dispatch a batch of messages to registered handlers."""
        for topic, messages in batch.items():
            handler = self._handlers.get(topic)
            if handler:
                try:
                    await handler(messages)
                except Exception as e:
                    logger.error(
                        f"Error in handler for topic '{topic}': {e}",
                        exc_info=True,
                    )
            else:
                logger.debug(
                    f"No handler for topic '{topic}', "
                    f"dropping {len(messages)} messages"
                )


# Singleton consumer
compute_consumer = ComputeKafkaConsumer()
