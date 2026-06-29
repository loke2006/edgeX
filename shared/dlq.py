"""
EdgeCloudX Shared — Kafka Dead Letter Queue
==============================================
Publishes failed messages to a DLQ topic with error metadata.

Usage:
    from shared.dlq import DeadLetterPublisher

    dlq = DeadLetterPublisher(bootstrap_servers="kafka:9092")
    await dlq.start()
    await dlq.send("traffic-density", original_msg, error, service="traffic-service")
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class DeadLetterPublisher:
    """Publishes failed Kafka messages to a dead-letter topic."""

    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        """Connect the DLQ producer to Kafka."""
        for attempt in range(5):
            try:
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    acks="all",
                )
                await self._producer.start()
                logger.info("DLQ producer connected")
                return
            except Exception as e:
                wait = min(2 ** attempt, 15)
                logger.warning(f"DLQ producer connect attempt {attempt + 1}/5 failed: {e}. Retry in {wait}s")
                await asyncio.sleep(wait)
        logger.error("DLQ producer failed to connect after 5 attempts")

    async def stop(self) -> None:
        """Disconnect the DLQ producer."""
        if self._producer:
            await self._producer.stop()
            logger.info("DLQ producer disconnected")

    async def send(
        self,
        original_topic: str,
        original_message: dict[str, Any],
        error: Exception,
        *,
        service: str = "unknown",
        retry_count: int = 0,
        trace_id: str = "",
    ) -> None:
        """Send a failed message to the dead-letter topic."""
        if not self._producer:
            logger.error("DLQ producer not started, dropping failed message")
            return

        dlq_topic = f"{original_topic}-dlq"
        envelope = {
            "original_topic": original_topic,
            "original_message": original_message,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
            },
            "service": service,
            "retry_count": retry_count,
            "trace_id": trace_id,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            await self._producer.send_and_wait(dlq_topic, envelope)
            logger.warning(
                f"Message sent to DLQ: {dlq_topic}",
                extra={"trace_id": trace_id, "original_topic": original_topic},
            )
        except Exception as e:
            # Last resort — log the failure so it's not silently lost
            logger.error(
                f"Failed to send to DLQ {dlq_topic}: {e}. "
                f"Original message: {json.dumps(original_message, default=str)[:500]}",
            )


async def retry_with_dlq(
    func,
    message: dict[str, Any],
    *,
    dlq: DeadLetterPublisher,
    topic: str,
    service: str,
    max_retries: int = 3,
    backoff_base: float = 0.5,
) -> bool:
    """
    Execute ``func(message)`` with retry logic.
    On final failure, send to DLQ.

    Returns True if processing succeeded, False if sent to DLQ.
    """
    trace_id = message.get("trace_id", "")
    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            await func(message)
            return True
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                wait = backoff_base * (2 ** attempt)
                logger.warning(
                    f"Retry {attempt + 1}/{max_retries} for {topic}: {e}. "
                    f"Waiting {wait:.1f}s...",
                    extra={"trace_id": trace_id},
                )
                await asyncio.sleep(wait)

    # All retries exhausted — send to DLQ
    if last_error:
        await dlq.send(
            topic, message, last_error,
            service=service,
            retry_count=max_retries,
            trace_id=trace_id,
        )
    return False
