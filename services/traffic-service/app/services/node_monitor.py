"""
EdgeCloudX Traffic Service — Node Health Monitor
===================================================
Consumes node-health Kafka topic and tracks edge node status.

Nodes are classified as:
  - healthy   : heartbeat within last 30s
  - degraded  : heartbeat within last 60s
  - dead      : no heartbeat for 60s+

Status is stored in Redis and published for the dashboard.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from shared.metrics import ACTIVE_EDGE_NODES

logger = logging.getLogger(__name__)

HEALTHY_TIMEOUT = 30  # seconds
DEGRADED_TIMEOUT = 60  # seconds


class NodeMonitor:
    """Tracks edge node health via Kafka heartbeats."""

    def __init__(
        self,
        kafka_servers: str = "kafka:9092",
        redis_url: str = "redis://redis:6379/0",
        health_topic: str = "node-health",
    ):
        self.kafka_servers = kafka_servers
        self.redis_url = redis_url
        self.health_topic = health_topic
        self._running = False
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._nodes: dict[str, dict] = {}  # node_id -> last heartbeat data

    async def run(self) -> None:
        """Start consuming heartbeats and checking node status."""
        self._running = True

        # Start Kafka consumer
        self._consumer = AIOKafkaConsumer(
            self.health_topic,
            bootstrap_servers=self.kafka_servers,
            group_id="node-monitor-group",
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        for attempt in range(10):
            try:
                await self._consumer.start()
                logger.info(f"Node monitor consumer started on topic: {self.health_topic}")
                break
            except Exception as e:
                wait = min(2 ** attempt, 30)
                logger.warning(f"Node monitor Kafka attempt {attempt + 1}/10: {e}. Retry in {wait}s")
                await asyncio.sleep(wait)
        else:
            logger.error("Node monitor failed to connect to Kafka")
            return

        # Run consumer + status checker concurrently
        consumer_task = asyncio.create_task(self._consume_heartbeats())
        checker_task = asyncio.create_task(self._check_status_loop())

        try:
            await asyncio.gather(consumer_task, checker_task)
        except asyncio.CancelledError:
            pass
        finally:
            await self._consumer.stop()

    async def stop(self) -> None:
        self._running = False

    async def _consume_heartbeats(self) -> None:
        """Process incoming heartbeat messages."""
        async for message in self._consumer:
            if not self._running:
                break

            data = message.value
            node_id = data.get("node_id") or data.get("edge_node_id", "unknown")

            self._nodes[node_id] = {
                "node_id": node_id,
                "last_seen": time.time(),
                "status": "healthy",
                "cpu_percent": data.get("cpu_percent", -1),
                "memory_percent": data.get("memory_percent", -1),
                "fps": data.get("fps", 0),
                "tick_count": data.get("tick_count", 0),
                "evs_tracked": data.get("evs_tracked", 0),
                "uptime_seconds": data.get("uptime_seconds", 0),
            }

    async def _check_status_loop(self) -> None:
        """Periodically check node statuses and publish updates."""
        while self._running:
            await asyncio.sleep(10)

            now = time.time()
            r = aioredis.from_url(self.redis_url, decode_responses=True)

            healthy_count = 0
            status_updates = []

            for node_id, info in list(self._nodes.items()):
                age = now - info["last_seen"]

                if age <= HEALTHY_TIMEOUT:
                    new_status = "healthy"
                    healthy_count += 1
                elif age <= DEGRADED_TIMEOUT:
                    new_status = "degraded"
                else:
                    new_status = "dead"

                old_status = info.get("status", "unknown")
                info["status"] = new_status

                # Store in Redis
                key = f"node:{node_id}"
                await r.hset(key, mapping={
                    "node_id": node_id,
                    "status": new_status,
                    "cpu_percent": str(info.get("cpu_percent", -1)),
                    "memory_percent": str(info.get("memory_percent", -1)),
                    "fps": str(info.get("fps", 0)),
                    "tick_count": str(info.get("tick_count", 0)),
                    "uptime_seconds": str(info.get("uptime_seconds", 0)),
                    "last_seen": str(info["last_seen"]),
                })
                await r.expire(key, 120)

                if new_status != old_status:
                    status_updates.append({
                        "node_id": node_id,
                        "old_status": old_status,
                        "new_status": new_status,
                        "cpu_percent": info.get("cpu_percent", -1),
                        "memory_percent": info.get("memory_percent", -1),
                    })

            # Update Prometheus gauge
            ACTIVE_EDGE_NODES.set(healthy_count)

            # Publish status changes
            if status_updates:
                await r.publish("nodes:status", json.dumps({
                    "type": "node_status_update",
                    "nodes": status_updates,
                }))
                for update in status_updates:
                    logger.info(
                        "Node status change",
                        extra={
                            "node_id": update["node_id"],
                            "old_status": update["old_status"],
                            "new_status": update["new_status"],
                        },
                    )

            await r.aclose()
