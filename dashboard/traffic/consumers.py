"""
EdgeCloudX Dashboard — WebSocket Consumers
=============================================
Django Channels consumers that subscribe to Redis Pub/Sub channels
and stream live updates to connected browser clients.
"""

import asyncio
import json
import logging

import redis.asyncio as aioredis
from channels.generic.websocket import AsyncWebsocketConsumer
from django.conf import settings

logger = logging.getLogger(__name__)


class BaseRedisConsumer(AsyncWebsocketConsumer):
    """
    Base WebSocket consumer that subscribes to a Redis Pub/Sub channel
    and forwards messages to the connected client.
    """

    redis_channel = ""
    consumer_type = "base"

    async def connect(self):
        await self.accept()
        logger.info(f"WebSocket connected: {self.consumer_type}")
        self._running = True
        self._task = asyncio.create_task(self._subscribe_redis())

    async def disconnect(self, close_code):
        self._running = False
        if hasattr(self, "_task"):
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(
            f"WebSocket disconnected: {self.consumer_type} "
            f"(code={close_code})"
        )

    async def _subscribe_redis(self):
        """Subscribe to Redis Pub/Sub and forward messages."""
        try:
            r = aioredis.from_url(settings.REDIS_URL)
            pubsub = r.pubsub()
            await pubsub.subscribe(self.redis_channel)
            logger.info(
                f"{self.consumer_type} subscribed to: {self.redis_channel}"
            )

            while self._running:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message and message["type"] == "message":
                    data = message["data"]
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    await self.send(text_data=data)

            await pubsub.unsubscribe(self.redis_channel)
            await r.aclose()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(
                f"{self.consumer_type} Redis error: {e}", exc_info=True
            )

    async def receive(self, text_data=None, bytes_data=None):
        """Handle incoming messages from the client (ping/pong)."""
        if text_data:
            try:
                data = json.loads(text_data)
                if data.get("type") == "ping":
                    await self.send(
                        text_data=json.dumps({"type": "pong"})
                    )
            except json.JSONDecodeError:
                pass


class TrafficConsumer(BaseRedisConsumer):
    """Streams live traffic grid updates from Redis."""

    redis_channel = "traffic:updates"
    consumer_type = "traffic"


class HeatmapConsumer(BaseRedisConsumer):
    """Streams heatmap updates from compute workers."""

    redis_channel = "compute:heatmap:updates"
    consumer_type = "heatmap"


class AlertConsumer(BaseRedisConsumer):
    """Streams emergency alert and corridor notifications."""

    redis_channel = "compute:emergency:corridor"
    consumer_type = "alerts"


class EVTrackerConsumer(BaseRedisConsumer):
    """Streams EV telemetry updates."""

    redis_channel = "compute:routes:updates"
    consumer_type = "ev_tracker"
