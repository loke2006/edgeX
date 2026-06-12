"""
EdgeCloudX Compute Workers — Congestion Analysis
===================================================
Ray remote task that computes weighted congestion scores per intersection
using an exponential moving average. Results are stored in Redis.
"""

import json
import logging

import ray
import redis

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@ray.remote
class CongestionAnalyzer:
    """
    Ray actor that maintains per-intersection congestion state
    and computes EMA-smoothed congestion scores.
    """

    def __init__(self):
        self.ema_scores: dict[str, float] = {}
        self.alpha = settings.congestion_ema_alpha
        self.redis_client = redis.from_url(settings.redis_url, decode_responses=True)
        logger.info("CongestionAnalyzer actor initialized")

    def process_batch(self, events: list[dict]) -> dict[str, float]:
        """
        Process a batch of traffic events and update congestion scores.

        Args:
            events: List of traffic-density event dicts from Kafka.

        Returns:
            Dict mapping intersection_id to updated EMA congestion score.
        """
        updated = {}

        for event in events:
            iid = event.get("intersection_id", "unknown")
            raw_score = float(event.get("congestion_score", 0.0))
            vehicle_count = int(event.get("vehicle_count", 0))

            # Exponential Moving Average smoothing
            prev = self.ema_scores.get(iid, raw_score)
            ema = self.alpha * raw_score + (1 - self.alpha) * prev
            self.ema_scores[iid] = ema

            # Determine congestion level
            if ema < 0.25:
                level = "low"
            elif ema < 0.5:
                level = "moderate"
            elif ema < 0.75:
                level = "high"
            else:
                level = "critical"

            # Store in Redis
            key = f"{settings.redis_compute_prefix}congestion:{iid}"
            state = {
                "intersection_id": iid,
                "raw_score": str(round(raw_score, 4)),
                "ema_score": str(round(ema, 4)),
                "vehicle_count": str(vehicle_count),
                "congestion_level": level,
            }
            self.redis_client.hset(key, mapping=state)
            updated[iid] = ema

        # Publish aggregated update to Redis channel
        if updated:
            self.redis_client.publish(
                f"{settings.redis_compute_prefix}congestion:updates",
                json.dumps({
                    "type": "congestion_update",
                    "intersections": {
                        k: round(v, 4) for k, v in updated.items()
                    },
                }),
            )

        return updated

    def get_all_scores(self) -> dict[str, float]:
        """Return current EMA scores for all tracked intersections."""
        return {k: round(v, 4) for k, v in self.ema_scores.items()}

    def shutdown(self):
        """Clean up Redis connection."""
        self.redis_client.close()
