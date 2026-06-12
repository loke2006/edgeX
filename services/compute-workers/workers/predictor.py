"""
EdgeCloudX Compute Workers — Traffic Predictor
=================================================
Ray remote task that maintains a rolling window of congestion history
and uses exponential smoothing to predict future congestion levels.
"""

import json
import logging
import time

import ray
import redis

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@ray.remote
class TrafficPredictor:
    """
    Ray actor that maintains per-intersection congestion history
    and predicts future congestion using exponential smoothing.
    """

    def __init__(self):
        self.history: dict[str, list[tuple[float, float]]] = {}
        self.max_history = 60  # Keep last 60 data points per intersection
        self.smoothing_alpha = 0.4
        self.redis_client = redis.from_url(
            settings.redis_url, decode_responses=True
        )
        logger.info("TrafficPredictor actor initialized")

    def record(self, intersection_id: str, score: float) -> None:
        """Record a congestion observation for an intersection."""
        ts = time.time()
        if intersection_id not in self.history:
            self.history[intersection_id] = []

        self.history[intersection_id].append((ts, score))

        # Trim to max window
        if len(self.history[intersection_id]) > self.max_history:
            self.history[intersection_id] = (
                self.history[intersection_id][-self.max_history:]
            )

    def predict(self) -> dict[str, dict]:
        """
        Generate predictions for all tracked intersections.
        Uses double exponential smoothing (Holt's method) for trend-aware
        forecasting.

        Returns:
            Dict mapping intersection_id to prediction info.
        """
        predictions = {}

        for iid, history in self.history.items():
            if len(history) < 3:
                continue

            scores = [s for _, s in history]

            # Simple exponential smoothing for level
            level = scores[0]
            trend = 0.0
            beta = 0.3  # trend smoothing factor

            for score in scores[1:]:
                prev_level = level
                level = self.smoothing_alpha * score + (
                    1 - self.smoothing_alpha
                ) * (prev_level + trend)
                trend = beta * (level - prev_level) + (1 - beta) * trend

            # Predict next 3 intervals (15 minutes at ~5s intervals)
            forecast_steps = [1, 3, 6]
            forecasts = {}
            for step in forecast_steps:
                predicted = max(0.0, min(1.0, level + trend * step))
                label = f"+{step * 5}s"
                forecasts[label] = round(predicted, 4)

            # Determine predicted trend direction
            if trend > 0.01:
                trend_dir = "increasing"
            elif trend < -0.01:
                trend_dir = "decreasing"
            else:
                trend_dir = "stable"

            pred = {
                "intersection_id": iid,
                "current_score": round(scores[-1], 4),
                "smoothed_score": round(level, 4),
                "trend": round(trend, 6),
                "trend_direction": trend_dir,
                "forecasts": forecasts,
            }
            predictions[iid] = pred

            # Store in Redis
            self.redis_client.set(
                f"{settings.redis_compute_prefix}predictions:{iid}",
                json.dumps(pred),
                ex=30,
            )

        # Publish predictions summary
        if predictions:
            summary = {
                "type": "prediction_update",
                "intersection_count": len(predictions),
                "trending_up": sum(
                    1 for p in predictions.values()
                    if p["trend_direction"] == "increasing"
                ),
                "trending_down": sum(
                    1 for p in predictions.values()
                    if p["trend_direction"] == "decreasing"
                ),
                "stable": sum(
                    1 for p in predictions.values()
                    if p["trend_direction"] == "stable"
                ),
            }
            self.redis_client.publish(
                f"{settings.redis_compute_prefix}predictions:updates",
                json.dumps(summary),
            )

        return predictions

    def shutdown(self):
        """Clean up Redis connection."""
        self.redis_client.close()
