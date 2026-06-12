"""
EdgeCloudX Compute Workers — Heatmap Generation
==================================================
Ray remote task that generates a smoothed congestion heatmap matrix
using Gaussian kernel smoothing. Results stored in Redis.
"""

import json
import logging

import numpy as np
import ray
import redis

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def gaussian_smooth(matrix: np.ndarray, sigma: float = 0.8) -> np.ndarray:
    """
    Apply simple Gaussian smoothing to a 2D congestion matrix.
    Uses a 3×3 kernel for lightweight computation.
    """
    kernel = np.array([
        [1, 2, 1],
        [2, 4, 2],
        [1, 2, 1],
    ], dtype=float)
    kernel /= kernel.sum()

    rows, cols = matrix.shape
    padded = np.pad(matrix, 1, mode="edge")
    result = np.zeros_like(matrix)

    for r in range(rows):
        for c in range(cols):
            region = padded[r:r + 3, c:c + 3]
            result[r, c] = np.sum(region * kernel)

    # Blend: 70% smoothed + 30% original for sharpness
    blended = 0.7 * result + 0.3 * matrix
    return np.clip(blended, 0.0, 1.0)


@ray.remote
def generate_heatmap() -> dict:
    """
    Read all intersection congestion data from Redis and generate
    a smoothed heatmap matrix.

    Returns:
        Dict with grid dimensions, raw matrix, smoothed matrix, and stats.
    """
    r = redis.from_url(settings.redis_url, decode_responses=True)
    rows = settings.grid_rows
    cols = settings.grid_cols

    # Build raw congestion matrix from Redis
    raw_matrix = np.zeros((rows, cols))

    for row in range(rows):
        for col in range(cols):
            # Try compute prefix first (from our congestion analyzer)
            key = f"{settings.redis_compute_prefix}congestion:int-{row}-{col}"
            data = r.hgetall(key)
            if data and "ema_score" in data:
                raw_matrix[row, col] = float(data["ema_score"])
            else:
                # Fallback to traffic-service intersection data
                key = f"{settings.redis_intersection_prefix}int-{row}-{col}"
                data = r.hgetall(key)
                if data and "congestion_score" in data:
                    raw_matrix[row, col] = float(data["congestion_score"])

    # Apply Gaussian smoothing
    smoothed_matrix = gaussian_smooth(raw_matrix)

    # Compute statistics
    avg_congestion = float(np.mean(raw_matrix))
    peak_congestion = float(np.max(raw_matrix))
    hotspot_count = int(np.sum(raw_matrix >= 0.7))

    result = {
        "grid_rows": rows,
        "grid_cols": cols,
        "raw_heatmap": raw_matrix.round(4).tolist(),
        "smoothed_heatmap": smoothed_matrix.round(4).tolist(),
        "avg_congestion": round(avg_congestion, 4),
        "peak_congestion": round(peak_congestion, 4),
        "hotspot_count": hotspot_count,
    }

    # Store in Redis
    r.set(
        f"{settings.redis_compute_prefix}heatmap:latest",
        json.dumps(result),
        ex=30,  # TTL 30 seconds
    )

    # Publish update notification
    r.publish(
        f"{settings.redis_compute_prefix}heatmap:updates",
        json.dumps({"type": "heatmap_update", **result}),
    )

    r.close()
    return result
