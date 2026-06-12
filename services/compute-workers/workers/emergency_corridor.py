"""
EdgeCloudX Compute Workers — Emergency Corridor
==================================================
Ray remote task that computes shortest green-wave signal corridors
for emergency vehicles. Triggers on emergency-alert Kafka messages.
"""

import heapq
import json
import logging

import ray
import redis

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@ray.remote
def compute_emergency_corridor(alert: dict) -> dict | None:
    """
    Compute a green corridor for an emergency vehicle.

    This finds the shortest path (ignoring congestion penalties)
    and sets signal overrides on all intersections along the path.

    Args:
        alert: Emergency alert dict with vehicle position and destination.

    Returns:
        Corridor info dict with path, signal overrides, and ETA.
    """
    r = redis.from_url(settings.redis_url, decode_responses=True)
    rows = settings.grid_rows
    cols = settings.grid_cols

    # Extract start/end from alert
    start_row = int(alert.get("start_row", alert.get("row", 0)))
    start_col = int(alert.get("start_col", alert.get("col", 0)))
    end_row = int(alert.get("end_row", alert.get("dest_row", rows - 1)))
    end_col = int(alert.get("end_col", alert.get("dest_col", cols - 1)))
    emergency_id = alert.get("emergency_id", alert.get("ev_id", "emergency-1"))
    emergency_type = alert.get("emergency_type", "ambulance")

    start = (start_row, start_col)
    end = (end_row, end_col)

    # Pure shortest path (no congestion penalty — emergency overrides)
    def neighbors(node):
        nr, nc = node
        for r2, c2 in [(nr - 1, nc), (nr + 1, nc), (nr, nc - 1), (nr, nc + 1)]:
            if 0 <= r2 < rows and 0 <= c2 < cols:
                yield (r2, c2)

    def heuristic(a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    open_set = [(0.0, start)]
    came_from = {start: None}
    g_score = {start: 0.0}

    while open_set:
        _, current = heapq.heappop(open_set)
        if current == end:
            break
        for nbr in neighbors(current):
            tentative_g = g_score[current] + 1.0
            if nbr not in g_score or tentative_g < g_score[nbr]:
                g_score[nbr] = tentative_g
                f = tentative_g + heuristic(nbr, end)
                heapq.heappush(open_set, (f, nbr))
                came_from[nbr] = current

    if end not in came_from:
        logger.warning(
            f"No path found for emergency corridor: {start} -> {end}"
        )
        r.close()
        return None

    # Reconstruct path
    path = []
    node = end
    while node is not None:
        path.append(node)
        node = came_from[node]
    path.reverse()

    # Set signal overrides to GREEN for all intersections in corridor
    green_intersections = []
    for pr, pc in path:
        iid = f"int-{pr}-{pc}"
        green_intersections.append(iid)

        # Override signal state in the main intersection data
        ikey = f"{settings.redis_intersection_prefix}{iid}"
        r.hset(ikey, "signal_state", "green")
        r.hset(ikey, "is_emergency_active", "True")

        # Store in compute namespace too
        ckey = f"{settings.redis_compute_prefix}corridor:{iid}"
        r.set(ckey, emergency_id, ex=60)  # 60s TTL

    # Estimated time of arrival (assuming ~2 seconds per intersection)
    eta_seconds = (len(path) - 1) * 2

    corridor = {
        "emergency_id": emergency_id,
        "emergency_type": emergency_type,
        "path": [
            {"row": pr, "col": pc, "intersection_id": f"int-{pr}-{pc}"}
            for pr, pc in path
        ],
        "green_intersections": green_intersections,
        "distance": len(path) - 1,
        "eta_seconds": eta_seconds,
        "signal_override": "green",
    }

    # Store corridor in Redis
    r.set(
        f"{settings.redis_compute_prefix}emergency:corridor:{emergency_id}",
        json.dumps(corridor),
        ex=120,  # 2 min TTL
    )

    # Publish corridor notification
    r.publish(
        f"{settings.redis_compute_prefix}emergency:corridor",
        json.dumps({"type": "emergency_corridor", **corridor}),
    )

    r.close()
    logger.info(
        f"Emergency corridor computed: {emergency_id} "
        f"({len(path)} steps, ETA {eta_seconds}s)"
    )

    return corridor
