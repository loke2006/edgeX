"""
EdgeCloudX Compute Workers — Route Optimizer
===============================================
Ray remote task that performs global route optimization for all EVs,
considering real-time congestion to avoid assigning multiple EVs
to the same congested corridors.
"""

import heapq
import json
import logging

import ray
import redis

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def a_star(
    grid_rows: int,
    grid_cols: int,
    start: tuple[int, int],
    end: tuple[int, int],
    congestion: dict[str, float],
    occupied_penalty: dict[str, float] | None = None,
) -> dict | None:
    """
    A* pathfinding with congestion-weighted edges and optional
    penalty for paths already assigned to other EVs.
    """
    if occupied_penalty is None:
        occupied_penalty = {}

    def neighbors(node):
        r, c = node
        for nr, nc in [(r - 1, c), (r + 1, c), (r, c - 1), (r, c + 1)]:
            if 0 <= nr < grid_rows and 0 <= nc < grid_cols:
                yield (nr, nc)

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
            key = f"{nbr[0]}-{nbr[1]}"
            base_cost = 1.0
            cong_penalty = congestion.get(key, 0.0) * 2.0
            occ_penalty = occupied_penalty.get(key, 0.0)
            edge_cost = base_cost + cong_penalty + occ_penalty

            tentative_g = g_score[current] + edge_cost
            if nbr not in g_score or tentative_g < g_score[nbr]:
                g_score[nbr] = tentative_g
                f = tentative_g + heuristic(nbr, end)
                heapq.heappush(open_set, (f, nbr))
                came_from[nbr] = current

    if end not in came_from:
        return None

    path = []
    node = end
    while node is not None:
        path.append(node)
        node = came_from[node]
    path.reverse()

    return {
        "path": [
            {"row": r, "col": c, "intersection_id": f"int-{r}-{c}"}
            for r, c in path
        ],
        "total_cost": round(g_score[end], 3),
        "distance": len(path) - 1,
        "steps": len(path),
    }


@ray.remote
def optimize_routes() -> dict:
    """
    Read all EV positions and destinations from Redis,
    run global optimization to avoid assigning multiple EVs
    to the same congested corridors.

    Returns:
        Dict mapping ev_id to optimized route.
    """
    r = redis.from_url(settings.redis_url, decode_responses=True)
    rows = settings.grid_rows
    cols = settings.grid_cols

    # Read congestion weights
    congestion = {}
    for row in range(rows):
        for col in range(cols):
            key = f"{settings.redis_compute_prefix}congestion:int-{row}-{col}"
            data = r.hgetall(key)
            if data and "ema_score" in data:
                congestion[f"{row}-{col}"] = float(data["ema_score"])
            else:
                fkey = f"{settings.redis_intersection_prefix}int-{row}-{col}"
                fdata = r.hgetall(fkey)
                if fdata and "congestion_score" in fdata:
                    congestion[f"{row}-{col}"] = float(fdata["congestion_score"])
                else:
                    congestion[f"{row}-{col}"] = 0.0

    # Read EV positions from Redis
    ev_keys = r.keys("ev:*")
    evs = []
    for ek in ev_keys:
        ev_data = r.hgetall(ek)
        if ev_data and "row" in ev_data and "col" in ev_data:
            evs.append({
                "ev_id": ev_data.get("ev_id", ek),
                "row": int(ev_data["row"]),
                "col": int(ev_data["col"]),
                "dest_row": int(ev_data.get("dest_row", rows - 1)),
                "dest_col": int(ev_data.get("dest_col", cols - 1)),
            })

    # If no EVs found, generate routes for default EV positions
    if not evs:
        evs = [
            {"ev_id": f"ev-{i}", "row": 0, "col": i, "dest_row": rows - 1, "dest_col": cols - 1 - i}
            for i in range(min(4, cols))
        ]

    # Global optimization: route EVs one by one, adding path penalties
    # to discourage later EVs from using same congested corridors
    occupied_penalty: dict[str, float] = {}
    optimized_routes = {}

    for ev in evs:
        start = (ev["row"], ev["col"])
        end = (ev["dest_row"], ev["dest_col"])

        route = a_star(rows, cols, start, end, congestion, occupied_penalty)
        if route:
            route["ev_id"] = ev["ev_id"]
            optimized_routes[ev["ev_id"]] = route

            # Add penalty for cells used by this EV's route
            for step in route["path"]:
                key = f"{step['row']}-{step['col']}"
                occupied_penalty[key] = occupied_penalty.get(key, 0.0) + 0.5

            # Store in Redis
            r.set(
                f"{settings.redis_compute_prefix}routes:{ev['ev_id']}",
                json.dumps(route),
                ex=30,
            )

    # Publish routes update
    r.publish(
        f"{settings.redis_compute_prefix}routes:updates",
        json.dumps({
            "type": "route_optimization",
            "ev_count": len(optimized_routes),
            "routes": {
                k: {"steps": v["steps"], "cost": v["total_cost"]}
                for k, v in optimized_routes.items()
            },
        }),
    )

    r.close()
    return optimized_routes
