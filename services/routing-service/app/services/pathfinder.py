"""
EdgeCloudX Routing Service — A* Pathfinder
===========================================
Implements A* pathfinding on a weighted city grid.
Edge weights are dynamically adjusted based on real-time congestion from Redis.
"""

import heapq
import logging
from typing import Optional

import redis.asyncio as aioredis

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class CityGrid:
    """Represents the city as a weighted graph for pathfinding."""

    def __init__(self, rows: int, cols: int):
        self.rows = rows
        self.cols = cols

    def neighbors(self, node: tuple[int, int]) -> list[tuple[int, int]]:
        """Get valid neighboring nodes (4-directional movement)."""
        row, col = node
        candidates = [
            (row - 1, col),  # Up
            (row + 1, col),  # Down
            (row, col - 1),  # Left
            (row, col + 1),  # Right
        ]
        return [
            (r, c) for r, c in candidates
            if 0 <= r < self.rows and 0 <= c < self.cols
        ]

    @staticmethod
    def heuristic(a: tuple[int, int], b: tuple[int, int]) -> float:
        """Manhattan distance heuristic."""
        return abs(a[0] - b[0]) + abs(a[1] - b[1])


class Pathfinder:
    """A* pathfinding engine with real-time congestion awareness."""

    def __init__(self):
        self.grid = CityGrid(settings.grid_rows, settings.grid_cols)

    async def get_congestion_weights(self) -> dict[str, float]:
        """Fetch real-time congestion scores from Redis."""
        weights = {}
        try:
            r = aioredis.from_url(settings.redis_url)
            for row in range(self.grid.rows):
                for col in range(self.grid.cols):
                    key = f"{settings.redis_intersection_prefix}int-{row}-{col}"
                    data = await r.hgetall(key)
                    if data:
                        score = float(data.get(b"congestion_score", b"0.0"))
                        weights[f"{row}-{col}"] = score
                    else:
                        weights[f"{row}-{col}"] = 0.0
            await r.aclose()
        except Exception as e:
            logger.warning(f"Failed to fetch congestion from Redis: {e}")
            # Default all weights to 0 (no congestion)
            for row in range(self.grid.rows):
                for col in range(self.grid.cols):
                    weights[f"{row}-{col}"] = 0.0
        return weights

    async def find_route(
        self,
        start: tuple[int, int],
        end: tuple[int, int],
        avoid_congestion: bool = True,
    ) -> Optional[dict]:
        """
        Find optimal route using A* with congestion-weighted edges.

        Returns:
            Dict with path, total_cost, distance, and congestion_avoided.
        """
        if not (0 <= start[0] < self.grid.rows and 0 <= start[1] < self.grid.cols):
            return None
        if not (0 <= end[0] < self.grid.rows and 0 <= end[1] < self.grid.cols):
            return None

        # Fetch live congestion weights
        congestion = await self.get_congestion_weights() if avoid_congestion else {}

        # A* algorithm
        open_set = []
        heapq.heappush(open_set, (0.0, start))
        came_from: dict[tuple[int, int], Optional[tuple[int, int]]] = {start: None}
        g_score: dict[tuple[int, int], float] = {start: 0.0}

        while open_set:
            current_f, current = heapq.heappop(open_set)

            if current == end:
                break

            for neighbor in self.grid.neighbors(current):
                # Base cost is 1.0 per grid step
                base_cost = 1.0

                # Add congestion penalty (0-2x multiplier)
                if avoid_congestion:
                    key = f"{neighbor[0]}-{neighbor[1]}"
                    congestion_score = congestion.get(key, 0.0)
                    # Higher congestion = higher cost to traverse
                    congestion_penalty = congestion_score * 2.0
                    edge_cost = base_cost + congestion_penalty
                else:
                    edge_cost = base_cost

                tentative_g = g_score[current] + edge_cost

                if neighbor not in g_score or tentative_g < g_score[neighbor]:
                    g_score[neighbor] = tentative_g
                    f_score = tentative_g + self.grid.heuristic(neighbor, end)
                    heapq.heappush(open_set, (f_score, neighbor))
                    came_from[neighbor] = current

        # Reconstruct path
        if end not in came_from:
            return None

        path = []
        current = end
        total_congestion = 0.0
        while current is not None:
            path.append(current)
            key = f"{current[0]}-{current[1]}"
            total_congestion += congestion.get(key, 0.0)
            current = came_from[current]
        path.reverse()

        return {
            "path": [{"row": r, "col": c, "intersection_id": f"int-{r}-{c}"} for r, c in path],
            "total_cost": round(g_score[end], 3),
            "distance": len(path) - 1,
            "total_congestion_exposure": round(total_congestion, 3),
            "steps": len(path),
        }

    async def find_emergency_corridor(
        self,
        start: tuple[int, int],
        end: tuple[int, int],
    ) -> Optional[dict]:
        """
        Find emergency green corridor — shortest path ignoring congestion.
        All intersections along the path should be set to green.
        """
        route = await self.find_route(start, end, avoid_congestion=False)
        if route:
            route["corridor_type"] = "emergency"
            route["signal_override"] = "green"
            # Mark all intersections in corridor
            intersection_ids = [step["intersection_id"] for step in route["path"]]
            route["green_intersections"] = intersection_ids
        return route
