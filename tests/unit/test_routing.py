"""
EdgeCloudX — Unit Tests: Routing (Pathfinder)
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "routing-service"))


class TestCityGrid:
    """Test city grid neighbor and heuristic functions."""

    def test_grid_neighbors_center(self):
        from app.services.pathfinder import CityGrid

        grid = CityGrid(4, 4)
        neighbors = grid.neighbors((1, 1))
        assert set(neighbors) == {(0, 1), (2, 1), (1, 0), (1, 2)}

    def test_grid_neighbors_corner(self):
        from app.services.pathfinder import CityGrid

        grid = CityGrid(4, 4)
        neighbors = grid.neighbors((0, 0))
        assert set(neighbors) == {(1, 0), (0, 1)}

    def test_grid_neighbors_edge(self):
        from app.services.pathfinder import CityGrid

        grid = CityGrid(4, 4)
        neighbors = grid.neighbors((0, 2))
        assert set(neighbors) == {(0, 1), (0, 3), (1, 2)}

    def test_heuristic_manhattan(self):
        from app.services.pathfinder import CityGrid

        assert CityGrid.heuristic((0, 0), (3, 3)) == 6
        assert CityGrid.heuristic((1, 1), (1, 1)) == 0
        assert CityGrid.heuristic((0, 0), (0, 3)) == 3

    def test_grid_bottom_right_corner(self):
        from app.services.pathfinder import CityGrid

        grid = CityGrid(4, 4)
        neighbors = grid.neighbors((3, 3))
        assert set(neighbors) == {(2, 3), (3, 2)}


class TestPathfinder:
    """Test A* pathfinding (without Redis, so no congestion weighting)."""

    @pytest.mark.asyncio
    async def test_basic_route(self):
        from app.services.pathfinder import Pathfinder

        pf = Pathfinder()
        result = await pf.find_route((0, 0), (3, 3), avoid_congestion=False)

        assert result is not None
        assert result["distance"] == 6  # Manhattan distance
        assert result["path"][0]["row"] == 0
        assert result["path"][0]["col"] == 0
        assert result["path"][-1]["row"] == 3
        assert result["path"][-1]["col"] == 3

    @pytest.mark.asyncio
    async def test_same_start_end(self):
        from app.services.pathfinder import Pathfinder

        pf = Pathfinder()
        result = await pf.find_route((2, 2), (2, 2), avoid_congestion=False)

        assert result is not None
        assert result["distance"] == 0
        assert len(result["path"]) == 1

    @pytest.mark.asyncio
    async def test_adjacent_route(self):
        from app.services.pathfinder import Pathfinder

        pf = Pathfinder()
        result = await pf.find_route((0, 0), (0, 1), avoid_congestion=False)

        assert result is not None
        assert result["distance"] == 1

    @pytest.mark.asyncio
    async def test_invalid_start(self):
        from app.services.pathfinder import Pathfinder

        pf = Pathfinder()
        result = await pf.find_route((-1, 0), (3, 3), avoid_congestion=False)
        assert result is None

    @pytest.mark.asyncio
    async def test_emergency_corridor(self):
        from app.services.pathfinder import Pathfinder

        pf = Pathfinder()
        result = await pf.find_emergency_corridor((0, 0), (3, 3))

        assert result is not None
        assert result["corridor_type"] == "emergency"
        assert result["signal_override"] == "green"
        assert len(result["green_intersections"]) > 0
