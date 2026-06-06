"""
EdgeCloudX — Unit Tests: Edge Node Detector
"""

import pytest
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "edge-node"))


class TestTrafficSimulator:
    """Test traffic simulation engine."""

    def test_simulator_init(self):
        from detector.frame_generator import TrafficSimulator

        sim = TrafficSimulator(grid_rows=4, grid_cols=4, density="medium")
        assert len(sim.intersections) == 16
        assert sim.grid_rows == 4
        assert sim.grid_cols == 4

    def test_tick_returns_events(self):
        from detector.frame_generator import TrafficSimulator

        sim = TrafficSimulator(grid_rows=2, grid_cols=2, density="low")
        events = sim.tick_simulation()

        assert len(events) == 4  # 2x2 grid
        for event in events:
            assert "intersection_id" in event
            assert "vehicle_count" in event
            assert "congestion_score" in event
            assert 0 <= event["congestion_score"] <= 1.0

    def test_frame_generation(self):
        from detector.frame_generator import TrafficSimulator

        sim = TrafficSimulator(grid_rows=2, grid_cols=2, density="low")
        frame = sim.generate_frame(640, 480)

        assert isinstance(frame, np.ndarray)
        assert frame.shape == (480, 640, 3)

    def test_density_levels(self):
        from detector.frame_generator import TrafficSimulator

        for density in ["low", "medium", "high"]:
            sim = TrafficSimulator(grid_rows=2, grid_cols=2, density=density)
            assert sim.density == density


class TestEVSimulator:
    """Test EV fleet simulation."""

    def test_fleet_init(self):
        from telemetry.ev_simulator import EVFleetSimulator

        fleet = EVFleetSimulator(ev_count=4, grid_rows=4, grid_cols=4)
        assert len(fleet.evs) == 4

    def test_fleet_tick(self):
        from telemetry.ev_simulator import EVFleetSimulator

        fleet = EVFleetSimulator(ev_count=3, grid_rows=4, grid_cols=4)
        data = fleet.tick()

        assert len(data) == 3
        for ev in data:
            assert "ev_id" in ev
            assert "position" in ev
            assert "battery_level" in ev
            assert "speed_kmh" in ev
            assert "status" in ev
            assert ev["battery_level"] >= 0
            assert ev["battery_level"] <= 100

    def test_ev_movement(self):
        from telemetry.ev_simulator import EVFleetSimulator

        fleet = EVFleetSimulator(ev_count=1, grid_rows=4, grid_cols=4)
        initial = fleet.tick()[0]
        # Run several ticks
        for _ in range(10):
            updated = fleet.tick()[0]

        # EV should have moved (position changed or battery decreased)
        assert updated["battery_level"] <= initial["battery_level"]
