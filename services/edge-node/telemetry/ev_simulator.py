"""
EdgeCloudX Edge Node — EV Telemetry Simulator
===============================================
Simulates multiple electric vehicles moving through the city grid,
publishing real-time telemetry data (position, battery, speed).
"""

import logging
import math
import random
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SimulatedEV:
    """A simulated electric vehicle."""
    ev_id: str
    row: float
    col: float
    target_row: int
    target_col: int
    battery_level: float  # 0-100%
    speed: float  # km/h
    status: str = "moving"  # moving, charging, parked, emergency

    def update(self, grid_rows: int, grid_cols: int) -> dict:
        """Update EV position and state. Returns telemetry dict."""

        # Move toward target
        if self.status == "moving":
            dr = self.target_row - self.row
            dc = self.target_col - self.col
            dist = math.sqrt(dr ** 2 + dc ** 2)

            if dist < 0.2:
                # Reached target — pick new destination
                self.target_row = random.randint(0, grid_rows - 1)
                self.target_col = random.randint(0, grid_cols - 1)
                self.row = round(self.row)
                self.col = round(self.col)
            else:
                # Move step (normalized direction * speed factor)
                step = 0.15 * (self.speed / 50.0)
                self.row += (dr / dist) * step
                self.col += (dc / dist) * step

            # Battery drain
            self.battery_level = max(0, self.battery_level - random.uniform(0.01, 0.05))

            # Speed variation
            self.speed = max(5, min(80, self.speed + random.gauss(0, 3)))

            # Low battery → charging
            if self.battery_level < 10:
                self.status = "charging"
                self.speed = 0

        elif self.status == "charging":
            self.battery_level = min(100, self.battery_level + random.uniform(0.5, 1.5))
            if self.battery_level > 80:
                self.status = "moving"
                self.speed = random.uniform(20, 50)

        return {
            "ev_id": self.ev_id,
            "position": {
                "row": round(self.row, 2),
                "col": round(self.col, 2),
            },
            "target": {
                "row": self.target_row,
                "col": self.target_col,
            },
            "battery_level": round(self.battery_level, 1),
            "speed_kmh": round(self.speed, 1),
            "status": self.status,
            "nearest_intersection": f"int-{int(round(self.row))}-{int(round(self.col))}",
        }


class EVFleetSimulator:
    """Manages a fleet of simulated EVs."""

    def __init__(self, ev_count: int = 4, grid_rows: int = 4, grid_cols: int = 4):
        self.grid_rows = grid_rows
        self.grid_cols = grid_cols
        self.evs: list[SimulatedEV] = []

        for i in range(ev_count):
            ev = SimulatedEV(
                ev_id=f"ev-{i:03d}",
                row=random.uniform(0, grid_rows - 1),
                col=random.uniform(0, grid_cols - 1),
                target_row=random.randint(0, grid_rows - 1),
                target_col=random.randint(0, grid_cols - 1),
                battery_level=random.uniform(40, 100),
                speed=random.uniform(20, 60),
            )
            self.evs.append(ev)

        logger.info(f"EV fleet simulator initialized with {ev_count} vehicles")

    def tick(self) -> list[dict]:
        """Update all EVs and return telemetry data."""
        return [ev.update(self.grid_rows, self.grid_cols) for ev in self.evs]
