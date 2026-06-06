"""
EdgeCloudX Edge Node — Synthetic Traffic Frame Generator
=========================================================
Generates synthetic traffic scenes with vehicles, intersections, and traffic lights.
Used when YOLOv8 is disabled or for simulation-only mode.
"""

import logging
import math
import random
from dataclasses import dataclass, field

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class Vehicle:
    """Simulated vehicle with position and direction."""
    x: float
    y: float
    dx: float  # velocity x
    dy: float  # velocity y
    vehicle_type: str = "car"  # car, truck, ambulance, ev
    speed: float = 1.0


@dataclass
class IntersectionSim:
    """Simulated intersection state."""
    intersection_id: str
    row: int
    col: int
    vehicles: list[Vehicle] = field(default_factory=list)
    vehicle_count: int = 0
    congestion_score: float = 0.0
    has_anomaly: bool = False
    anomaly_type: str | None = None


class TrafficSimulator:
    """
    Generates realistic traffic simulation data without actual video processing.
    Simulates vehicle flow, congestion patterns, and anomaly events.
    """

    def __init__(self, grid_rows: int = 4, grid_cols: int = 4, density: str = "medium"):
        self.grid_rows = grid_rows
        self.grid_cols = grid_cols
        self.density = density
        self.tick = 0

        # Density ranges
        self.density_ranges = {
            "low": (0, 8),
            "medium": (3, 18),
            "high": (8, 30),
        }

        # Initialize intersections
        self.intersections: dict[str, IntersectionSim] = {}
        for r in range(grid_rows):
            for c in range(grid_cols):
                iid = f"int-{r}-{c}"
                self.intersections[iid] = IntersectionSim(
                    intersection_id=iid,
                    row=r,
                    col=c,
                    vehicle_count=random.randint(*self.density_ranges[density]),
                )

        logger.info(
            f"Traffic simulator initialized: {grid_rows}x{grid_cols} grid, "
            f"density={density}"
        )

    def tick_simulation(self) -> list[dict]:
        """
        Advance simulation by one tick.
        Returns list of traffic events for all intersections.
        """
        self.tick += 1
        events = []

        for iid, intersection in self.intersections.items():
            # Simulate vehicle count changes (random walk with mean reversion)
            min_v, max_v = self.density_ranges[self.density]
            target = (min_v + max_v) // 2

            # Mean-reverting random walk
            delta = random.gauss(0, 2)
            reversion = (target - intersection.vehicle_count) * 0.1
            new_count = max(0, int(intersection.vehicle_count + delta + reversion))

            # Add time-of-day variation (sinusoidal pattern)
            hour_factor = math.sin(self.tick * 0.01) * 0.3 + 1.0
            new_count = max(0, int(new_count * hour_factor))

            intersection.vehicle_count = min(new_count, 50)

            # Calculate congestion score (normalized 0-1)
            max_capacity = 40
            congestion = min(1.0, intersection.vehicle_count / max_capacity)

            # Add some noise
            congestion = max(0.0, min(1.0, congestion + random.gauss(0, 0.05)))
            intersection.congestion_score = round(congestion, 3)

            # Check for anomalies (rare events)
            intersection.has_anomaly = False
            intersection.anomaly_type = None
            if random.random() < 0.005:  # 0.5% chance per tick
                anomaly_types = ["accident", "breakdown", "road_block", "construction"]
                intersection.has_anomaly = True
                intersection.anomaly_type = random.choice(anomaly_types)
                # Anomalies cause congestion spike
                intersection.congestion_score = min(1.0, intersection.congestion_score + 0.3)

            events.append({
                "intersection_id": iid,
                "vehicle_count": intersection.vehicle_count,
                "congestion_score": intersection.congestion_score,
                "anomaly_detected": intersection.has_anomaly,
                "anomaly_type": intersection.anomaly_type,
            })

        return events

    def generate_frame(self, width: int = 640, height: int = 480) -> np.ndarray:
        """
        Generate a synthetic traffic camera frame.
        Creates a visual representation of an intersection with vehicles.
        """
        # Create dark road background
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        frame[:] = (40, 40, 40)  # Dark asphalt

        # Draw road markings
        center_x, center_y = width // 2, height // 2

        # Horizontal road
        frame[center_y - 40:center_y + 40, :] = (60, 60, 60)
        # Vertical road
        frame[:, center_x - 40:center_x + 40] = (60, 60, 60)

        # Dashed center lines
        for i in range(0, width, 30):
            frame[center_y - 1:center_y + 1, i:i + 15] = (200, 200, 200)
        for i in range(0, height, 30):
            frame[i:i + 15, center_x - 1:center_x + 1] = (200, 200, 200)

        # Draw random vehicles
        random_int = random.choice(list(self.intersections.values()))
        for _ in range(random_int.vehicle_count):
            vx = random.randint(20, width - 20)
            vy = random.randint(20, height - 20)
            # Cars as colored rectangles
            color = random.choice([
                (200, 50, 50),   # Red
                (50, 50, 200),   # Blue
                (200, 200, 50),  # Yellow
                (50, 200, 50),   # Green
                (200, 200, 200), # White
            ])
            frame[vy - 5:vy + 5, vx - 8:vx + 8] = color

        return frame
