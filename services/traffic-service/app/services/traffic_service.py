"""
EdgeCloudX Traffic Service — Business Logic
==============================================
Enhanced with trace ID propagation, Prometheus metrics, and Redis latency tracking.
"""

import json
import logging
import time
from datetime import datetime
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models.traffic import CongestionLevel, Intersection, SignalState, TrafficEvent
from app.schemas.traffic import IntersectionSchema, TrafficGridSchema, TrafficUpdateSchema
from shared.metrics import EVENTS_TOTAL, DB_LATENCY, REDIS_LATENCY

logger = logging.getLogger(__name__)
settings = get_settings()


def _calculate_congestion_level(score: float) -> CongestionLevel:
    """Determine congestion level from a 0-1 score."""
    if score < 0.25:
        return CongestionLevel.LOW
    elif score < 0.5:
        return CongestionLevel.MODERATE
    elif score < 0.75:
        return CongestionLevel.HIGH
    else:
        return CongestionLevel.CRITICAL


class TrafficService:
    """Core business logic for traffic data processing."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_grid_state(self) -> TrafficGridSchema:
        """Get the full traffic grid state."""
        result = await self.db.execute(
            select(Intersection).order_by(Intersection.grid_row, Intersection.grid_col)
        )
        intersections = result.scalars().all()

        # If no intersections exist yet, seed the grid
        if not intersections:
            intersections = await self._seed_grid()

        total_vehicles = sum(i.vehicle_count for i in intersections)
        avg_congestion = (
            sum(i.congestion_score for i in intersections) / len(intersections)
            if intersections
            else 0.0
        )
        active_emergencies = sum(1 for i in intersections if i.is_emergency_active)

        return TrafficGridSchema(
            grid_rows=settings.grid_rows,
            grid_cols=settings.grid_cols,
            intersections=[IntersectionSchema.model_validate(i) for i in intersections],
            total_vehicles=total_vehicles,
            avg_congestion=round(avg_congestion, 3),
            active_emergencies=active_emergencies,
        )

    async def get_intersection(self, intersection_id: str) -> Optional[Intersection]:
        """Get a specific intersection by ID."""
        result = await self.db.execute(
            select(Intersection).where(Intersection.intersection_id == intersection_id)
        )
        return result.scalar_one_or_none()

    async def process_traffic_update(
        self,
        update: TrafficUpdateSchema,
        trace_id: str = "",
        event_id: str = "",
    ) -> Intersection:
        """Process an incoming traffic update from an edge node."""
        db_start = time.time()

        # Find or create intersection
        intersection = await self.get_intersection(update.intersection_id)
        if not intersection:
            # Auto-create intersection based on ID pattern (e.g., "int-0-0")
            parts = update.intersection_id.split("-")
            row = int(parts[1]) if len(parts) >= 3 else 0
            col = int(parts[2]) if len(parts) >= 3 else 0
            intersection = Intersection(
                intersection_id=update.intersection_id,
                grid_row=row,
                grid_col=col,
                name=f"Intersection {row},{col}",
            )
            self.db.add(intersection)

        # Update intersection state
        congestion_level = _calculate_congestion_level(update.congestion_score)
        intersection.vehicle_count = update.vehicle_count
        intersection.congestion_score = update.congestion_score
        intersection.congestion_level = congestion_level
        intersection.last_updated = datetime.utcnow()

        # Record historical event with trace context
        event = TrafficEvent(
            intersection_id=update.intersection_id,
            edge_node_id=update.edge_node_id,
            trace_id=trace_id or None,
            event_id=event_id or None,
            vehicle_count=update.vehicle_count,
            congestion_score=update.congestion_score,
            congestion_level=congestion_level,
            anomaly_detected=update.anomaly_detected,
            anomaly_type=update.anomaly_type,
            event_timestamp=update.timestamp,
        )
        self.db.add(event)
        await self.db.flush()

        # Track DB latency
        DB_LATENCY.labels(service="traffic-service", operation="process_update").observe(
            time.time() - db_start
        )

        # Publish to Redis for real-time dashboard
        await self._publish_to_redis(intersection, trace_id=trace_id)

        logger.info(
            "Processed traffic update",
            extra={
                "trace_id": trace_id,
                "intersection": update.intersection_id,
                "vehicles": update.vehicle_count,
                "congestion": round(update.congestion_score, 2),
            },
        )

        return intersection

    async def _publish_to_redis(self, intersection: Intersection, trace_id: str = "") -> None:
        """Publish intersection state update to Redis Pub/Sub."""
        redis_start = time.time()
        try:
            r = aioredis.from_url(settings.redis_url)

            # Store current state in Redis hash
            key = f"{settings.redis_intersection_prefix}{intersection.intersection_id}"
            state = {
                "intersection_id": intersection.intersection_id,
                "grid_row": str(intersection.grid_row),
                "grid_col": str(intersection.grid_col),
                "signal_state": intersection.signal_state.value,
                "vehicle_count": str(intersection.vehicle_count),
                "congestion_score": str(intersection.congestion_score),
                "congestion_level": intersection.congestion_level.value,
                "is_emergency_active": str(intersection.is_emergency_active),
                "last_updated": datetime.utcnow().isoformat(),
            }
            await r.hset(key, mapping=state)

            # Publish to channel for WebSocket consumers
            await r.publish(
                settings.redis_traffic_channel,
                json.dumps(state),
            )

            await r.aclose()

            REDIS_LATENCY.labels(service="traffic-service", operation="publish").observe(
                time.time() - redis_start
            )
        except Exception as e:
            logger.warning(f"Failed to publish to Redis: {e}", extra={"trace_id": trace_id})

    async def _seed_grid(self) -> list[Intersection]:
        """Seed the database with initial intersection grid."""
        intersections = []
        for row in range(settings.grid_rows):
            for col in range(settings.grid_cols):
                intersection = Intersection(
                    intersection_id=f"int-{row}-{col}",
                    grid_row=row,
                    grid_col=col,
                    name=f"Intersection {row},{col}",
                    signal_state=SignalState.RED,
                    vehicle_count=0,
                    congestion_level=CongestionLevel.LOW,
                    congestion_score=0.0,
                )
                self.db.add(intersection)
                intersections.append(intersection)

        await self.db.flush()
        logger.info(
            "Grid seeded",
            extra={
                "count": len(intersections),
                "grid": f"{settings.grid_rows}x{settings.grid_cols}",
            },
        )
        return intersections
