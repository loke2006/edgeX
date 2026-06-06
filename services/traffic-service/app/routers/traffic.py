"""
EdgeCloudX Traffic Service — Traffic Router
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import get_db
from app.models.traffic import CongestionLevel, Intersection, TrafficEvent
from app.schemas.traffic import (
    IntersectionSchema,
    TrafficEventSchema,
    TrafficGridSchema,
    TrafficUpdateSchema,
)
from app.services.traffic_service import TrafficService

router = APIRouter(prefix="/traffic", tags=["traffic"])


@router.get("/grid", response_model=TrafficGridSchema)
async def get_traffic_grid(db: AsyncSession = Depends(get_db)):
    """Get the full traffic grid state with all intersections."""
    service = TrafficService(db)
    return await service.get_grid_state()


@router.get("/intersection/{intersection_id}", response_model=IntersectionSchema)
async def get_intersection(
    intersection_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific intersection's current state."""
    service = TrafficService(db)
    intersection = await service.get_intersection(intersection_id)
    if not intersection:
        raise HTTPException(status_code=404, detail=f"Intersection {intersection_id} not found")
    return intersection


@router.post("/update", response_model=IntersectionSchema)
async def update_traffic(
    update: TrafficUpdateSchema,
    db: AsyncSession = Depends(get_db),
):
    """Manually update traffic data for an intersection (used by edge nodes or testing)."""
    service = TrafficService(db)
    return await service.process_traffic_update(update)


@router.get("/events", response_model=list[TrafficEventSchema])
async def get_traffic_events(
    intersection_id: Optional[str] = Query(None, description="Filter by intersection"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Get historical traffic events, optionally filtered by intersection."""
    query = select(TrafficEvent).order_by(TrafficEvent.received_at.desc())

    if intersection_id:
        query = query.where(TrafficEvent.intersection_id == intersection_id)

    query = query.limit(limit).offset(offset)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/stats")
async def get_traffic_stats(db: AsyncSession = Depends(get_db)):
    """Get aggregate traffic statistics."""
    # Total vehicles
    total_vehicles_q = await db.execute(
        select(func.sum(Intersection.vehicle_count))
    )
    total_vehicles = total_vehicles_q.scalar() or 0

    # Average congestion
    avg_congestion_q = await db.execute(
        select(func.avg(Intersection.congestion_score))
    )
    avg_congestion = round(avg_congestion_q.scalar() or 0.0, 3)

    # Congestion distribution
    congestion_dist = {}
    for level in CongestionLevel:
        count_q = await db.execute(
            select(func.count()).where(Intersection.congestion_level == level)
        )
        congestion_dist[level.value] = count_q.scalar() or 0

    # Active emergencies
    emergencies_q = await db.execute(
        select(func.count()).where(Intersection.is_emergency_active == True)  # noqa
    )
    active_emergencies = emergencies_q.scalar() or 0

    return {
        "total_vehicles": total_vehicles,
        "avg_congestion_score": avg_congestion,
        "congestion_distribution": congestion_dist,
        "active_emergencies": active_emergencies,
        "timestamp": datetime.utcnow().isoformat(),
    }
