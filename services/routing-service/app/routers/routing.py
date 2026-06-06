"""
EdgeCloudX Routing Service — Routing Router
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.pathfinder import Pathfinder

router = APIRouter(prefix="/route", tags=["routing"])
pathfinder = Pathfinder()


class RouteRequest(BaseModel):
    """Request body for route calculation."""
    start_row: int = Field(ge=0, description="Start intersection row")
    start_col: int = Field(ge=0, description="Start intersection column")
    end_row: int = Field(ge=0, description="End intersection row")
    end_col: int = Field(ge=0, description="End intersection column")
    avoid_congestion: bool = Field(True, description="Consider congestion in routing")
    ev_id: Optional[str] = Field(None, description="EV identifier for tracking")


class RouteResponse(BaseModel):
    """Response with calculated route."""
    ev_id: Optional[str] = None
    path: list[dict]
    total_cost: float
    distance: int
    total_congestion_exposure: float
    steps: int
    corridor_type: Optional[str] = None
    signal_override: Optional[str] = None
    green_intersections: Optional[list[str]] = None


@router.post("/calculate", response_model=RouteResponse)
async def calculate_route(request: RouteRequest):
    """Calculate optimal route between two intersections."""
    result = await pathfinder.find_route(
        start=(request.start_row, request.start_col),
        end=(request.end_row, request.end_col),
        avoid_congestion=request.avoid_congestion,
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail="No route found between the specified intersections",
        )

    result["ev_id"] = request.ev_id
    return RouteResponse(**result)


@router.post("/emergency-corridor", response_model=RouteResponse)
async def calculate_emergency_corridor(request: RouteRequest):
    """Calculate emergency green corridor — shortest path with signal override."""
    result = await pathfinder.find_emergency_corridor(
        start=(request.start_row, request.start_col),
        end=(request.end_row, request.end_col),
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail="No emergency corridor found",
        )

    result["ev_id"] = request.ev_id
    return RouteResponse(**result)


@router.get("/ev/{ev_id}")
async def get_ev_route(ev_id: str):
    """Get the current route for a specific EV (placeholder for Phase 2 integration)."""
    return {
        "ev_id": ev_id,
        "status": "awaiting_telemetry",
        "message": "EV route will be available after edge node integration (Phase 2)",
    }
