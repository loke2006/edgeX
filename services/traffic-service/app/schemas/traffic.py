"""
EdgeCloudX Traffic Service — Pydantic Schemas (validations for input data)
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class IntersectionSchema(BaseModel):
    """Schema for intersection data."""

    intersection_id: str
    grid_row: int
    grid_col: int
    name: Optional[str] = None
    signal_state: str = "red"
    vehicle_count: int = 0
    congestion_level: str = "low"
    congestion_score: float = 0.0
    is_emergency_active: bool = False
    last_updated: Optional[datetime] = None

    class Config:
        from_attributes = True


class TrafficUpdateSchema(BaseModel):
    """Schema for incoming traffic updates from edge nodes."""

    intersection_id: str = Field(..., description="Intersection identifier")
    edge_node_id: str = Field(..., description="Source edge node ID")
    vehicle_count: int = Field(ge=0, description="Number of vehicles detected")
    congestion_score: float = Field(ge=0.0, le=1.0, description="Congestion score (0-1)")
    anomaly_detected: bool = False
    anomaly_type: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TrafficGridSchema(BaseModel):
    """Schema for the full traffic grid state."""

    grid_rows: int
    grid_cols: int
    intersections: list[IntersectionSchema]
    total_vehicles: int
    avg_congestion: float
    active_emergencies: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TrafficEventSchema(BaseModel):
    """Schema for historical traffic events."""

    intersection_id: str
    edge_node_id: str
    vehicle_count: int
    congestion_score: float
    congestion_level: str
    anomaly_detected: bool
    anomaly_type: Optional[str] = None
    event_timestamp: datetime
    received_at: datetime

    class Config:
        from_attributes = True


class HealthResponse(BaseModel):
    """Health check response."""

    service: str
    status: str
    version: str = "0.2.0"
    kafka_connected: bool = False
    redis_connected: bool = False
    db_connected: bool = False
    timestamp: datetime = Field(default_factory=datetime.utcnow)
