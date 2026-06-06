"""
EdgeCloudX Traffic Service — Database Models
"""

import enum
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Enum, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all database models."""
    pass


class SignalState(str, enum.Enum):
    RED = "red"
    GREEN = "green"
    YELLOW = "yellow"


class CongestionLevel(str, enum.Enum):
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class Intersection(Base):
    """Represents a traffic intersection in the city grid."""

    __tablename__ = "intersections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    intersection_id = Column(String(50), unique=True, nullable=False, index=True)
    grid_row = Column(Integer, nullable=False)
    grid_col = Column(Integer, nullable=False)
    name = Column(String(200), nullable=True)

    # Current state
    signal_state = Column(Enum(SignalState), default=SignalState.RED)
    vehicle_count = Column(Integer, default=0)
    congestion_level = Column(Enum(CongestionLevel), default=CongestionLevel.LOW)
    congestion_score = Column(Float, default=0.0)

    # Metadata
    is_emergency_active = Column(Boolean, default=False)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<Intersection {self.intersection_id} [{self.grid_row},{self.grid_col}]>"


class TrafficEvent(Base):
    """Historical record of a traffic event from an edge node."""

    __tablename__ = "traffic_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    intersection_id = Column(String(50), nullable=False, index=True)
    edge_node_id = Column(String(50), nullable=False)

    # Event data
    vehicle_count = Column(Integer, default=0)
    congestion_score = Column(Float, default=0.0)
    congestion_level = Column(Enum(CongestionLevel), default=CongestionLevel.LOW)
    anomaly_detected = Column(Boolean, default=False)
    anomaly_type = Column(String(100), nullable=True)

    # Timing
    event_timestamp = Column(DateTime, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<TrafficEvent {self.intersection_id} vehicles={self.vehicle_count}>"
