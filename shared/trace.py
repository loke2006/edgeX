"""
EdgeCloudX Shared — Distributed Trace Context
================================================
Generates and propagates correlation IDs across the microservice mesh.

Every event gets:
  - trace_id  : groups all events from the same origin tick / request
  - event_id  : uniquely identifies this single event
  - parent_id : (optional) the event_id of the upstream caller

Usage:
    from shared.trace import new_trace_id, new_event_id, TraceContext

    ctx = TraceContext.new("traffic-service")
    event = {**payload, **ctx.as_dict()}
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


def new_trace_id() -> str:
    """Generate a globally-unique trace ID (UUID4, hex, 32 chars)."""
    return uuid.uuid4().hex


def new_event_id() -> str:
    """Generate a globally-unique event ID (UUID4, hex, 32 chars)."""
    return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class TraceContext:
    """Immutable trace context that flows with every event."""

    trace_id: str
    event_id: str
    service_name: str
    parent_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # ── Factory helpers ──

    @classmethod
    def new(cls, service_name: str, *, trace_id: str | None = None) -> "TraceContext":
        """Create a brand-new context (root span)."""
        return cls(
            trace_id=trace_id or new_trace_id(),
            event_id=new_event_id(),
            service_name=service_name,
        )

    @classmethod
    def child(cls, parent: "TraceContext", service_name: str) -> "TraceContext":
        """Derive a child context (same trace, new event, parent link)."""
        return cls(
            trace_id=parent.trace_id,
            event_id=new_event_id(),
            service_name=service_name,
            parent_id=parent.event_id,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any], service_name: str) -> "TraceContext":
        """
        Reconstruct from an incoming Kafka message / HTTP header dict.
        If trace_id is missing, a new root context is created.
        """
        tid = data.get("trace_id")
        if not tid:
            return cls.new(service_name)
        return cls(
            trace_id=tid,
            event_id=new_event_id(),
            service_name=service_name,
            parent_id=data.get("event_id"),
        )

    # ── Serialisation ──

    def as_dict(self) -> dict[str, str]:
        """Return trace fields suitable for merging into a message payload."""
        d: dict[str, str] = {
            "trace_id": self.trace_id,
            "event_id": self.event_id,
        }
        if self.parent_id:
            d["parent_id"] = self.parent_id
        return d

    def __repr__(self) -> str:
        return f"Trace({self.trace_id[:8]}…/{self.event_id[:8]}…@{self.service_name})"
