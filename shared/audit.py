"""
EdgeCloudX Shared — Audit Logging
====================================
Records important system actions for compliance and debugging.

Actions logged:
  - signal_changed, emergency_activated, emergency_resolved
  - user_login, user_registered, role_changed
  - node_disconnected, node_reconnected
  - config_changed

Usage:
    from shared.audit import AuditLogger

    audit = AuditLogger(db_url="postgresql+asyncpg://...")
    await audit.init()
    await audit.log("signal_changed", actor="system", resource="int-0-0",
                     details={"from": "red", "to": "green"}, trace_id="...")
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import Column, DateTime, Integer, String, Text, select, desc
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


class AuditBase(DeclarativeBase):
    pass


class AuditLog(AuditBase):
    """Persistent audit log entry."""

    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    action = Column(String(100), nullable=False, index=True)
    actor = Column(String(200), nullable=False, index=True)  # username or "system"
    resource = Column(String(200), nullable=True)  # e.g. intersection_id, user_id
    details = Column(Text, nullable=True)  # JSON string
    trace_id = Column(String(64), nullable=True, index=True)
    service = Column(String(100), nullable=True)


class AuditLogger:
    """Records important actions to the audit_logs table."""

    def __init__(self, db_url: str):
        self._engine = create_async_engine(db_url, pool_size=3, echo=False)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    async def init(self) -> None:
        """Create the audit_logs table if it doesn't exist."""
        async with self._engine.begin() as conn:
            await conn.run_sync(AuditBase.metadata.create_all)
        logger.info("Audit log table initialized")

    async def log(
        self,
        action: str,
        *,
        actor: str = "system",
        resource: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        trace_id: Optional[str] = None,
        service: Optional[str] = None,
    ) -> None:
        """Record an audit log entry."""
        import json

        try:
            async with self._session_factory() as session:
                entry = AuditLog(
                    action=action,
                    actor=actor,
                    resource=resource,
                    details=json.dumps(details, default=str) if details else None,
                    trace_id=trace_id,
                    service=service,
                )
                session.add(entry)
                await session.commit()
                logger.debug(f"Audit: {action} by {actor} on {resource}")
        except Exception as e:
            # Audit logging should never crash the service
            logger.error(f"Failed to write audit log: {e}")

    async def query(
        self,
        *,
        action: Optional[str] = None,
        actor: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Query audit logs with optional filters."""
        try:
            async with self._session_factory() as session:
                stmt = select(AuditLog).order_by(desc(AuditLog.timestamp))

                if action:
                    stmt = stmt.where(AuditLog.action == action)
                if actor:
                    stmt = stmt.where(AuditLog.actor == actor)
                if since:
                    stmt = stmt.where(AuditLog.timestamp >= since)

                stmt = stmt.limit(limit)
                result = await session.execute(stmt)
                rows = result.scalars().all()

                import json
                return [
                    {
                        "id": r.id,
                        "timestamp": r.timestamp.isoformat() if r.timestamp else None,
                        "action": r.action,
                        "actor": r.actor,
                        "resource": r.resource,
                        "details": json.loads(r.details) if r.details else None,
                        "trace_id": r.trace_id,
                        "service": r.service,
                    }
                    for r in rows
                ]
        except Exception as e:
            logger.error(f"Failed to query audit logs: {e}")
            return []
