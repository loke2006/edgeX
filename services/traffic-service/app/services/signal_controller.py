"""
EdgeCloudX Traffic Service — Adaptive Signal Controller
=========================================================
Dynamically adjusts traffic signals based on real-time congestion data.

Algorithm:
  - Critical congestion → extend green phase
  - Emergency active → force green on corridor
  - Low congestion → standard red cycling
  - Moderate → yellow transition window

Runs as a periodic background task (every 5 seconds).
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis

from shared.audit import AuditLogger
from shared.metrics import SIGNAL_CHANGES_TOTAL, CONGESTION_SCORE

logger = logging.getLogger(__name__)


class AdaptiveSignalController:
    """Periodically adjusts traffic signals based on congestion."""

    def __init__(
        self,
        redis_url: str,
        db_url: str,
        audit: Optional[AuditLogger] = None,
        cycle_seconds: float = 5.0,
        grid_rows: int = 4,
        grid_cols: int = 4,
    ):
        self.redis_url = redis_url
        self.db_url = db_url
        self.audit = audit
        self.cycle_seconds = cycle_seconds
        self.grid_rows = grid_rows
        self.grid_cols = grid_cols
        self._running = False

    async def run(self) -> None:
        """Main control loop."""
        self._running = True
        logger.info("Adaptive signal controller started")

        while self._running:
            try:
                await self._control_cycle()
            except Exception as e:
                logger.error(f"Signal control cycle error: {e}", exc_info=True)
            await asyncio.sleep(self.cycle_seconds)

    async def stop(self) -> None:
        self._running = False
        logger.info("Adaptive signal controller stopped")

    async def _control_cycle(self) -> None:
        """Single control cycle — read congestion, compute signals, update."""
        r = aioredis.from_url(self.redis_url, decode_responses=True)

        try:
            changes = []

            for row in range(self.grid_rows):
                for col in range(self.grid_cols):
                    iid = f"int-{row}-{col}"
                    key = f"intersection:{iid}"
                    data = await r.hgetall(key)

                    if not data:
                        continue

                    score = float(data.get("congestion_score", "0.0"))
                    current_signal = data.get("signal_state", "red")
                    is_emergency = data.get("is_emergency_active", "False") == "True"

                    # Update Prometheus gauge
                    CONGESTION_SCORE.labels(intersection_id=iid).set(score)

                    # Determine new signal state
                    new_signal = self._compute_signal(
                        score, current_signal, is_emergency
                    )

                    if new_signal != current_signal:
                        # Update Redis
                        await r.hset(key, "signal_state", new_signal)
                        changes.append({
                            "intersection_id": iid,
                            "from": current_signal,
                            "to": new_signal,
                            "congestion_score": score,
                            "is_emergency": is_emergency,
                        })

                        # Prometheus counter
                        SIGNAL_CHANGES_TOTAL.labels(
                            intersection_id=iid,
                            from_state=current_signal,
                            to_state=new_signal,
                        ).inc()

            # Publish batch signal changes to Redis for dashboard
            if changes:
                await r.publish("traffic:signals", json.dumps({
                    "type": "signal_update",
                    "changes": changes,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }))

                # Audit log significant changes
                if self.audit:
                    for change in changes:
                        if change["is_emergency"] or change["to"] == "green":
                            await self.audit.log(
                                "signal_changed",
                                actor="adaptive_controller",
                                resource=change["intersection_id"],
                                details=change,
                                service="traffic-service",
                            )

                logger.info(
                    "Signal cycle complete",
                    extra={"changes": len(changes)},
                )

        finally:
            await r.aclose()

    @staticmethod
    def _compute_signal(
        congestion_score: float,
        current: str,
        is_emergency: bool,
    ) -> str:
        """Determine the optimal signal state."""
        # Emergency override — always green
        if is_emergency:
            return "green"

        # Adaptive logic based on congestion
        if congestion_score >= 0.75:
            # Critical — keep/switch to green to flush traffic
            return "green"
        elif congestion_score >= 0.5:
            # High — transition to yellow (caution)
            return "yellow"
        elif congestion_score >= 0.25:
            # Moderate — cycle to red (controlled flow)
            if current == "green":
                return "yellow"
            return "red"
        else:
            # Low — standard red
            return "red"
