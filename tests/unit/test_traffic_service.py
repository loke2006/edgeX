"""
EdgeCloudX — Unit Tests: Traffic Service
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import sys
import os

# Add traffic-service to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "traffic-service"))


class TestTrafficSchemas:
    """Test Pydantic schemas for traffic service."""

    def test_traffic_update_schema_valid(self):
        from app.schemas.traffic import TrafficUpdateSchema

        update = TrafficUpdateSchema(
            intersection_id="int-0-0",
            edge_node_id="edge-node-01",
            vehicle_count=15,
            congestion_score=0.6,
        )
        assert update.intersection_id == "int-0-0"
        assert update.vehicle_count == 15
        assert update.congestion_score == 0.6
        assert update.anomaly_detected is False

    def test_traffic_update_schema_with_anomaly(self):
        from app.schemas.traffic import TrafficUpdateSchema

        update = TrafficUpdateSchema(
            intersection_id="int-1-2",
            edge_node_id="edge-node-01",
            vehicle_count=25,
            congestion_score=0.9,
            anomaly_detected=True,
            anomaly_type="accident",
        )
        assert update.anomaly_detected is True
        assert update.anomaly_type == "accident"

    def test_traffic_update_schema_invalid_congestion(self):
        from app.schemas.traffic import TrafficUpdateSchema

        with pytest.raises(Exception):
            TrafficUpdateSchema(
                intersection_id="int-0-0",
                edge_node_id="edge-node-01",
                vehicle_count=-1,  # Invalid
                congestion_score=0.5,
            )

    def test_health_response_schema(self):
        from app.schemas.traffic import HealthResponse

        health = HealthResponse(
            service="traffic-service",
            status="healthy",
        )
        assert health.service == "traffic-service"
        assert health.version == "0.1.0"


class TestCongestionLevel:
    """Test congestion level calculation."""

    def test_congestion_levels(self):
        from app.services.traffic_service import _calculate_congestion_level
        from app.models.traffic import CongestionLevel

        assert _calculate_congestion_level(0.1) == CongestionLevel.LOW
        assert _calculate_congestion_level(0.3) == CongestionLevel.MODERATE
        assert _calculate_congestion_level(0.6) == CongestionLevel.HIGH
        assert _calculate_congestion_level(0.9) == CongestionLevel.CRITICAL

    def test_congestion_boundaries(self):
        from app.services.traffic_service import _calculate_congestion_level
        from app.models.traffic import CongestionLevel

        assert _calculate_congestion_level(0.0) == CongestionLevel.LOW
        assert _calculate_congestion_level(0.24) == CongestionLevel.LOW
        assert _calculate_congestion_level(0.25) == CongestionLevel.MODERATE
        assert _calculate_congestion_level(0.49) == CongestionLevel.MODERATE
        assert _calculate_congestion_level(0.5) == CongestionLevel.HIGH
        assert _calculate_congestion_level(0.74) == CongestionLevel.HIGH
        assert _calculate_congestion_level(0.75) == CongestionLevel.CRITICAL
        assert _calculate_congestion_level(1.0) == CongestionLevel.CRITICAL
