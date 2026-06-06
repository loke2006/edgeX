"""
EdgeCloudX Edge Node — Configuration
"""

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Node identity
    edge_node_id: str = "edge-node-01"
    intersection_count: int = 16
    grid_rows: int = 4
    grid_cols: int = 4

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    traffic_topic: str = "traffic-density"
    ev_topic: str = "ev-telemetry"
    emergency_topic: str = "emergency-alerts"
    anomaly_topic: str = "anomaly-events"
    health_topic: str = "node-health"

    # Simulation
    event_interval_ms: int = 1000
    vehicle_density: str = "medium"  # low, medium, high
    enable_yolo: bool = False  # Disable YOLO by default (use simulation)
    ev_count: int = 4
    emergency_probability: float = 0.02  # 2% chance per tick

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
