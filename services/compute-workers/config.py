"""
EdgeCloudX Compute Workers — Configuration
=============================================
Pydantic settings for Ray workers, Kafka consumers, and Redis integration.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Service configuration loaded from environment variables."""

    # Service
    service_name: str = "compute-workers"
    debug: bool = False

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_traffic_topic: str = "traffic-density"
    kafka_ev_topic: str = "ev-telemetry"
    kafka_emergency_topic: str = "emergency-alerts"
    kafka_consumer_group: str = "compute-workers-group"

    # Redis
    redis_url: str = "redis://redis:6379/0"
    redis_intersection_prefix: str = "intersection:"
    redis_compute_prefix: str = "compute:"

    # Grid
    grid_rows: int = 4
    grid_cols: int = 4

    # Ray
    ray_num_cpus: int = 2

    # Workers
    heatmap_interval_seconds: float = 5.0
    prediction_interval_seconds: float = 10.0
    route_optimization_interval_seconds: float = 8.0
    congestion_ema_alpha: float = 0.3

    # API
    api_port: int = 8000

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
