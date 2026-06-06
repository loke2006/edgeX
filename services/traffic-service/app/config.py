"""
EdgeCloudX Traffic Service — Configuration
"""

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Service configuration loaded from environment variables."""

    # Service
    service_name: str = "traffic-service"
    debug: bool = False

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_traffic_topic: str = "traffic-density"
    kafka_consumer_group: str = "traffic-service-group"

    # Redis
    redis_url: str = "redis://redis:6379/0"
    redis_traffic_channel: str = "traffic:updates"
    redis_intersection_prefix: str = "intersection:"

    # PostgreSQL
    database_url: str = "postgresql+asyncpg://edgecloudx:edgecloudx_secret@postgres:5432/edgecloudx"

    # Grid
    grid_rows: int = 4
    grid_cols: int = 4

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
