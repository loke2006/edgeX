"""
EdgeCloudX Routing Service — Configuration
"""

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "routing-service"
    debug: bool = False
    kafka_bootstrap_servers: str = "kafka:9092"
    redis_url: str = "redis://redis:6379/0"
    redis_intersection_prefix: str = "intersection:"
    grid_rows: int = 4
    grid_cols: int = 4

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
