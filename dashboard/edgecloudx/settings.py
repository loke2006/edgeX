"""
EdgeCloudX Dashboard — Django Settings
=========================================
Django Channels configuration with Redis channel layer for
real-time WebSocket communication.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY", "dev-secret-key-dashboard-change-in-production"
)

DEBUG = os.environ.get("DJANGO_DEBUG", "True").lower() in ("true", "1")

ALLOWED_HOSTS = ["*"]

# Application definition
INSTALLED_APPS = [
    "daphne",
    "django.contrib.staticfiles",
    "channels",
    "traffic",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "edgecloudx.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.template.context_processors.static",
            ],
        },
    },
]

# ASGI application (for Django Channels)
ASGI_APPLICATION = "edgecloudx.asgi.application"

# Channel Layers — Redis backend
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [REDIS_URL],
            "capacity": 1500,
            "expiry": 10,
        },
    },
}

# Static files
STATIC_URL = "/static/"
STATICFILES_DIRS = [
    BASE_DIR / "static",
]
STATIC_ROOT = BASE_DIR / "staticfiles"

STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedStaticFilesStorage",
    },
}

# Backend microservice URLs
TRAFFIC_SERVICE_URL = os.environ.get(
    "TRAFFIC_SERVICE_URL", "http://traffic-service:8000"
)
ROUTING_SERVICE_URL = os.environ.get(
    "ROUTING_SERVICE_URL", "http://routing-service:8000"
)
ANALYTICS_SERVICE_URL = os.environ.get(
    "ANALYTICS_SERVICE_URL", "http://analytics-service:8000"
)
ALERT_SERVICE_URL = os.environ.get(
    "ALERT_SERVICE_URL", "http://alert-service:8000"
)
AUTH_SERVICE_URL = os.environ.get(
    "AUTH_SERVICE_URL", "http://auth-service:8000"
)

# Logging
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
