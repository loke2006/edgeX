"""
EdgeCloudX Shared — OpenTelemetry Instrumentation
====================================================
Distributed tracing with automatic instrumentation for FastAPI, Redis,
SQLAlchemy, and aiokafka. Exports traces to Jaeger.

Usage:
    from shared.telemetry import init_telemetry

    # Call once during service startup
    init_telemetry("traffic-service")
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def init_telemetry(service_name: str) -> Optional[object]:
    """
    Initialize OpenTelemetry with Jaeger exporter and auto-instrumentation.

    Returns the tracer provider, or None if OTel packages are not installed.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.info(
            "OpenTelemetry packages not installed, tracing disabled. "
            "Install: opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc"
        )
        return None

    # Jaeger/OTel collector endpoint
    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")

    resource = Resource.create({
        "service.name": service_name,
        "service.version": "0.2.0",
        "deployment.environment": os.environ.get("ENVIRONMENT", "development"),
    })

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    logger.info(
        f"OpenTelemetry initialized for {service_name} → {otlp_endpoint}"
    )

    # Auto-instrument frameworks (best effort — skip if not installed)
    _auto_instrument_fastapi()
    _auto_instrument_redis()
    _auto_instrument_sqlalchemy()
    _auto_instrument_httpx()

    return provider


def _auto_instrument_fastapi():
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentator
        FastAPIInstrumentator().instrument()
        logger.debug("OpenTelemetry: FastAPI auto-instrumented")
    except ImportError:
        pass


def _auto_instrument_redis():
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentator
        RedisInstrumentator().instrument()
        logger.debug("OpenTelemetry: Redis auto-instrumented")
    except ImportError:
        pass


def _auto_instrument_sqlalchemy():
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentator
        SQLAlchemyInstrumentor = SQLAlchemyInstrumentator
        SQLAlchemyInstrumentor().instrument()
        logger.debug("OpenTelemetry: SQLAlchemy auto-instrumented")
    except ImportError:
        pass


def _auto_instrument_httpx():
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
        logger.debug("OpenTelemetry: httpx auto-instrumented")
    except ImportError:
        pass
