"""
EdgeCloudX Shared — Prometheus Custom Metrics
================================================
Defines business-level Prometheus metrics shared across services.

Usage:
    from shared.metrics import EVENTS_TOTAL, KAFKA_LAG, API_LATENCY
    EVENTS_TOTAL.labels(service="traffic-service", topic="traffic-density", status="ok").inc()
"""

from prometheus_client import Counter, Gauge, Histogram, Info

# ── Counters ──

EVENTS_TOTAL = Counter(
    "edgecloudx_events_total",
    "Total events processed",
    ["service", "topic", "status"],
)

ROUTING_REQUESTS_TOTAL = Counter(
    "edgecloudx_routing_requests_total",
    "Total routing/pathfinding requests",
    ["route_type"],  # shortest, fastest, emergency
)

SIGNAL_CHANGES_TOTAL = Counter(
    "edgecloudx_signal_changes_total",
    "Total traffic signal state changes",
    ["intersection_id", "from_state", "to_state"],
)

DLQ_MESSAGES_TOTAL = Counter(
    "edgecloudx_dlq_messages_total",
    "Messages sent to dead-letter queue",
    ["original_topic", "service"],
)

# ── Gauges ──

KAFKA_LAG = Gauge(
    "edgecloudx_kafka_consumer_lag",
    "Kafka consumer lag (messages behind)",
    ["service", "topic"],
)

ACTIVE_EDGE_NODES = Gauge(
    "edgecloudx_active_edge_nodes",
    "Number of active edge nodes",
)

ACTIVE_EMERGENCIES = Gauge(
    "edgecloudx_active_emergencies",
    "Number of currently active emergency alerts",
)

CONGESTION_SCORE = Gauge(
    "edgecloudx_congestion_score",
    "Current congestion score per intersection",
    ["intersection_id"],
)

ACTIVE_EVS = Gauge(
    "edgecloudx_active_evs",
    "Number of active electric vehicles being tracked",
)

# ── Histograms ──

API_LATENCY = Histogram(
    "edgecloudx_api_latency_seconds",
    "API endpoint latency",
    ["service", "endpoint", "method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

REDIS_LATENCY = Histogram(
    "edgecloudx_redis_latency_seconds",
    "Redis operation latency",
    ["service", "operation"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5),
)

DB_LATENCY = Histogram(
    "edgecloudx_db_latency_seconds",
    "Database operation latency",
    ["service", "operation"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

KAFKA_PROCESS_LATENCY = Histogram(
    "edgecloudx_kafka_process_seconds",
    "Time to process a Kafka message batch",
    ["service", "topic"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# ── Info ──

SERVICE_INFO = Info(
    "edgecloudx_service",
    "Service metadata",
)
