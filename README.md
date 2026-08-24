<p align="center">
  <h1 align="center">⚡ EdgeCloudX</h1>
  <p align="center"><strong>Autonomous Smart City Traffic & Navigation Grid</strong></p>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.11+-blue?style=flat-square&logo=python" />
  <img src="https://img.shields.io/badge/kafka-KRaft-orange?style=flat-square&logo=apachekafka" />
  <img src="https://img.shields.io/badge/docker-compose-2496ED?style=flat-square&logo=docker" />
  <img src="https://img.shields.io/badge/kubernetes-ready-326CE5?style=flat-square&logo=kubernetes" />
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" />
</p>

---

## Overview

EdgeCloudX is a large-scale, distributed smart-city infrastructure platform designed to manage autonomous EV navigation, dynamic traffic signaling, and emergency response. The system bridges ultra-low-latency **edge computing** with heavy **cloud-based analytics** using an event-driven microservices architecture.

## Architecture

```
┌──────────────────────┐
│   Traffic Cameras    │    Edge Layer
│  Simulated Edge AI   │    (YOLOv8 + OpenCV)
└──────────┬───────────┘
           │  trace_id + event_id
           ▼
┌─────────────────────────────────┐
│        Apache Kafka Bus         │    Event Streaming
│  traffic-density │ ev-telemetry │    (KRaft Mode)
│  emergency-alerts│ anomaly │ DLQ│
└──────────┬──────────────────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
┌──────────┐ ┌──────────┐
│   Ray    │ │ FastAPI  │    Compute + API Layer
│  Cluster │ │ Services │    (RBAC + Audit)
└────┬─────┘ └────┬─────┘
     │            │
     ▼            ▼
┌─────────┐ ┌──────────┐
│  Redis  │ │ Postgres │    State + Storage
└────┬────┘ └────┬─────┘
     │           │
     ▼           ▼
┌────────────┐ ┌──────────────────┐
│  Dashboard │ │  Spark Streaming │  Analytics
│ (WebSocket)│ │ (Rolling Aggs)   │
└────────────┘ └──────────────────┘
           ↓
┌────────────────────────────────┐
│   Observability Stack          │
│  Prometheus + Grafana + Jaeger │
│  + Loki (Structured JSON Logs) │
└────────────────────────────────┘
```

## Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| Edge AI | PyTorch, YOLOv8, OpenCV | Local vehicle/anomaly detection |
| Streaming | Apache Kafka (KRaft) | Event-driven message bus + DLQ |
| Compute | Ray | Distributed parallel processing |
| Services | FastAPI | Async microservices with RBAC |
| Dashboard | Django Channels | Real-time WebSocket UI |
| State | Redis | Sub-second caching & Pub/Sub |
| Storage | PostgreSQL | Historical analytics data |
| Analytics | PySpark Structured Streaming | Rolling aggregations & trends |
| Auth | JWT + RBAC | Role-based access control |
| Tracing | OpenTelemetry + Jaeger | Distributed trace correlation |
| Monitoring | Prometheus, Grafana, Loki | Metrics, dashboards, log aggregation |
| Infra | Docker, Kubernetes | Container orchestration |

## Features

### Observability
- **Correlation IDs** — Every event carries `trace_id` + `event_id` from edge → Kafka → service → Redis → dashboard
- **Structured JSON Logging** — All services emit machine-parseable JSON logs with trace context
- **Prometheus Custom Metrics** — Business-level counters, gauges, and histograms (congestion, latency, DLQ)
- **Node Heartbeats** — Edge nodes report CPU, memory, FPS, uptime; classified as healthy/degraded/dead

### Resilience
- **Kafka Dead Letter Queue** — Failed messages retry 3x with backoff, then land in `{topic}-dlq`
- **Adaptive Traffic Signals** — Congestion-aware signal controller (critical→green, emergency→force green corridor)

### Analytics
- **Historical Aggregation** — Hourly/daily congestion stats computed in background, stored in PostgreSQL
- **Spark Streaming** — 5-min/15-min rolling averages, peak intersection detection, trend analysis
- **Trend Detection** — Compare recent vs previous period to classify intersections as improving/worsening/stable

### Security
- **RBAC** — Roles: `admin`, `operator`, `viewer`, `edge_node` with permission matrix
- **Audit Logs** — Signal changes, emergency events, user management actions logged to PostgreSQL
- **Security Headers** — `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`, etc.
- **JWT Refresh Tokens** — Separate access/refresh tokens with configurable expiration
- **Non-Root Containers** — All Docker images run as `appuser`

## Quick Start

### Prerequisites
- Docker Desktop with WSL2 backend
- Python 3.11+
- 16GB RAM minimum (20GB recommended with Spark)

### Launch

```bash
# Clone and navigate
cd edgeX

# Start infrastructure (Kafka, Redis, PostgreSQL)
docker compose up -d kafka redis postgres kafka-init

# Start all microservices
docker compose up -d

# Check health
curl http://localhost:8001/health  # Traffic Service
curl http://localhost:8002/health  # Routing Service
curl http://localhost:8003/health  # Analytics Service
curl http://localhost:8004/health  # Alert Service
curl http://localhost:8005/health  # Auth Service
curl http://localhost:8006/health  # Compute Workers
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Traffic Service | 8001 | Traffic data ingestion, adaptive signals, node monitoring |
| Routing Service | 8002 | EV pathfinding & route optimization |
| Analytics Service | 8003 | Historical analytics, trends, peak hours, audit logs |
| Alert Service | 8004 | Emergency alert management (RBAC-protected) |
| Auth Service | 8005 | JWT authentication & RBAC user management |
| Compute Workers | 8006 | Ray-based distributed compute (congestion, heatmap, prediction) |
| Spark Analytics | — | PySpark Structured Streaming (rolling averages → Redis/Postgres) |
| Dashboard | 8000 | Real-time operations dashboard (WebSocket) |
| Kafka | 9092/9094 | Event streaming (internal/external) |
| Redis | 6379 | State cache & Pub/Sub |
| PostgreSQL | 5432 | Persistent storage & analytics |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Dashboards & visualization |
| Jaeger | 16686 | Distributed tracing UI |
| Loki | 3100 | Log aggregation |

## Shared Module (`shared/`)

Cross-cutting concerns shared by all microservices:

| Module | Purpose |
|--------|---------|
| `trace.py` | Trace ID generation, TraceContext propagation |
| `logging.py` | Structured JSON logging with trace correlation |
| `metrics.py` | Custom Prometheus metric definitions |
| `dlq.py` | Dead Letter Queue publisher + retry helper |
| `middleware.py` | RBAC middleware + security headers |
| `audit.py` | Audit logging to PostgreSQL |
| `telemetry.py` | OpenTelemetry + Jaeger auto-instrumentation |

## Development Phases

- [x] **Phase 1** — Core Infrastructure (Kafka, Redis, Postgres, FastAPI)
- [x] **Phase 2** — Edge AI Nodes (YOLOv8, OpenCV, Kafka Producers)
- [x] **Phase 3** — Distributed Compute (Ray Cluster)
  - Congestion analysis, route optimization, heatmap generation
  - Spark Structured Streaming (rolling averages, hourly aggregations)
  - Historical analytics (trends, peak hours)
- [x] **Phase 4** — Real-Time Dashboard (Django Channels)
  - Live traffic grid, heatmap, alerts, EV tracking
  - Node health monitoring, adaptive signal status
- [x] **Phase 5** — Cloud-Native (Kubernetes)
  - Kustomize manifests for all services (base + dev/prod overlays)
  - StatefulSets for Kafka (KRaft) and PostgreSQL
  - HPAs for traffic-service and compute-workers
  - ConfigMap/Secret for K8s-native config management
  - Health probes (liveness + readiness) on all services
  - Deploy script (`infra/k8s/deploy.bat`)
- [x] **Phase 6** — Observability (Prometheus, Grafana, Loki)
  - Structured JSON logging with trace correlation
  - Correlation / Trace IDs (trace_id + event_id end-to-end)
  - Node heartbeats (CPU, memory, FPS, uptime)
  - Custom Prometheus metrics (congestion, latency, DLQ)
  - Dead Letter Queue with retry + backoff
  - OpenTelemetry + Jaeger distributed tracing
  - RBAC (admin, operator, viewer, edge_node)
  - Audit logging, security headers, JWT refresh
- [ ] **Phase 7** — Federated Learning (Flower)
- [ ] **Phase 8** — Chaos Engineering

## API Endpoints

### Auth Service
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/auth/register` | Public | Register new user |
| POST | `/auth/login` | Public | Get JWT tokens |
| POST | `/auth/refresh` | Auth | Refresh access token |
| GET | `/auth/verify` | Auth | Verify token & get user info |
| GET | `/auth/users` | Admin | List all users |
| PUT | `/auth/users/{id}/role` | Admin | Change user role |

### Alert Service
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/alerts/emergency` | Operator+ | Create manual alert |
| GET | `/alerts/active` | Viewer+ | List active alerts |
| POST | `/alerts/{id}/resolve` | Operator+ | Resolve an alert |

### Analytics Service
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/analytics/congestion` | Real-time congestion report |
| GET | `/analytics/heatmap` | Congestion heatmap matrix |
| GET | `/analytics/history/hourly` | Hourly historical data |
| GET | `/analytics/history/daily` | Daily historical data |
| GET | `/analytics/trends` | Congestion trend analysis |
| GET | `/analytics/peak-hours` | Busiest hours of the day |
| GET | `/audit/logs` | Query audit logs |

## License

MIT
