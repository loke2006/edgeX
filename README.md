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
           │
           ▼
┌─────────────────────────────┐
│       Apache Kafka Bus      │    Event Streaming
│  traffic-density │ ev-telem │    (KRaft Mode)
│  emergency-alerts│ anomaly  │
└──────────┬──────────────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌──────────┐
│  Ray    │ │ FastAPI  │    Compute + API Layer
│ Cluster │ │ Services │
└────┬────┘ └────┬─────┘
     │           │
     ▼           ▼
┌─────────┐ ┌──────────┐
│  Redis  │ │ Postgres │    State + Storage
└────┬────┘ └──────────┘
     │
     ▼
┌────────────────────────┐
│  Django Dashboard      │    Real-Time UI
│  (Channels/WebSocket)  │    (WebSocket)
└────────────────────────┘
```

## Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| Edge AI | PyTorch, YOLOv8, OpenCV | Local vehicle/anomaly detection |
| Streaming | Apache Kafka (KRaft) | Event-driven message bus |
| Compute | Ray | Distributed parallel processing |
| Services | FastAPI | Async microservices |
| Dashboard | Django Channels | Real-time WebSocket UI |
| State | Redis | Sub-second caching & Pub/Sub |
| Storage | PostgreSQL | Historical analytics data |
| ML | Flower | Federated learning |
| Infra | Docker, Kubernetes | Container orchestration |
| Monitoring | Prometheus, Grafana, Loki | Observability stack |

## Quick Start

### Prerequisites
- Docker Desktop with WSL2 backend
- Python 3.11+
- 16GB RAM minimum

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
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Traffic Service | 8001 | Traffic data ingestion & grid state |
| Routing Service | 8002 | EV pathfinding & route optimization |
| Analytics Service | 8003 | Congestion analytics & history |
| Alert Service | 8004 | Emergency alert management |
| Auth Service | 8005 | JWT authentication |
| Dashboard | 8000 | Real-time operations dashboard |
| Kafka | 9092/9094 | Event streaming (internal/external) |
| Redis | 6379 | State cache & Pub/Sub |
| PostgreSQL | 5432 | Persistent storage |

## Development Phases

- [x] **Phase 1** — Core Infrastructure (Kafka, Redis, Postgres, FastAPI)
- [ ] **Phase 2** — Edge AI Nodes (YOLOv8, OpenCV, Kafka Producers)
- [ ] **Phase 3** — Distributed Compute (Ray Cluster)
- [ ] **Phase 4** — Real-Time Dashboard (Django Channels)
- [ ] **Phase 5** — Cloud-Native (Kubernetes)
- [ ] **Phase 6** — Observability (Prometheus, Grafana, Loki)
- [ ] **Phase 7** — Federated Learning (Flower)
- [ ] **Phase 8** — Chaos Engineering

## License

MIT
