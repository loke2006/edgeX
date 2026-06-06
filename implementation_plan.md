# EdgeCloudX — Autonomous Smart City Traffic & Navigation Grid

## Full Implementation Plan (From Scratch)

EdgeCloudX is a large-scale, distributed smart-city infrastructure platform for autonomous EV navigation, dynamic traffic signaling, and emergency response. It bridges ultra-low-latency edge computing with cloud-based analytics using an event-driven microservices architecture.

---

## User Review Required

> [!IMPORTANT]
> **This is a massive project.** The full system spans 8 phases with 15+ services. I recommend we build it **phase by phase**, with each phase being a working milestone. Phase 1 alone will take significant effort. Please confirm:
> 1. Should we proceed phase-by-phase (recommended) or attempt a skeleton of all phases at once?
> 2. Do you have Docker Desktop installed and running on Windows?
> 3. Do you have a preferred Python version? (I'll target Python 3.11+ for compatibility with all libraries)

> [!WARNING]
> **Hardware Requirements**: Running Kafka, Redis, PostgreSQL, Ray, and the full stack locally requires at minimum:
> - 16GB RAM (32GB recommended)
> - Docker Desktop with WSL2 backend
> - ~20GB disk space for containers and models

> [!CAUTION]
> **YOLOv8 model weights** (~25MB for `yolov8n`) will be downloaded at runtime. GPU support requires NVIDIA CUDA toolkit. CPU-only mode will work but will be significantly slower for edge node inference.

---

## Open Questions

1. **Simulation vs Real Data**: The spec calls for "simulated traffic cameras." Should we build a synthetic data generator that creates realistic traffic video frames, or use pre-recorded traffic video clips?
2. **GCP Deployment**: Phase 5 mentions GCP + Terraform. Do you have a GCP account, or should we keep everything local with Minikube/kind for Kubernetes?
3. **Dashboard Frontend**: The spec mentions Tailwind CSS. Should we stick with Django templates + Tailwind, or would you prefer a separate React/Next.js frontend communicating via WebSockets?
4. **Scope for V1**: Should we implement ALL elite features (Digital Twin, Chaos Engineering, Multi-City, Predictive AI, Smart EV Battery) or focus on the core 8 phases first?

---

## Project Directory Structure

```
d:\Programing\projects\edgeX\
├── docker-compose.yml                 # Full stack orchestration
├── docker-compose.dev.yml             # Development overrides
├── .env                               # Environment variables
├── .env.example                       # Template for env vars
├── .gitignore
├── README.md
├── Makefile                           # Convenience commands
├── pyproject.toml                     # Root Python project config
│
├── services/                          # All microservices
│   ├── edge-node/                     # Phase 2: Edge AI Node
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── config.py
│   │   ├── main.py                    # Entry point
│   │   ├── detector/
│   │   │   ├── __init__.py
│   │   │   ├── yolo_detector.py       # YOLOv8 inference engine
│   │   │   └── frame_generator.py     # Synthetic traffic frames
│   │   ├── producer/
│   │   │   ├── __init__.py
│   │   │   └── kafka_producer.py      # Async Kafka event producer
│   │   └── telemetry/
│   │       ├── __init__.py
│   │       └── ev_simulator.py        # EV telemetry simulator
│   │
│   ├── traffic-service/               # Phase 1: Traffic FastAPI
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── app/
│   │   │   ├── __init__.py
│   │   │   ├── main.py               # FastAPI app
│   │   │   ├── config.py
│   │   │   ├── models/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── traffic.py         # SQLAlchemy models
│   │   │   │   └── intersection.py
│   │   │   ├── schemas/
│   │   │   │   ├── __init__.py
│   │   │   │   └── traffic.py         # Pydantic schemas
│   │   │   ├── routers/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── traffic.py         # Traffic endpoints
│   │   │   │   └── health.py          # Health check
│   │   │   ├── services/
│   │   │   │   ├── __init__.py
│   │   │   │   └── traffic_service.py # Business logic
│   │   │   └── consumers/
│   │   │       ├── __init__.py
│   │   │       └── kafka_consumer.py  # Kafka consumer
│   │   └── alembic/                   # DB migrations
│   │       └── ...
│   │
│   ├── routing-service/               # Phase 1: Routing FastAPI
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── __init__.py
│   │       ├── main.py
│   │       ├── config.py
│   │       ├── routers/
│   │       │   ├── __init__.py
│   │       │   └── routing.py         # EV routing endpoints
│   │       └── services/
│   │           ├── __init__.py
│   │           └── pathfinder.py      # A* / Dijkstra routing
│   │
│   ├── analytics-service/             # Phase 1: Analytics FastAPI
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── __init__.py
│   │       ├── main.py
│   │       └── routers/
│   │           └── analytics.py       # Congestion analytics API
│   │
│   ├── alert-service/                 # Phase 1: Alert FastAPI
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── __init__.py
│   │       ├── main.py
│   │       └── routers/
│   │           └── alerts.py          # Emergency alert API
│   │
│   ├── auth-service/                  # Phase 1: Auth FastAPI
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── __init__.py
│   │       ├── main.py
│   │       ├── routers/
│   │       │   └── auth.py            # JWT auth endpoints
│   │       └── utils/
│   │           └── jwt_handler.py     # JWT token utils
│   │
│   └── compute-workers/               # Phase 3: Ray Workers
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── config.py
│       ├── main.py                    # Ray worker entry
│       ├── workers/
│       │   ├── __init__.py
│       │   ├── congestion.py          # Congestion analysis
│       │   ├── heatmap.py             # Heatmap generation
│       │   ├── route_optimizer.py     # Global route optimization
│       │   ├── emergency_corridor.py  # Green corridor logic
│       │   └── predictor.py           # Traffic prediction ML
│       └── consumers/
│           ├── __init__.py
│           └── kafka_consumer.py      # Stream consumer for Ray
│
├── dashboard/                         # Phase 4: Django Dashboard
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── manage.py
│   ├── edgecloudx/                    # Django project
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   ├── asgi.py                    # ASGI for Channels
│   │   ├── urls.py
│   │   └── routing.py                # WebSocket routing
│   ├── traffic/                       # Django app
│   │   ├── __init__.py
│   │   ├── models.py
│   │   ├── views.py
│   │   ├── consumers.py              # WebSocket consumers
│   │   ├── routing.py
│   │   └── templates/
│   │       └── traffic/
│   │           ├── base.html
│   │           ├── dashboard.html     # Main dashboard
│   │           ├── heatmap.html       # Traffic heatmap
│   │           └── components/
│   │               ├── traffic_grid.html
│   │               ├── ev_tracker.html
│   │               ├── node_health.html
│   │               └── alerts.html
│   └── static/
│       ├── css/
│       │   └── dashboard.css
│       └── js/
│           ├── websocket.js           # WS connection manager
│           ├── traffic_grid.js        # Live traffic visualization
│           ├── heatmap.js             # Heatmap rendering
│           └── charts.js             # Metrics charts
│
├── federated/                         # Phase 7: Federated Learning
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── server/
│   │   ├── __init__.py
│   │   └── fl_server.py              # Flower FL server
│   └── client/
│       ├── __init__.py
│       ├── fl_client.py              # Flower FL client
│       └── anomaly_model.py          # Lightweight anomaly head
│
├── infra/                             # Phase 5 & 6: Infrastructure
│   ├── kubernetes/
│   │   ├── namespace.yaml
│   │   ├── kafka/
│   │   │   ├── kafka-deployment.yaml
│   │   │   └── kafka-service.yaml
│   │   ├── redis/
│   │   │   ├── redis-deployment.yaml
│   │   │   └── redis-service.yaml
│   │   ├── postgres/
│   │   │   ├── postgres-deployment.yaml
│   │   │   └── postgres-service.yaml
│   │   ├── services/
│   │   │   ├── traffic-service.yaml
│   │   │   ├── routing-service.yaml
│   │   │   ├── analytics-service.yaml
│   │   │   ├── alert-service.yaml
│   │   │   └── auth-service.yaml
│   │   ├── hpa/                       # Horizontal Pod Autoscalers
│   │   │   └── traffic-hpa.yaml
│   │   └── configmaps/
│   │       └── app-config.yaml
│   ├── monitoring/                    # Phase 6: Observability
│   │   ├── prometheus/
│   │   │   ├── prometheus.yml
│   │   │   └── alert-rules.yml
│   │   ├── grafana/
│   │   │   ├── provisioning/
│   │   │   │   ├── datasources.yml
│   │   │   │   └── dashboards.yml
│   │   │   └── dashboards/
│   │   │       ├── traffic-overview.json
│   │   │       ├── node-health.json
│   │   │       └── kafka-metrics.json
│   │   └── loki/
│   │       └── loki-config.yml
│   └── terraform/                     # Cloud provisioning
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
│
├── chaos/                             # Phase 8: Chaos Engineering
│   ├── scenarios/
│   │   ├── kill_kafka_broker.py
│   │   ├── kill_worker_node.py
│   │   ├── network_partition.py
│   │   └── cpu_stress.py
│   └── runner.py                      # Chaos test orchestrator
│
├── scripts/                           # Utility scripts
│   ├── setup.sh                       # One-command setup
│   ├── seed_data.py                   # Seed initial data
│   └── generate_traffic.py            # Traffic data generator
│
└── tests/                             # Integration & unit tests
    ├── conftest.py
    ├── unit/
    │   ├── test_traffic_service.py
    │   ├── test_routing.py
    │   └── test_detector.py
    └── integration/
        ├── test_kafka_pipeline.py
        ├── test_redis_pubsub.py
        └── test_end_to_end.py
```

---

## Proposed Changes — Phase by Phase

### Phase 1 — Core Infrastructure

This is the foundation. We set up Docker Compose to orchestrate Kafka, Redis, PostgreSQL, and the FastAPI microservices skeleton.

---

#### [NEW] [docker-compose.yml](file:///d:/Programing/projects/edgeX/docker-compose.yml)
Full Docker Compose orchestration with:
- **Kafka** (KRaft mode — no ZooKeeper): Single broker with 5 topics pre-created (`traffic-density`, `ev-telemetry`, `emergency-alerts`, `anomaly-events`, `node-health`)
- **Redis** 7.x: Configured with Pub/Sub enabled
- **PostgreSQL** 16: With health checks and volume persistence
- **Network**: Shared `edgecloudx-net` bridge network
- All services with health checks, restart policies, and proper dependency ordering

#### [NEW] [.env.example](file:///d:/Programing/projects/edgeX/.env.example)
Template for all environment variables: Kafka bootstrap servers, Redis URL, PostgreSQL DSN, JWT secret, service ports.

#### [NEW] [services/traffic-service/](file:///d:/Programing/projects/edgeX/services/traffic-service/)
FastAPI microservice handling traffic data ingestion and querying:
- **Endpoints**: `GET /traffic/grid`, `GET /traffic/intersection/{id}`, `POST /traffic/update`, `GET /health`
- **Kafka Consumer**: Subscribes to `traffic-density` topic, processes events, stores to PostgreSQL
- **Redis Integration**: Publishes processed state to Redis Pub/Sub for dashboard
- **SQLAlchemy** models with Alembic migrations

#### [NEW] [services/routing-service/](file:///d:/Programing/projects/edgeX/services/routing-service/)
FastAPI microservice for EV routing:
- **Endpoints**: `POST /route/calculate`, `GET /route/ev/{ev_id}`, `GET /health`
- **Pathfinding**: A* algorithm on a weighted graph representing city grid
- Considers real-time congestion data from Redis

#### [NEW] [services/analytics-service/](file:///d:/Programing/projects/edgeX/services/analytics-service/)
FastAPI microservice for congestion analytics:
- **Endpoints**: `GET /analytics/congestion`, `GET /analytics/history`, `GET /health`
- Aggregates historical traffic data from PostgreSQL

#### [NEW] [services/alert-service/](file:///d:/Programing/projects/edgeX/services/alert-service/)
FastAPI microservice for emergency alerts:
- **Endpoints**: `POST /alerts/emergency`, `GET /alerts/active`, `GET /health`
- Kafka consumer on `emergency-alerts` topic
- Triggers green corridor logic

#### [NEW] [services/auth-service/](file:///d:/Programing/projects/edgeX/services/auth-service/)
FastAPI microservice for authentication:
- **Endpoints**: `POST /auth/login`, `POST /auth/register`, `GET /auth/verify`
- JWT-based auth with bcrypt password hashing
- Shared auth middleware for other services

---

### Phase 2 — Edge AI Nodes

Simulated edge nodes that process traffic video and produce Kafka events.

#### [NEW] [services/edge-node/](file:///d:/Programing/projects/edgeX/services/edge-node/)
Edge AI processing node:
- **YOLOv8 Detector**: Loads frozen `yolov8n.pt` model, runs inference on video frames
- **Frame Generator**: Synthetic traffic frame generator using OpenCV (draws vehicles, intersections, traffic lights on canvas)
- **Kafka Producer**: Async `aiokafka` producer publishing to `traffic-density`, `emergency-alerts`, `anomaly-events`
- **EV Simulator**: Generates EV telemetry data (position, battery, speed) and publishes to `ev-telemetry`
- Configurable: number of simulated intersections, vehicle density, event frequency

---

### Phase 3 — Distributed Compute (Ray)

#### [NEW] [services/compute-workers/](file:///d:/Programing/projects/edgeX/services/compute-workers/)
Ray cluster workers consuming Kafka streams:
- **Congestion Worker**: Calculates real-time congestion scores per grid cell
- **Heatmap Worker**: Generates traffic density heatmaps (numpy matrices → serialized to Redis)
- **Route Optimizer**: Global path optimization across all EVs using congestion-weighted graphs
- **Emergency Corridor**: Computes green-wave signal corridors for emergency vehicles
- **Traffic Predictor**: Time-series prediction using lightweight LSTM model

---

### Phase 4 — Real-Time Dashboard

#### [NEW] [dashboard/](file:///d:/Programing/projects/edgeX/dashboard/)
Django Channels application:
- **WebSocket Consumers**: Subscribe to Redis Pub/Sub, stream updates to connected browsers
- **Live Traffic Grid**: Interactive grid showing intersection states, vehicle counts, signal colors
- **EV Tracking**: Real-time EV positions on a city map
- **Heatmap Visualization**: Canvas-rendered congestion heatmaps
- **Node Health Panel**: Edge node status, latency, throughput
- **Emergency Alerts**: Live alert feed with audio notifications
- **Cluster Metrics**: Ray worker utilization, Kafka lag, Redis memory
- Frontend: Tailwind CSS + vanilla JavaScript with WebSocket integration

---

### Phase 5 — Cloud-Native (Kubernetes)

#### [NEW] [infra/kubernetes/](file:///d:/Programing/projects/edgeX/infra/kubernetes/)
Kubernetes manifests for all services:
- Deployments, Services, ConfigMaps for each microservice
- Horizontal Pod Autoscalers (HPA) based on CPU/memory
- PersistentVolumeClaims for PostgreSQL and Kafka
- Namespace isolation

---

### Phase 6 — Observability

#### [NEW] [infra/monitoring/](file:///d:/Programing/projects/edgeX/infra/monitoring/)
Full monitoring stack:
- **Prometheus**: Metric scraping from all FastAPI services (using `prometheus-fastapi-instrumentator`)
- **Grafana**: Pre-configured dashboards for traffic overview, node health, Kafka metrics
- **Loki**: Centralized log aggregation from all containers via Promtail

---

### Phase 7 — Federated Learning

#### [NEW] [federated/](file:///d:/Programing/projects/edgeX/federated/)
Flower-based federated learning:
- **FL Server**: Coordinates weight aggregation using FedAvg strategy
- **FL Client**: Runs on each edge node, trains local anomaly detection head
- **Anomaly Model**: Lightweight CNN classifier for traffic anomalies (accident, breakdown, congestion)

---

### Phase 8 — Chaos Engineering

#### [NEW] [chaos/](file:///d:/Programing/projects/edgeX/chaos/)
Failure simulation and resilience testing:
- Kill Kafka broker and verify consumer failover
- Kill Ray worker and verify task redistribution
- Simulate network partition between edge nodes and cloud
- CPU stress testing for autoscaler validation

---

## Verification Plan

### Automated Tests
```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (requires Docker Compose running)
pytest tests/integration/ -v

# Kafka pipeline test
python -m pytest tests/integration/test_kafka_pipeline.py -v

# Full end-to-end test
python -m pytest tests/integration/test_end_to_end.py -v
```

### Manual Verification
- **Phase 1**: `docker compose up` → All containers healthy → Hit health endpoints
- **Phase 2**: Edge node produces events visible in Kafka UI
- **Phase 3**: Ray dashboard shows active workers processing tasks
- **Phase 4**: Open browser → Dashboard shows live updating traffic grid
- **Phase 5**: `kubectl get pods` → All pods running
- **Phase 6**: Grafana dashboards showing metrics
- **Phase 7**: FL training rounds completing, model accuracy improving
- **Phase 8**: Kill services → Watch recovery → Validate no data loss

### Service Health Verification
```bash
# Check all services
curl http://localhost:8001/health  # traffic-service
curl http://localhost:8002/health  # routing-service
curl http://localhost:8003/health  # analytics-service
curl http://localhost:8004/health  # alert-service
curl http://localhost:8005/health  # auth-service
```
