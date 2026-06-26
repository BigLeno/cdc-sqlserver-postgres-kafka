# Kafka UI

Web interface for monitoring the Kafka cluster and Debezium
connectors. Runs via Docker and is exposed behind Nginx on port 80.

## Folder structure

```
kafka-ui/
├── README.md
├── docker-compose.yml
└── .env.example
```

## Prerequisites

- Docker Engine + Docker Compose v2
- Apache Kafka reachable from the container (see [`../kafka/`](../kafka/))
- Kafka Connect REST API reachable (Debezium container on port 8083)

## Installation

```bash
mkdir -p /opt/kafka-ui && cd /opt/kafka-ui
# Copy the files from this folder into /opt/kafka-ui

sudo docker compose up -d
```

## Configuration

All credentials and hostnames are loaded from environment variables
defined in `.env` (never committed). See `.env.example` for the full
list.

### `docker-compose.yml`

```yaml
services:
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    restart: unless-stopped
    ports:
      - "8080:8080"
    env_file:
      - .env
```

### `.env.example`

```env
# Cluster display name (shown in the Kafka UI header)
KAFKA_CLUSTERS_0_NAME=local

# Kafka broker
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092

# Kafka Connect (Debezium)
KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME=debezium
KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS=http://localhost:8083

# Auth
AUTH_TYPE=LOGIN_FORM
SPRING_SECURITY_USER_NAME=admin
SPRING_SECURITY_USER_PASSWORD=changeme
```

## Docker commands

```bash
# Start
cd /opt/kafka-ui
sudo docker compose up -d

# Stop
sudo docker compose down

# Logs
sudo docker compose logs -f
```

## Features

- **Dashboard** — cluster overview: brokers, topics, consumer groups
- **Topics** — listing, message browsing and per-topic configuration
- **Consumers** — consumer groups with real-time lag
- **Kafka Connect** — status of Source and Sink connectors with the
  ability to pause, resume and recreate them

## Authentication

Access is protected via `LOGIN_FORM`. Credentials live in `.env` and
are injected via `env_file` — never hardcoded in the compose file [1].

## Nginx reverse proxy

Kafka UI is exposed on port 80 via Nginx, eliminating the need to type
`:8080` in the URL. See [`../nginx/`](../nginx/) for the full proxy
configuration including selective access logging.

> **Security note:** the original setup exposes Kafka UI on port 80
> without TLS. For public deployments, terminate TLS at Nginx and bind
> the admin interface to localhost or a private network [1].
