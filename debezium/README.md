# Debezium

Debezium 3.5.2 running via Docker with two connectors configured:
**Source** (SQL Server → Kafka) and **Sink** (Kafka → PostgreSQL).
The Docker image is a custom build that extends the official Debezium
image with the JDBC Sink plugin and the PostgreSQL driver [1].

## Folder structure

```
debezium/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── source-connector.json
├── sink-connector.json
└── .env.example
```

## Prerequisites

- Docker Engine + Docker Compose v2
- Apache Kafka reachable from the container (see [`../kafka/`](../kafka/))
- SQL Server with CDC enabled at database and table level (see SQL
  Server requirements at the bottom) [1]
- PostgreSQL reachable from the container
- `jq` installed on the host (`sudo apt install -y jq`) — required by
  `setup-connectors.sh`

## Installation

```bash
mkdir -p /opt/debezium && cd /opt/debezium
# Copy the files from this folder into /opt/debezium

# Build the custom image (extends official Debezium with JDBC Sink + PG driver)
sudo docker compose build

# Start the container
sudo docker compose up -d
```

## Configuration

All credentials are loaded from environment variables defined in
`.env` (never committed). See `.env.example` for the full list.

### `docker-compose.yml`

```yaml
services:
  debezium:
    build: .
    container_name: debezium
    restart: unless-stopped
    ports:
      - "8083:8083"
    env_file:
      - .env
    environment:
      BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS}
      GROUP_ID: "1"
      CONFIG_STORAGE_TOPIC: "debezium_connect_configs"
      OFFSET_STORAGE_TOPIC: "debezium_connect_offsets"
      STATUS_STORAGE_TOPIC: "debezium_connect_statuses"
      CONFIG_STORAGE_REPLICATION_FACTOR: "1"
      OFFSET_STORAGE_REPLICATION_FACTOR: "1"
      STATUS_STORAGE_REPLICATION_FACTOR: "1"
      OFFSET_STORAGE_PARTITIONS: "25"
      STATUS_STORAGE_PARTITIONS: "5"
      JVM_OPTS: "-Xms512M -Xmx1G"
```

> **Why `OFFSET_STORAGE_PARTITIONS: 25`?**
> Each topic in Kafka Connect requires its own partition in the offsets
> topic to allow parallel task execution. Setting this to 25 allows up
> to 25 parallel connector tasks before needing to grow the topic [1].

> **Note:** `CONFIG_STORAGE_REPLICATION_FACTOR`,
> `OFFSET_STORAGE_REPLICATION_FACTOR`, and
> `STATUS_STORAGE_REPLICATION_FACTOR` must be `1` for a single-broker
> setup. Higher values will cause startup failures
> (`replication factor larger than available brokers`) [1].

> **JVM heap sizing**
>
> The `JVM_OPTS` defines how much RAM the Kafka Connect worker can
> consume. As a general rule:
>
> | Scenario | Recommendation |
> |---|---|
> | Small setup (< 20 topics) | `-Xms256M -Xmx512M` |
> | Medium setup (20–100 topics) | `-Xms512M -Xmx1G` |
> | Large setup (> 100 topics) | `-Xms1G -Xmx2G` |
>
> Ensure the heap **does not exceed 50% of host RAM** to avoid
> competing with the Kafka broker and the operating system. Monitor
> with `docker stats` or a Prometheus JMX exporter.

## Docker commands

```bash
# Build and start
cd /opt/debezium
sudo docker compose build
sudo docker compose up -d

# Stop
sudo docker compose down

# Real-time logs
sudo docker compose logs -f

# Filtered logs (errors, warnings, streaming, snapshot)
sudo docker compose logs -f | grep -i "error\|warn\|streaming\|snapshot"
```

## Quick setup

After the container is running, register both connectors with a single
command:

```bash
cd /opt/debezium
chmod +x setup-connectors.sh
./setup-connectors.sh
```

The script:
1. Waits for the Kafka Connect REST API to become ready
2. Skips connectors that already exist (idempotent)
3. Registers the Source and Sink from the local JSON files
4. Prints the final status of each connector

To run the connectors manually instead, see the
[Source Connector](#source-connector--sql-server--kafka) and
[Sink Connector](#sink-connector--kafka--postgresql) sections below.

## Source Connector — SQL Server → Kafka

Captures every change from the SQL Server transaction log and publishes
it as a change event to a Kafka topic (one per table).

### `source-connector.json`

```json
{
  "name": "source-sqlserver-connector",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "${SQLSERVER_HOST}",
    "database.port": "${SQLSERVER_PORT}",
    "database.user": "${SQLSERVER_USER}",
    "database.password": "${SQLSERVER_PASSWORD}",
    "database.names": "${SQLSERVER_DATABASE}",
    "database.encrypt": "false",
    "topic.prefix": "${TOPIC_PREFIX}",
    "table.include.list": "${TABLE_INCLUDE_LIST}",
    "snapshot.mode": "recovery",
    "schema.history.internal.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
    "schema.history.internal.kafka.topic": "${TOPIC_PREFIX}.schema-changes"
  }
}
```

> **Why `snapshot.mode: recovery`?** When the DBA disables and re-enables
> CDC on SQL Server, the Log Sequence Numbers (LSNs) are reset. The
> stored Kafka offset points to an LSN that no longer exists. The
> `recovery` mode recovers the schema history without re-running a full
> snapshot of the data, avoiding hours of backfill on large tables [1].

### Register the connector manually

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @source-connector.json | python3 -m json.tool
```

## Sink Connector — Kafka → PostgreSQL

Consumes Kafka topics and applies upserts to the target schema in
PostgreSQL.

### Strategy

- `insert.mode: upsert` → `INSERT ... ON CONFLICT (pk) DO UPDATE` [1]
- `delete.enabled: true` → propagates `DELETE` operations from SQL Server [1]
- `topics.regex` → only consumes topics matching the configured table
  pattern (defaults to tables whose names start with a lowercase letter
  followed by an uppercase letter, e.g. `tbCLIENTS`, `tbOrders`) [1]
- `primary.key.mode: record_key` → reads the PK from the Kafka message
  key [1]
- `quote.identifiers: false` → table names in lowercase without double
  quotes [1]
- `schema.evolution: none` → schema changes are not auto-applied; see
  Runbook for the DDL change procedure [1]

### Prerequisite

Every table in the target PostgreSQL schema **must have a PRIMARY KEY
defined** before registering the Sink. Use your preferred tool
(`psql`, pgAdmin, dbeaver, or a migration script) to add PKs that
match the source table primary keys.

### `sink-connector.json`

```json
{
  "name": "sink-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE}",
    "connection.username": "${POSTGRES_USER}",
    "connection.password": "${POSTGRES_PASSWORD}",
    "insert.mode": "upsert",
    "delete.enabled": "true",
    "schema.evolution": "none",
    "topics.regex": "${TOPIC_PREFIX}\\.${SQLSERVER_DATABASE}\\.dbo\\.(tb[A-Z]|TB).*",
    "table.name.format": "bronze.${topic}",
    "primary.key.mode": "record_key",
    "quote.identifiers": "false",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "${TOPIC_PREFIX}\\.${SQLSERVER_DATABASE}\\.dbo\\.(.+)",
    "transforms.route.replacement": "$1"
  }
}
```

### Register the connector manually

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @sink-connector.json | python3 -m json.tool
```

## Connector management

```bash
# Status of Source
curl -s "${KAFKA_CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/status" \
  | python3 -m json.tool

# Status of Sink
curl -s "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/status" \
  | python3 -m json.tool

# List all connectors
curl -s "${KAFKA_CONNECT_URL}/connectors" | python3 -m json.tool

# Pause Source
curl -s -X PUT "${KAFKA_CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/pause"

# Resume Source
curl -s -X PUT "${KAFKA_CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/resume"

# Restart Sink task
curl -s -X POST "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/tasks/0/restart"

# Delete a connector
curl -s -X DELETE "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}"

# List available plugins
curl -s "${KAFKA_CONNECT_URL}/connector-plugins" | python3 -m json.tool
```

## SQL Server requirements

For CDC to work, the DBA must ensure:

1. **CDC enabled on the database:**
   ```sql
   EXEC sys.sp_cdc_enable_db
   ```

2. **CDC enabled on each table to be replicated:**
   ```sql
   EXEC sys.sp_cdc_enable_table
       @source_schema = 'dbo',
       @source_name   = 'tbEXAMPLE',
       @role_name     = NULL
   ```

3. **SQL Server Agent running** — required by CDC to scan the
   transaction log [1]:
   ```sql
   SELECT program_name, status
   FROM sys.dm_exec_sessions
   WHERE program_name LIKE '%SQLAgent%'
   ```

## Troubleshooting

| Error | Cause | Action |
|---|---|---|
| `no unique or exclusion constraint` | PK does not exist in PostgreSQL | Add PK to the target table (see prerequisite) |
| `Unable to get last available log position` | CDC disabled on SQL Server | DBA re-enables CDC on the database |
| `db history topic is missing` | Schema history topic lost | Recreate connector with `snapshot.mode: recovery` |
| `terminating connection due to administrator command` | PostgreSQL restarted | Restart Sink task |
