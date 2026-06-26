# Runbook

Operational guide for diagnosing and resolving issues in the CDC
pipeline. Based on real production scenarios.

## Folder structure

```
runbook/
├── README.md             # This file
└── reset-connector.sh    # Helper for source/sink connector reset
```

## Conventions used in this document

- All commands assume Kafka Connect REST API at
  `${KAFKA_CONNECT_URL:-http://localhost:8083}`.
- All credentials are read from environment variables. See
  [`../debezium/.env.example`](../debezium/.env.example).
- Placeholders use the `${VARIABLE}` syntax. Export them in your
  shell before running commands, or use `envsubst` for JSON payloads.

## Quick status check

```bash
# Source connector
curl -s "${KAFKA_CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/status" \
  | python3 -m json.tool

# Sink connector
curl -s "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/status" \
  | python3 -m json.tool

# Both at once
curl -s "${KAFKA_CONNECT_URL}/connectors" | python3 -m json.tool
```

Possible states: `RUNNING` ✅ | `PAUSED` ⏸️ | `FAILED` ❌

---

## Common problems

### 1. Source FAILED — `Unable to get last available log position`

**Cause:** CDC was disabled and re-enabled on SQL Server, which resets
the LSNs. The stored Kafka offset now points to an LSN that no longer
exists [1].

**Solution:**
```bash
bash reset-connector.sh source
```

This script deletes the connector, waits for cleanup, and recreates
it using `snapshot.mode: recovery` — which recovers the schema
history without re-running a full snapshot of the data [1].

### 2. Source FAILED — `Invalid object name '<db>.cdc.change_tables'`

**Cause:** CDC is disabled on the source database.

**Diagnosis:**
```sql
SELECT is_cdc_enabled FROM sys.databases WHERE name = '<your-database>'
-- 0 = disabled, 1 = enabled
```

**Solution:** Ask the DBA to run:
```sql
-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db

-- Enable CDC on each operational table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'tbEXAMPLE',
    @role_name     = NULL
```

After the DBA finishes, restart the task:
```bash
curl -s -X POST \
  "${KAFKA_CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/tasks/0/restart"
```

### 3. Source FAILED — `db history topic is missing`

**Cause:** The schema history topic (`${TOPIC_PREFIX}.schema-changes`)
was deleted or never created [1].

**Solution:** Recreate the connector with `snapshot.mode: recovery`
(same procedure as item 1):
```bash
bash reset-connector.sh source
```

### 4. Sink FAILED — `there is no unique or exclusion constraint matching the ON CONFLICT specification`

**Cause:** Tables in the target PostgreSQL schema do not have a
PRIMARY KEY defined [1].

**Solution:**
```bash
# Run the PK creation script
python3 scripts/add_primary_keys.py
```

Then recreate the Sink connector:
```bash
bash reset-connector.sh sink
```

### 5. Sink FAILED — `terminating connection due to administrator command`

**Cause:** PostgreSQL was restarted or an administrator terminated
connections manually [1].

**Solution:** Restart the Sink task:
```bash
curl -s -X POST \
  "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/tasks/0/restart"
```

### 6. Debezium fails to start — `replication factor larger than available brokers`

**Cause:** The internal Kafka Connect topics are configured with a
replication factor greater than 1 on a single-broker setup [1].

**Solution:** Verify `docker-compose.yml` in `/opt/debezium` and
ensure:
```yaml
CONFIG_STORAGE_REPLICATION_FACTOR: "1"
OFFSET_STORAGE_REPLICATION_FACTOR: "1"
STATUS_STORAGE_REPLICATION_FACTOR: "1"
```

### 7. Kafka does not start after a reboot

**Cause:** The service is not enabled to start automatically [1].

```bash
# Check status
sudo systemctl status kafka

# Enable on boot
sudo systemctl enable kafka

# Start manually
sudo systemctl start kafka
```

### 8. Disk full on the broker host

**Cause:** Kafka topics are consuming too much space. Common during
the initial snapshot of large databases [1].

**Diagnosis:**
```bash
df -h /
du -sh /var/lib/kafka/logs/
```

**Solution:** Apply a per-topic retention cap using the helper
script:
```bash
sudo bash kafka/manage-topics.sh set-retention ${TOPIC_PREFIX} 200
```

Then restart Kafka to reclaim disk space immediately:
```bash
sudo systemctl restart kafka
```

---

## Pause and resume the pipeline

Use when you need to perform maintenance on SQL Server or PostgreSQL
without losing events.

```bash
# Pause the Source (stops capturing changes)
curl -s -X PUT \
  "${KAFKA_CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/pause"

# Pause the Sink (stops writing to PostgreSQL)
curl -s -X PUT \
  "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/pause"

# Resume both
curl -s -X PUT \
  "${KAFKA_CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/resume"
curl -s -X PUT \
  "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/resume"
```

> **Important:** Kafka retains events while the Sink is paused. When
> resumed, the Sink processes everything that was queued automatically
> [1].

---

## Check Sink lag

Lag indicates how many messages have not yet been processed by the
Sink.

```bash
# Via Kafka UI (recommended)
# ${KAFKA_UI_URL} → Consumers → connect-${SINK_CONNECTOR_NAME}

# Via CLI
sudo -u kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group "connect-${SINK_CONNECTOR_NAME}"
```

**Lag = 0** means the target schema is synchronised with Kafka.

Recommended thresholds:

| Lag | Status |
|---|---|
| 0 | ✅ Synchronised |
| < 1 000 | 🟢 Healthy |
| 1 000 – 10 000 | 🟡 Investigate |
| > 10 000 | 🔴 Action required |

---

## Validate real-time replication

After any intervention, validate that CDC is working:

```sql
-- 1. Update a record in SQL Server
UPDATE dbo.tbEXAMPLE
SET some_column = 'test_cdc_' + CONVERT(VARCHAR, GETDATE())
WHERE id = 1

-- 2. Verify in PostgreSQL (wait ~5 seconds)
SELECT id, some_column
FROM bronze.tbexample
WHERE id = 1
```

---

## Full reload of the target schema

Use only if the target schema is corrupted or severely out of date.
**Destroys all data in the target schema and rebuilds from scratch.**

```bash
# 1. Pause the Sink to avoid conflicts during the load
curl -s -X PUT \
  "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/pause"

# 2. Run the scripts in order
python3 scripts/migrate_data.py
python3 scripts/rename_to_lowercase.py
python3 scripts/add_primary_keys.py

# 3. Resume the Sink
curl -s -X PUT \
  "${KAFKA_CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/resume"
```

---

## Useful logs

```bash
# Debezium logs in real time
cd /opt/debezium && sudo docker compose logs -f

# Errors only
cd /opt/debezium && sudo docker compose logs -f \
  | grep -i "error\|failed\|exception"

# Kafka UI access logs
sudo tail -f /var/log/nginx/kafka-ui-access.log

# Kafka status
sudo systemctl status kafka

# Disk usage
watch -n 30 df -h /
```

---

## Summary table

| Symptom | Likely cause | Action |
|---|---|---|
| Source FAILED, invalid LSN | CDC re-enabled on SQL Server | `reset-connector.sh source` |
| Source FAILED, `cdc.change_tables` | CDC disabled on database | DBA re-enables CDC |
| Sink FAILED, ON CONFLICT | PKs missing in target schema | Run `add_primary_keys.py` |
| Sink FAILED, terminating connection | PostgreSQL restarted | Restart Sink task |
| Debezium fails to start | Replication factor > brokers | Fix `docker-compose.yml` |
| Kafka does not start | Service not enabled | `systemctl enable kafka` |
| Disk full | Kafka retention | `manage-topics.sh set-retention` |
| High Sink lag | Backlog of messages | Wait or check errors |
