# Apache Kafka (KRaft)

Apache Kafka 4.3.0 running in **KRaft mode** (no Zookeeper) on a single
broker. Managed by `systemd` and tuned for low-volume CDC workloads.

## Folder structure

```
kafka/
├── README.md
├── server.properties       # Broker configuration
├── kafka.service           # systemd unit
└── manage-topics.sh        # Topic management & retention helper
```

## Prerequisites

- Ubuntu 22.04+
- Java 17+ (`java -version`)
- 8 GB RAM minimum, 50 GB free disk for small setups
- A non-root user `kafka` to run the broker (recommended)

```bash
# Create dedicated user
sudo useradd -r -s /bin/false kafka
```

## Installation

```bash
# Download and extract
cd /tmp
wget https://archive.apache.org/dist/kafka/4.3.0/kafka_2.13-4.3.0.tgz
sudo tar -xzf kafka_2.13-4.3.0.tgz -C /opt
sudo mv /opt/kafka_2.13-4.3.0 /opt/kafka
sudo chown -R kafka:kafka /opt/kafka
```

## KRaft setup (single broker)

```bash
# Generate cluster ID and format storage
KAFKA_CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
  -t "${KAFKA_CLUSTER_ID}" \
  -c /opt/kafka/config/server.properties
```

Save the cluster ID — you need it to recover from a disaster.

## systemd integration

```bash
sudo cp kafka/kafka.service /etc/systemd/system/kafka.service
sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka
sudo systemctl status kafka
```

## Configuration

The bundled `server.properties` is tuned for a single-broker CDC
deployment. Key settings:

| Setting | Value | Why |
|---|---|---|
| `process.roles` | `broker,controller` | Single-node KRaft, no Zookeeper [1] |
| `node.id` | `1` | Single broker |
| `listeners` | `PLAINTEXT://:9092,CONTROLLER://:9093` | Standard ports |
| `auto.create.topics.enable` | `false` | Topics must be explicit |
| `num.partitions` | `1` | CDC is low-volume; no need for parallelism |
| `log.retention.bytes` | unset | Controlled per topic [1] |

> **Why no Zookeeper?** KRaft is the recommended production mode since
> Kafka 3.3+ and became the default in 4.0. It removes a dependency
> and simplifies operations [1].

## Topic management

All topics in this pipeline are created automatically by the Debezium
Source connector (one topic per table). You rarely need to create
topics manually.

### List topics

```bash
sudo -u kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### Inspect a topic

```bash
sudo -u kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic <topic-name>
```

### Check consumer lag

```bash
sudo -u kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group connect-sink-postgres-connector
```

## Retention tuning

Default retention is unlimited, which fills the disk during the
initial snapshot of large databases. Use `manage-topics.sh` to apply
a global cap [1]:

```bash
sudo bash kafka/manage-topics.sh set-retention <topic-prefix> 200MB
```

This sets `retention.bytes=209715200` on every topic matching the
prefix. Restart Kafka to reclaim disk space immediately:

```bash
sudo systemctl restart kafka
```

## Useful commands

```bash
# Status
sudo systemctl status kafka

# Tail logs
sudo journalctl -u kafka -f

# Disk usage
du -sh /var/lib/kafka/logs/

# Disk free
df -h /
```

## Disaster recovery

To rebuild from scratch on a new host:

1. Install Kafka (same version)
2. Copy or regenerate the cluster ID
3. Format storage
4. Recreate the internal Kafka Connect topics
   (`debezium_connect_configs`, `debezium_connect_offsets`,
   `debezium_connect_statuses`, `<prefix>.schema-changes`) with
   `replication.factor=1`
5. Re-register the Debezium connectors with `snapshot.mode=initial`

RPO depends on how often you back up `/var/lib/kafka/logs/`. For most
CDC workloads, regenerating from the source SQL Server via Debezium
is acceptable (no separate backup needed).
