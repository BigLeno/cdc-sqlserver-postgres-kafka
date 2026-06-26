#!/usr/bin/env bash
# =============================================
# reset-connector.sh — Reset a Debezium connector
# =============================================
# Automates the DELETE + sleep + POST pattern used in runbook
# scenarios 1, 3, and 4. Reads the JSON config from ../debezium/
# and substitutes \${...} placeholders using envsubst.
# =============================================

set -euo pipefail

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEBEZIUM_DIR="${DEBEZIUM_DIR:-${SCRIPT_DIR}/../debezium}"

usage() {
  cat <<EOF
Usage: $0 <source|sink>

Environment variables required:
  SOURCE_CONNECTOR_NAME  Name of the Source connector
  SINK_CONNECTOR_NAME    Name of the Sink connector
  KAFKA_CONNECT_URL      REST API base (default: http://localhost:8083)

Source also requires:
  SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER, SQLSERVER_PASSWORD,
  SQLSERVER_DATABASE, TOPIC_PREFIX, TABLE_INCLUDE_LIST,
  KAFKA_BOOTSTRAP_SERVERS

Sink also requires:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE,
  POSTGRES_USER, POSTGRES_PASSWORD, TOPIC_PREFIX

Example:
  export SOURCE_CONNECTOR_NAME=source-sqlserver-connector
  export SINK_CONNECTOR_NAME=sink-postgres-connector
  bash reset-connector.sh source
EOF
}

delete_connector() {
  local name="$1"
  echo "Deleting connector '${name}'..."
  curl -s -X DELETE "${KAFKA_CONNECT_URL}/connectors/${name}"
  echo
}

wait_cleanup() {
  echo "Waiting 5 seconds for cleanup..."
  sleep 5
}

create_connector() {
  local json_file="$1"
  echo "Creating connector from ${json_file}..."
  envsubst < "${json_file}" | curl -s -X POST "${KAFKA_CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" -d @- | python3 -m json.tool
}

check_connect_ready() {
  local retries=30
  echo "Checking Kafka Connect readiness..."
  for ((i=1; i<=retries; i++)); do
    if curl -sf "${KAFKA_CONNECT_URL}/" > /dev/null; then
      echo "Kafka Connect is ready"
      return 0
    fi
    echo "  attempt ${i}/${retries}..."
    sleep 2
  done
  echo "Kafka Connect did not respond after ${retries} attempts" >&2
  return 1
}

reset_source() {
  local name="${SOURCE_CONNECTOR_NAME:?SOURCE_CONNECTOR_NAME is required}"
  local json="${DEBEZIUM_DIR}/source-connector.json"

  check_connect_ready
  delete_connector "${name}"
  wait_cleanup
  create_connector "${json}"
}

reset_sink() {
  local name="${SINK_CONNECTOR_NAME:?SINK_CONNECTOR_NAME is required}"
  local json="${DEBEZIUM_DIR}/sink-connector.json"

  check_connect_ready
  delete_connector "${name}"
  wait_cleanup
  create_connector "${json}"
}

case "${1:-}" in
  source) reset_source ;;
  sink)   reset_sink ;;
  *)      usage; exit 1 ;;
esac
