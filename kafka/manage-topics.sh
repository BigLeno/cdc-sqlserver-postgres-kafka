#!/usr/bin/env bash
# =============================================
# Manage-topics — Topic & retention helper
# =============================================
# Wraps kafka-topics.sh and kafka-configs.sh with safer
# defaults for a single-broker CDC pipeline.
# =============================================

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

usage() {
  cat <<EOF
Usage: $0 <command> [args]

Commands:
  list                         List all topics
  describe <topic>             Show partitions & replicas
  set-retention <prefix> <MB>  Apply retention.bytes to all topics matching prefix
  delete-prefix <prefix>       Delete all topics matching prefix (asks confirmation)

Examples:
  $0 list
  $0 describe source.dbo.tbclie
  $0 set-retention source 200
  $0 delete-prefix source

EOF
}

require_kafka() {
  if [[ ! -x "${KAFKA_HOME}/bin/kafka-topics.sh" ]]; then
    echo "kafka-topics.sh not found at ${KAFKA_HOME}/bin/" >&2
    exit 1
  fi
}

cmd_list() {
  sudo -u kafka "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" --list
}

cmd_describe() {
  local topic="$1"
  sudo -u kafka "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --describe --topic "${topic}"
}

cmd_set_retention() {
  local prefix="$1"
  local mb="$2"
  local bytes=$((mb * 1024 * 1024))

  local topics
  topics=$(sudo -u kafka "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" --list | grep "^${prefix}" || true)

  if [[ -z "${topics}" ]]; then
    echo "No topics found with prefix '${prefix}'"
    return 0
  fi

  for topic in ${topics}; do
    echo "Setting ${topic} retention.bytes=${bytes} (${mb}MB)"
    sudo -u kafka "${KAFKA_HOME}/bin/kafka-configs.sh" \
      --bootstrap-server "${BOOTSTRAP}" \
      --entity-type topics \
      --entity-name "${topic}" \
      --alter \
      --add-config "retention.bytes=${bytes}"
  done

  echo "Done. Restart kafka to reclaim disk space:"
  echo "  sudo systemctl restart kafka"
}

cmd_delete_prefix() {
  local prefix="$1"
  local topics
  topics=$(sudo -u kafka "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" --list | grep "^${prefix}" || true)

  if [[ -z "${topics}" ]]; then
    echo "No topics found with prefix '${prefix}'"
    return 0
  fi

  echo "The following topics will be DELETED:"
  echo "${topics}"
  read -rp "Are you sure? Type 'yes' to continue: " confirm
  if [[ "${confirm}" != "yes" ]]; then
    echo "Aborted."
    return 1
  fi

  for topic in ${topics}; do
    echo "Deleting ${topic}"
    sudo -u kafka "${KAFKA_HOME}/bin/kafka-topics.sh" \
      --bootstrap-server "${BOOTSTRAP}" \
      --delete --topic "${topic}"
  done
}

# --- main ---
require_kafka

case "${1:-}" in
  list)              cmd_list ;;
  describe)          cmd_describe "$2" ;;
  set-retention)     cmd_set_retention "$2" "$3" ;;
  delete-prefix)     cmd_delete_prefix "$2" ;;
  *)                 usage; exit 1 ;;
esac
