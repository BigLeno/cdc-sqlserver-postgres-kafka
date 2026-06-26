#!/usr/bin/env bash
# Substitute env vars in connector JSONs and register them in Kafka Connect.
# Requires: envsubst (apt install gettext-base)

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# Source the .env file if it exists
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
  set +a
fi

register() {
  local name="$1"
  local template="$2"
  local resolved="$TMP_DIR/$name.json"

  echo "→ Resolving $name.json"
  envsubst < "$template" > "$resolved"

  echo "→ Registering $name in Kafka Connect"
  curl -s -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$resolved" | python3 -m json.tool

  echo
}

register "source-connector" "$SCRIPT_DIR/source-connector.json"
register "sink-connector"   "$SCRIPT_DIR/sink-connector.json"

echo "✓ Both connectors registered. Check status:"
echo "  curl -s $CONNECT_URL/connectors | python3 -m json.tool"
