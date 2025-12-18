#!/usr/bin/env bash
set -euo pipefail

docker compose up -d
mkdir -p logs

nats --server localhost:4222 kv add config >/dev/null 2>&1 || true
nats --server localhost:5222 kv add config >/dev/null 2>&1 || true

echo "OK: entorno arriba (nats-a:4222, nats-b:5222) y bucket config creado."
