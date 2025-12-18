#!/usr/bin/env bash
set -euo pipefail

echo "[A] put theme=dark"
nats --server localhost:4222 kv put config theme dark >/dev/null

echo "[B] get theme (debe ser dark)"
nats --server localhost:5222 kv get config theme

echo "OK"
