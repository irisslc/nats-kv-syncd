#!/usr/bin/env bash
set -euo pipefail

echo "Simulando partición: parando nats-hub (leafs siguen locales)"
docker stop nats-hub >/dev/null

echo "[A] theme=dark"
nats --server localhost:4222 kv put config theme dark >/dev/null

echo "[B] theme=light (concurrente)"
nats --server localhost:5222 kv put config theme light >/dev/null

echo "Reparando partición: arrancando nats-hub"
docker start nats-hub >/dev/null

echo "Esperando 3s..."
sleep 3

echo "[A] get theme"
nats --server localhost:4222 kv get config theme

echo "[B] get theme"
nats --server localhost:5222 kv get config theme

echo "OK (deben coincidir según LWW)"
