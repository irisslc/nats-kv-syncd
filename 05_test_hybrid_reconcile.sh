#!/usr/bin/env bash
set -euo pipefail

mkdir -p logs
echo "1) Paramos agentes"
./scripts/98_stop_agents.sh >/dev/null

echo "2) Arrancamos A con reconcile rápido (10s)"
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:4222   --bucket config --node-id site-a   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-a   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=10s   > logs/site-a.log 2>&1 &
echo $! > logs/site-a.pid

echo "3) Escribimos directamente en KV de nats-b SIN agente B (fuera de banda)"
nats --server localhost:5222 kv put config outOfBand X >/dev/null

echo "4) Arrancamos B con reconcile rápido (10s) y esperamos 15s"
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:5222   --bucket config --node-id site-b   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-b   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=10s   > logs/site-b.log 2>&1 &
echo $! > logs/site-b.pid

sleep 15

echo "[A] get outOfBand (debe existir tras reconcile)"
nats --server localhost:4222 kv get config outOfBand

echo "OK"
