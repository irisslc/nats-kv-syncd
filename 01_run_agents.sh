#!/usr/bin/env bash
set -euo pipefail

mkdir -p logs
./scripts/98_stop_agents.sh >/dev/null 2>&1 || true

GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:4222   --bucket config --node-id site-a   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-a   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=5m   > logs/site-a.log 2>&1 &
echo $! > logs/site-a.pid

GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:5222   --bucket config --node-id site-b   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-b   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=5m   > logs/site-b.log 2>&1 &
echo $! > logs/site-b.pid

sleep 1
echo "OK: agentes en background. Logs en ./logs/site-a.log y ./logs/site-b.log"
