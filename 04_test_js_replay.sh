#!/usr/bin/env bash
set -euo pipefail

mkdir -p logs
echo "Parando agentes"
./scripts/98_stop_agents.sh >/dev/null

echo "Arrancando solo A (sin reconcile)"
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:4222   --bucket config --node-id site-a   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-a   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=0   > logs/site-a.log 2>&1 &
echo $! > logs/site-a.pid

echo "Generando 3 ops mientras B está parado"
nats --server localhost:4222 kv put config replayKey v1 >/dev/null
nats --server localhost:4222 kv put config replayKey v2 >/dev/null
nats --server localhost:4222 kv del config replayKey >/dev/null

echo "Arrancando B: debe hacer replay duradero"
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:5222   --bucket config --node-id site-b   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-b   --announce-on-start=false --announce-on-reconnect=false --reconcile-every=0   > logs/site-b.log 2>&1 &
echo $! > logs/site-b.pid

sleep 2

echo "[B] consumer state"
nats --server localhost:5222 consumer info REPKVOPS syncd-site-b | grep -E "Unprocessed Messages|Outstanding Acks|Acknowledgment Floor|Last Delivered"

echo "[B] replayKey debe NO existir"
nats --server localhost:5222 kv get config replayKey || true

echo "OK"
