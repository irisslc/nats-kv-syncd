#!/usr/bin/env bash
set -euo pipefail

# Robust nats CLI resolution (works even under sudo where PATH may be reset)
resolve_nats() {
  if command -v nats >/dev/null 2>&1; then
    command -v nats
    return 0
  fi

  for p in /usr/local/bin/nats /usr/bin/nats /bin/nats; do
    if [[ -x "$p" ]]; then
      echo "$p"
      return 0
    fi
  done

  if [[ -n "${SUDO_USER:-}" ]]; then
    for p in "/home/${SUDO_USER}/go/bin/nats" "/home/${SUDO_USER}/.local/bin/nats"; do
      if [[ -x "$p" ]]; then
        echo "$p"
        return 0
      fi
    done
  fi

  for p in "${HOME}/go/bin/nats" "${HOME}/.local/bin/nats"; do
    if [[ -x "$p" ]]; then
      echo "$p"
      return 0
    fi
  done

  return 1
}

NATS_BIN="$(resolve_nats || true)"
if [[ -z "${NATS_BIN:-}" ]]; then
  echo "ERROR: No se encontró el binario 'nats' en PATH ni en rutas comunes." >&2
  echo "Sugerencias:" >&2
  echo "  - Ejecuta el script SIN sudo: ./${0#./}" >&2
  echo "  - O bien instala/enlaza nats en /usr/local/bin:" >&2
  echo "      sudo ln -sf "$(go env GOPATH)/bin/nats" /usr/local/bin/nats" >&2
  exit 127
fi

mkdir -p logs
echo "Parando agentes"
./98_stop_agents.sh >/dev/null

echo "Arrancando solo A (sin reconcile)"
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:4222   --bucket config --node-id site-a   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-a   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=0   > logs/site-a.log 2>&1 &
echo $! > logs/site-a.pid

echo "Generando 3 ops mientras B está parado"
"$NATS_BIN" --server localhost:4222 kv put config replayKey v1 >/dev/null
"$NATS_BIN" --server localhost:4222 kv put config replayKey v2 >/dev/null
"$NATS_BIN" --server localhost:4222 kv del config replayKey --force >/dev/null

echo "Arrancando B: debe hacer replay duradero"
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:5222   --bucket config --node-id site-b   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-b   --announce-on-start=false --announce-on-reconnect=false --reconcile-every=0   > logs/site-b.log 2>&1 &
echo $! > logs/site-b.pid

sleep 2

echo "[B] consumer state"
"$NATS_BIN" --server localhost:5222 consumer info REPKVOPS syncd-site-b | grep -E "Unprocessed Messages|Outstanding Acks|Acknowledgment Floor|Last Delivered"

echo "[B] replayKey debe NO existir"
"$NATS_BIN" --server localhost:5222 kv get config replayKey || true

echo "OK"

