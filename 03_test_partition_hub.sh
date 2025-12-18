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
  echo "  - Ejecuta el script SIN sudo: ./scripts/03_test_partition_hub.sh" >&2
  echo "  - O bien instala/enlaza nats en /usr/local/bin:" >&2
  echo "      sudo ln -sf \"$(go env GOPATH)/bin/nats\" /usr/local/bin/nats" >&2
  exit 127
fi

echo "Simulando partición: parando nats-hub (leafs siguen locales)"
docker stop nats-hub >/dev/null

echo "[A] theme=dark"
"$NATS_BIN" --server localhost:4222 kv put config theme dark >/dev/null

echo "[B] theme=light (concurrente)"
"$NATS_BIN" --server localhost:5222 kv put config theme light >/dev/null

echo "Reparando partición: arrancando nats-hub"
docker start nats-hub >/dev/null

echo "Forzando reconexión de clientes: reiniciando nats-a y nats-b (los datos se mantienen al hacer restart)"
docker restart nats-a nats-b >/dev/null

echo "Esperando 5s para que leaf nodes y agentes reconecten y ejecuten reconcile..."
sleep 5


echo "[A] get theme"
"$NATS_BIN" --server localhost:4222 kv get config theme

echo "[B] get theme"
"$NATS_BIN" --server localhost:5222 kv get config theme

echo "OK (deben coincidir según LWW)"
