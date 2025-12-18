#!/usr/bin/env bash
set -euo pipefail

# Robust nats CLI resolution (works even under sudo where PATH may be reset)
resolve_nats() {
  # 1) If nats is already in PATH
  if command -v nats >/dev/null 2>&1; then
    command -v nats
    return 0
  fi

  # 2) Common global locations
  for p in /usr/local/bin/nats /usr/bin/nats /bin/nats; do
    if [[ -x "$p" ]]; then
      echo "$p"
      return 0
    fi
  done

  # 3) If running under sudo, try the invoking user's Go bin
  if [[ -n "${SUDO_USER:-}" ]]; then
    for p in "/home/${SUDO_USER}/go/bin/nats" "/home/${SUDO_USER}/.local/bin/nats"; do
      if [[ -x "$p" ]]; then
        echo "$p"
        return 0
      fi
    done
  fi

  # 4) Fallback: current user's Go bin
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
  echo "  - Ejecuta el script SIN sudo: ./scripts/02_test_leaf_basic.sh" >&2
  echo "  - O bien instala/enlaza nats en /usr/local/bin:" >&2
  echo "      sudo ln -sf "$(go env GOPATH)/bin/nats" /usr/local/bin/nats" >&2
  exit 127
fi

echo "[A] put theme=dark"
"$NATS_BIN" --server localhost:4222 kv put config theme dark >/dev/null

echo "[B] get theme (debe ser dark)"
"$NATS_BIN" --server localhost:5222 kv get config theme

echo "OK"
