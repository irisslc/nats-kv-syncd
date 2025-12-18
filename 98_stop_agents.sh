#!/usr/bin/env bash
set -euo pipefail

kill_if_pidfile () {
  local f="$1"
  if [[ -f "$f" ]]; then
    local pid
    pid="$(cat "$f" || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      sleep 0.3
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$f"
  fi
}

mkdir -p logs
kill_if_pidfile "logs/site-a.pid"
kill_if_pidfile "logs/site-b.pid"

echo "OK: agentes parados (si estaban corriendo)."
