#!/usr/bin/env bash
set -euo pipefail

./scripts/98_stop_agents.sh >/dev/null 2>&1 || true
docker compose down -v || true
rm -rf logs || true
echo "OK: entorno parado y logs eliminados."
