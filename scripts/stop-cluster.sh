#!/usr/bin/env bash
# Stops every process started by start-cluster.sh
source "$(dirname "$0")/env.sh"
for f in run/*.pid; do [ -f "$f" ] || continue; kill "$(cat "$f")" 2>/dev/null && echo "  stopped $(basename "$f" .pid)"; rm -f "$f"; done
