#!/usr/bin/env bash
# Starts the web demo console (http://localhost:8080). Builds first if needed.
# From the page you can start / kill -9 / restart every node and the router.
#   usage: scripts/web.sh [httpPort]
source "$(dirname "$0")/env.sh"
[ -f out/com/inventory/web/WebConsole.class ] || ./build.sh
exec java -Xmx128m -cp out com.inventory.web.WebConsole "$CONF" "${1:-8080}"
