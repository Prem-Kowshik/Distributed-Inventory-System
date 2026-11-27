#!/usr/bin/env bash
# Runs the router/coordinator in the foreground.
source "$(dirname "$0")/env.sh"
exec java $JAVA_OPTS -cp out com.inventory.router.Router "$CONF"
