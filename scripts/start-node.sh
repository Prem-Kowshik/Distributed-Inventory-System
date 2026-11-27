#!/usr/bin/env bash
# Runs ONE storage node in the foreground (use one terminal per node).   usage: scripts/start-node.sh m1
source "$(dirname "$0")/env.sh"
exec java $JAVA_OPTS -cp out com.inventory.node.StorageNode "$CONF" "$1"
