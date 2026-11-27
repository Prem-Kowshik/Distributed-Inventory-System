#!/usr/bin/env bash
# Restarts a node (or the router) in the background, appending to its log.  usage: scripts/restart-node.sh m1 | router
source "$(dirname "$0")/env.sh"
mkdir -p logs run
if [ "$1" = "router" ]; then
  nohup java $JAVA_OPTS -cp out com.inventory.router.Router "$CONF" >> logs/router.log 2>&1 &
else
  nohup java $JAVA_OPTS -cp out com.inventory.node.StorageNode "$CONF" "$1" >> logs/$1.log 2>&1 &
fi
echo "restarted $1 (pid $!)"
