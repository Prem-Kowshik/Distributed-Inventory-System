#!/usr/bin/env bash
# Starts all 9 storage nodes + router in the BACKGROUND. Output goes to logs/<id>.log
source "$(dirname "$0")/env.sh"
mkdir -p logs run
[ -d out ] || ./build.sh
for id in $(node_ids); do
  if [ -f run/$id.pid ] && kill -0 "$(cat run/$id.pid)" 2>/dev/null; then echo "  $id already running"; continue; fi
  nohup java $JAVA_OPTS -cp out com.inventory.node.StorageNode "$CONF" "$id" >> logs/$id.log 2>&1 &
  echo "  started node $id (pid $!)"
done
if ! ( [ -f run/router.pid ] && kill -0 "$(cat run/router.pid)" 2>/dev/null ); then
  nohup java $JAVA_OPTS -cp out com.inventory.router.Router "$CONF" >> logs/router.log 2>&1 &
  echo "  started router (pid $!)"
fi
echo "waiting for elections..."
for i in $(seq 1 40); do
  sleep 0.5
  n=$(java -cp out com.inventory.client.Client -c STATUS 2>/dev/null | grep -c ' PRIMARY ')
  [ "$n" -ge 3 ] && break
done
java -cp out com.inventory.client.Client -c STATUS
echo
echo "Logs: tail -f logs/router.log logs/m1.log    |   Client: scripts/client.sh"
