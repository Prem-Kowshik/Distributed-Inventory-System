#!/usr/bin/env bash
# Fresh cluster: wipe data, build, start 9 nodes + router in background, load the catalogue.
cd "$(dirname "$0")/.."
scripts/reset.sh
./build.sh
scripts/start-cluster.sh
java -cp out com.inventory.client.Client -f demo/seed.txt 2>/dev/null
