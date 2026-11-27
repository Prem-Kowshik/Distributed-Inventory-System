#!/usr/bin/env bash
# Simulates a torn write: appends half a record (no checksum, no newline) to a node's WAL.
# Run it while the node is DOWN, then restart the node and watch recovery truncate it.
source "$(dirname "$0")/env.sh"
printf '999\t9\t9\tRESTOCK LAPTOP 50' >> data/$1/wal.log
echo "appended a torn record to data/$1/wal.log"
