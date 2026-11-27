#!/usr/bin/env bash
# Stops the cluster and deletes ALL data, logs and pid files (fresh start).
source "$(dirname "$0")/env.sh"
scripts/stop-cluster.sh >/dev/null 2>&1
rm -rf data logs run
echo "cluster data wiped"
