#!/usr/bin/env bash
# Hard-kills a process (kill -9 = power failure, no cleanup).  usage: scripts/kill-node.sh m1 | router
source "$(dirname "$0")/env.sh"
[ -f run/$1.pid ] || { echo "no pid file for $1"; exit 1; }
kill -9 "$(cat run/$1.pid)" 2>/dev/null && echo "killed $1 (kill -9 pid $(cat run/$1.pid))"
rm -f run/$1.pid
