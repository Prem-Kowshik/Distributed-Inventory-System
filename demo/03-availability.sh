#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
title "DEMO 3 - AVAILABILITY (service continues during failures)"

step "Continuous traffic (70% reads / 30% 2PC BUYs) for 25 s; we kill PUNE's primary at t=6s and another PUNE node at t=14s"
P=$(primary_of PUNE)
C loadgen 25 10 PEN &
LG=$!
sleep 6;  echo "    >>> t=6s: kill -9 $P (PUNE primary)"; kill9 $P
sleep 8;  B=$(backups_of PUNE | head -1); echo "    >>> t=14s: kill -9 $B (another PUNE replica, only 1 of 3 left)"; kill9 $B
wait $LG
run STATUS
echo "    -> requests during the failover were delayed (~1.5-2 s) but not failed: the router waits for the new primary."
pause

step "Whole warehouse outage: kill every DELHI replica"
for d in $(C -c STATUS | awk '$1=="DELHI" && $3=="UP"{sub(/\*$/,"",$2); print $2}'); do kill9 $d; done
wait_s 2
run STOCK LAPTOP
echo "    -> DELHI is unavailable, but MUMBAI and PUNE still serve reads and orders (graceful degradation)"
run BUY LAPTOP 3
pause

step "CAP trade-off: in QUORUM mode a write needs a majority (2 of 3). PUNE has only 1 live replica."
run MODE quorum
wait_s 2
run RESTOCK PUNE PEN 5
echo "    -> rejected (consistency over availability). Reads still work:"
run INVENTORY PUNE
run MODE sync
run RESTOCK PUNE PEN 5
echo "    -> in SYNC (ISR) mode the single survivor accepts writes (availability over durability)."
pause

step "Bring everything back"
for id in $P $B d1 d2 d3; do start $id; done
wait_s 5
run STATUS
run CHECK
run AUDIT
