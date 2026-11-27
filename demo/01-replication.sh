#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
title "DEMO 1 - REPLICATION (primary-backup, WAL log shipping)"
P=$(primary_of MUMBAI); B=($(backups_of MUMBAI))
echo "MUMBAI warehouse: primary=$P, backups=${B[*]}"
mark $P ${B[0]} ${B[1]}

step "A write goes to the primary, is appended to its WAL, then shipped to both backups before the ack"
run RESTOCK MUMBAI LAPTOP 5
logs $P "write" 1
logs ${B[0]} "replicated" 1
step "Every replica now holds the same data (same LSN, same table hash)"
run CHECK
run NODE ${B[0]} STATUS
pause

step "Reads can be served by any replica - the backups have the new value"
run STOCK LAPTOP
run STOCK LAPTOP
pause

step "Synchronous vs asynchronous replication"
run MODE async
run RESTOCK MUMBAI PHONE 1
echo "    ^ acked immediately (replicas_acked=async): faster, but a crash right now could lose it"
run MODE sync
run RESTOCK MUMBAI PHONE 1
echo "    ^ acked only after both backups have it (replicas_acked=2/2)"
wait_s 1
run CHECK
