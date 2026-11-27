#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
title "DEMO 5 - RECOVERY (WAL redo, torn writes, catch-up, checkpoints, 2PC recovery)"

# ---------------------------------------------------------------- A
B=$(backups_of DELHI | head -1)
step "A) Crash a DELHI backup ($B); writes continue without it"
kill9 $B
run RESTOCK DELHI LAPTOP 2
run RESTOCK DELHI PHONE 3
run BUY PHONE 1 DELHI
step "   Simulate a torn write: the crash happened in the middle of appending a WAL record"
scripts/corrupt-wal.sh $B | sed 's/^/    /'
mark $B
start $B
wait_s 4
logs $B "RECOVERY|checkpoint|WAL|state|in-doubt|recovery took|BACKUP|catch|TRANSFER"
P=$(primary_of DELHI); mark $P
run CHECK
pause

# ---------------------------------------------------------------- B
step "B) Checkpoint: snapshot the tables and truncate the WAL (bounded recovery time)"
run CHECKPOINT
ls -l data/$B/ | sed 's/^/    /'
pause

# ---------------------------------------------------------------- C
step "C) Participant crash after PREPARE: the prepare record is durable AND replicated"
DP=$(primary_of DELHI); mark router
run NODE $DP FAIL crash_after_prepare
run ORDER PEN:2@MUMBAI PHONE:1@DELHI
logs router "2PC|FAILOVER|retry|DOWN"
echo "    -> DELHI's primary died after voting; the coordinator retried on the new DELHI primary, which already"
echo "       had the PREPARE record (replicated) and voted YES. The transaction committed."
start $DP
pause

# ---------------------------------------------------------------- D
step "D) Coordinator crash AFTER logging COMMIT but BEFORE telling participants"
MP=$(primary_of MUMBAI); PP=$(primary_of PUNE); mark $MP $PP
run FAILPOINT crash_after_decision
run ORDER PHONE:1@MUMBAI PEN:3@PUNE
echo "    (router is dead now). Participants are prepared and IN-DOUBT, stock stays reserved = 2PC blocking:"
wait_s 7
logs $MP "in-doubt|IN-DOUBT|PREPARED" 3
mark router
echo "    restart the router: it reads its coordinator log and re-drives COMMIT"
start router
wait_s 5
logs router "recovery|redriv|re-driv|delivered|coordinator log"
run TXLOG 3
pause

# ---------------------------------------------------------------- E
step "E) Coordinator crash BEFORE a decision -> presumed abort on restart, reservations released"
run FAILPOINT crash_after_prepare
run ORDER PEN:7@MUMBAI PEN:7@DELHI
wait_s 1
mark router
start router
wait_s 5
logs router "presumed|recovery|delivered"
run STOCK PEN
run AUDIT
run CHECK
