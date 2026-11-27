#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
title "DEMO 2 - FAULT TOLERANCE (failure detection + automatic failover)"
P=$(primary_of MUMBAI)
run STATUS
run STOCK PHONE
mark router
pause

step "kill -9 the MUMBAI primary ($P): simulates a server crash / power loss"
kill9 $P
wait_s 3
logs router "DOWN|FAILOVER|ELECTION"
run STATUS
NP=$(primary_of MUMBAI)
echo "    New primary is $NP with a higher epoch. No acknowledged write was lost:"
run STOCK PHONE
pause

step "The warehouse keeps accepting writes on the new primary"
run RESTOCK MUMBAI PHONE 10
run AUDIT
pause

step "Old primary comes back. It must NOT act as primary any more (epoch fencing) -> rejoins as BACKUP"
mark $P
start $P
wait_s 4
logs $P "RECOVERY|replayed|WAL tail|BACKUP|following|TRANSFER|in-doubt|replicated"
run STATUS
run CHECK
