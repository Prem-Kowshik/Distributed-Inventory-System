#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
title "DEMO 6 - LOAD BALANCING (reads spread across replicas)"

step "Round robin: 90 STOCK queries from 6 concurrent clients"
run LB rr
run STATS reset
C bench 90 6 STOCK LAPTOP
run STATS
pause

step "Baseline - no balancing (everything hits the primary)"
run STATS reset
run LB primary
C bench 90 6 STOCK LAPTOP
run STATS
pause

M=($(backups_of MUMBAI))
step "A slow replica: add 300 ms latency to ${M[0]}. Compare round-robin vs least-connections"
run NODE ${M[0]} DELAY 300
run LB rr
run STATS reset
C bench 90 6 STOCK LAPTOP
run STATS
run LB leastconn
run STATS reset
C bench 90 6 STOCK LAPTOP
run STATS
echo "    -> least-connections routes around the slow node (fewer reads, lower latency)"
run NODE ${M[0]} DELAY 0
pause

step "Replica failure: reads are redistributed automatically"
run LB rr
kill9 ${M[1]}
wait_s 2
run STATS reset
C bench 60 6 STOCK LAPTOP
run STATS
start ${M[1]}
wait_s 3

step "Data-level balancing: BUY allocates from the warehouse with the most stock first"
run STOCK PEN
run BUY PEN 6000
run STOCK PEN
