#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
title "DEMO 4 - DISTRIBUTED TRANSACTIONS (Two-Phase Commit across warehouses)"
mark router

step "A customer buys 15 laptops. No single warehouse has 15 -> the order spans several warehouses (shards)"
run STOCK LAPTOP
run BUY LAPTOP 15
run STOCK LAPTOP
logs router "2PC"
pause

step "Atomicity: MUMBAI can supply its line but DELHI cannot (only a few phones) -> the whole order aborts"
run ORDER PHONE:2@MUMBAI PHONE:500@DELHI
run STOCK PHONE
echo "    -> MUMBAI voted YES and reserved 2 phones, but the global decision was ABORT, so the reservation"
echo "       was released (RESERVED=0, quantity unchanged): all-or-nothing"
pause

step "Stock transfer between warehouses (debit + credit must both happen)"
run TRANSFER PHONE 5 MUMBAI PUNE
run STOCK PHONE
pause

step "A participant fails during PREPARE (injected): PUNE votes NO -> global ABORT"
PP=$(primary_of PUNE)
run NODE $PP FAIL vote_no
run ORDER PEN:1@MUMBAI PEN:1@PUNE PEN:1@DELHI
pause

step "Isolation under concurrency: 25 customers race for the 15 headphones in PUNE"
run STOCK HEADPHONE
C bench 25 25 BUY HEADPHONE 1
run STOCK HEADPHONE
echo "    -> exactly 15 committed, the rest failed/aborted; stock never goes negative (escrow reservation at PREPARE)"
pause

run TXLOG 8
run AUDIT
