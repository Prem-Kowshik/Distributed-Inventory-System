# Design: Multi-Warehouse Distributed Inventory DBMS

## 1. Problem

An e-commerce company stores stock in several regional warehouses. The database must meet six goals:

- Keep each warehouse's data close to it. This is **fragmentation** (sharding).
- Survive server crashes without losing acknowledged updates. This needs **replication** and **fault tolerance**.
- Keep serving customers while failures happen. This is **availability**.
- Process an order that takes items from several warehouses atomically. This needs **distributed transactions**.
- Restart crashed servers in a correct state. This is **recovery**.
- Spread read traffic across replicas. This is **load balancing**.

## 2. Architecture

```
                         +-----------------------------------------+
   clients  ---TCP--->   |  ROUTER (port 7000)                     |
 (CLI/loadgen)           |  - query routing (shard = warehouse)    |
                         |  - read load balancer (rr/leastconn/..) |
                         |  - 2PC coordinator + coordinator.log    |
                         |  - failure detector + failover manager  |
                         +-----+----------------+----------------+-+
                               |                |                |
              MUMBAI shard     |   PUNE shard   |   DELHI shard  |
          +--------------------+  +-------------+--+  +----------+------+
          | m1* PRIMARY        |  | p1* PRIMARY    |  | d1* PRIMARY     |
          |   | WAL shipping   |  |   |            |  |   |             |
          |   v                |  |   v            |  |   v             |
          | m2 BACKUP m3 BACKUP|  | p2      p3     |  | d2      d3      |
          +--------------------+  +----------------+  +-----------------+
   each node: in-memory tables + write-ahead log (fsync) + periodic snapshot on its own disk dir
```

- **Fragmentation.** The data is fragmented horizontally by `warehouse`. Each product row lives in the warehouse that physically holds the stock. One SKU, such as LAPTOP, can exist in several fragments with different quantities.
- **Allocation.** Each fragment is fully replicated on 3 nodes. A cluster has 9 storage nodes plus 1 router, and every one of them is a separate OS process (JVM).
- **Communication.** Plain TCP with a text protocol, one request per connection. A single `Net.ConnectFailed` exception tells the caller "request not delivered", which is the one case where blind retries are safe.

### Schema at each node (`InventoryState`)

| Table | Columns | Purpose |
|---|---|---|
| products | sku (PK), name, price, qty, reserved, received, shipped, transferNet | stock; `available = qty - reserved` |
| prepared_tx | txid (PK), items | 2PC participants' prepared state; part of the durable state |
| tx_outcomes | txid (PK), COMMIT/ABORT | recent decisions; makes COMMIT and ABORT idempotent |

## 3. Storage engine and write-ahead log

The node follows the WAL rule. Every change is (1) validated, (2) appended to `wal.log` and **fsync'd**, (3) applied to the in-memory tables, and (4) shipped to the replicas. Only after these steps is the client acknowledged.

A log record is the line `lsn \t epoch \t prevEpoch \t payload \t crc32`. The payload is a **logical** operation:

| Operation | Effect |
|---|---|
| `ADD sku name qty price` | create product |
| `RESTOCK sku delta` | qty += delta (received += delta) |
| `ENSURE sku name price` | create product row if absent (used before a transfer) |
| `PREP tx sku:delta:kind,...` | 2PC prepare: reserved += quantities taken; tx enters prepared_tx |
| `COMMIT tx` / `ABORT tx` | apply or release the reservation; record the outcome |
| `NOOP ...` | written by a newly promoted primary to start its epoch |

The state is a **deterministic state machine**. Applying the same log in the same order produces identical tables. Replication relies on this, and `CHECK` verifies it by comparing CRC hashes of the tables.

**Checkpointing.** Every `checkpoint.every` records (default 50), or when the user runs `CHECKPOINT`, the node does two things. It writes a snapshot of all tables to `snapshot.tmp` and atomically renames it to `snapshot.dat`. Then it truncates the WAL. Recovery time is therefore bounded by the snapshot size plus at most 50 log records.

## 4. Replication

- The scheme is **primary-backup with log shipping**. Only the primary accepts writes, and it ships each WAL record to the backups in LSN order. Each follower has its own single-thread executor, so order is preserved.
- Each record carries its `prevEpoch`. A follower accepts record `n` only if its last record is `n-1` with that epoch. This is the same log-matching check Raft uses. Otherwise the follower replies `NEED lastLsn lastEpoch`.
- **Catch-up.** If the primary still holds the missing records in its log, it ships them (incremental catch-up). If they were checkpointed away, or the follower's log *diverged* (for example an old primary with an unreplicated tail), the primary ships a full **snapshot (state transfer)** and the follower replaces its state.
- A replication heartbeat every second lets an idle follower discover it is behind.
- **ISR (in-sync replica set).** The primary tracks which followers acknowledged recently. A follower that times out is removed from the ISR and re-added after it catches up.

| Mode | Ack rule | Trade-off |
|---|---|---|
| `sync` (default) | wait for all ISR members | survives any failure that leaves one ISR member; stays writable with 1 node |
| `quorum` | refuse the write unless a majority (2 of 3) is reachable | CP: never acknowledges a write that a single crash could lose; unavailable for writes in a minority |
| `async` | acknowledge after the local fsync | lowest latency; the last writes can be lost on failover |

## 5. Fault tolerance: failure detection and failover

- **Detection.** The router PINGs every node every 500 ms. After 3 consecutive misses (about 1.5 s) the node is DOWN. A PING reply carries the node's role, epoch, last LSN, the primary it follows, and its in-doubt count.
- **Failover.** If a warehouse's primary is DOWN, the router picks the live replica with the **highest LSN**, so no acknowledged write is lost. It sends that replica `PROMOTE epoch+1` and tells the other live replicas to `FOLLOW` the new primary.
- **Epochs.** Epochs (also called terms) are persisted by the router (`data/router/epochs`) and by every node (`data/<id>/epoch`), so they never go backwards.
- **Fencing.** A node rejects any replication message whose epoch is lower than its own (`STALE`). A primary that sees `STALE` steps down. A restarted old primary always starts as BACKUP and is told to FOLLOW. The result: an old ("zombie") primary can never overwrite the new one, even if it was only network-partitioned.
- **Measured outage.** About 1.5 s, which is mostly detection time. Promotion itself takes 20-80 ms.

## 6. Availability

- During a failover the router does not return an error. A write waits (up to 6 s) for the new primary and is then retried. Retries happen only after a *connect* failure, because then the request was certainly not delivered. If the request was sent but no reply came, the router reports "outcome UNKNOWN" instead of risking a duplicate. 2PC messages are idempotent, so those are always retried.
- Reads are served by any replica, so reads continue even while the primary is down.
- **Partial availability.** If an entire warehouse is down, the other warehouses keep serving, and `BUY` allocates stock from the live warehouses.
- **CAP.** Under a partition you choose between `MODE quorum` (consistency first) and `MODE sync` (availability first). Demo 3 shows both.
- In our test, loadgen measured **100% availability with a worst-case latency of about 1.2 s** while two of PUNE's three nodes were killed.

## 7. Distributed transactions: Two-Phase Commit

The router is the coordinator. The warehouse primaries are the participants.

```
Coordinator                         Participant (warehouse primary)
 log BEGIN T (fsync)
 --- PREPARE T items ------------->  validate available >= requested
                                     log PREP T (fsync + replicate to backups)
                                     reserved += items          (escrow lock)
 <-- VOTE YES / NO ----------------
 log DECISION T COMMIT|ABORT (fsync)   <- COMMIT POINT
 --- COMMIT/ABORT T -------------->  log COMMIT/ABORT T; qty -= items, reserved -= items
 <-- OK ---------------------------
 log END T
```

- **Concurrency control.** We use escrow (reservation) locking instead of long-held 2PL row locks. PREPARE moves units from `available` into `reserved`. Many orders can prepare against the same SKU at once, as long as the total stays within stock. This gives no deadlocks and no lost updates, and stock never goes negative. In the demo, 25 concurrent buyers compete for 15 units and exactly 15 commit.
- **Prepare durability.** The PREP record is replicated before the vote is sent. If the participant's primary crashes after voting, the new primary already knows the transaction is prepared. Demo 5C shows this.
- **Presumed abort.** A transaction with BEGIN but no DECISION in the coordinator log is aborted after a coordinator crash.
- **Re-drive.** A DECISION without END is re-sent until every participant acknowledges, even across participant failovers.
- **Termination protocol / blocking.** A participant that has stayed prepared for more than 5 s asks the coordinator with `TXSTATUS`. If the coordinator is down, the participant must stay blocked with its stock reserved, because it cannot decide alone. This is the well-known blocking weakness of 2PC (3PC, or Paxos-Commit with a replicated coordinator, addresses it). Demo 5D shows the blocking and its resolution.

## 8. Recovery

**Storage node restart (`StorageNode.recover`):**

1. Load `snapshot.dat` (the last checkpoint).
2. Read `wal.log`. Verify each record's CRC32 and LSN continuity. Stop at the first bad record, which is a torn write from a crash mid-append, and **truncate** the file there.
3. **REDO** every record after the snapshot's LSN.
4. Transactions still in `prepared_tx` are **in-doubt**. Their stock stays reserved until the decision is learned from the coordinator or from the primary's log.
5. Restart as BACKUP. The router sends FOLLOW, and the primary brings the node up to date by catch-up or state transfer.

No UNDO pass is needed. The tables live in memory and are rebuilt from the log, so uncommitted changes never reach disk before their log record (no-steal). The "undo" of a prepared-but-aborted transaction is the logical ABORT record, which releases the reservation.

**Router restart (`TxCoordinator.recover`):**

1. Replay `coordinator.log`.
2. Apply presumed abort for undecided transactions.
3. Re-drive decided but unfinished transactions.
4. Reload the epochs and adopt the existing primaries.

## 9. Load balancing

| Policy | Behaviour | Use |
|---|---|---|
| `rr` | round robin over in-sync replicas | even spread (about 33% each) |
| `leastconn` | fewest in-flight requests, then fewest served | adapts to a slow or overloaded replica |
| `random` | uniform | stateless alternative |
| `primary` | all reads to the primary | baseline without balancing |

- Writes always go to the primary.
- Replicas that are down or not following the current primary are excluded automatically.
- **Data-level balancing.** `BUY` allocates units from the warehouse with the most available stock first, or from a preferred warehouse if one is given.

## 10. Correctness checks built into the system

- `AUDIT` checks the global invariant, for every SKU, `on_hand = received - shipped`, with transfers netting to zero. It reads from the primaries of all warehouses.
- `CHECK` verifies that replicas at the same LSN have identical table hashes.

## 11. Limitations and possible extensions

- The router is a single control process. Its state (epochs, coordinator log) is durable, so a restart recovers correctly, but while it is down no failover or new transactions can happen. The fix is to replicate the router with Raft or ZooKeeper, and to use Paxos-Commit for the coordinator.
- Reads from backups in `async` mode can be stale. In `sync` mode they reflect every acknowledged write.
- Nodes cannot be added or removed at runtime. Possible extensions are dynamic membership and consistent-hash re-sharding.
- The quorum check happens before the write; a fully atomic version would commit only after the majority acknowledges, as Raft does.

## 12. Likely viva questions

- **Why 3 replicas?** A majority (2 of 3) can tolerate 1 failure. With quorum mode you get consistency, and with sync/ISR mode you keep write availability down to 1 node.
- **How do you avoid split brain?** With epochs and fencing. Promotion always uses a new, higher epoch, and nodes reject any message from a lower epoch.
- **What if a primary crashes after a client got an ACK?** In sync mode the ACK is sent only after every ISR backup has the record. The router promotes the highest-LSN live replica, which has the record.
- **What happens if the coordinator crashes between the phases?** Before the DECISION is logged, the transaction is presumed aborted. After the DECISION is logged, the router re-drives it on restart. In between, participants block with stock reserved.
- **Why log PREPARE before voting YES?** A YES is a promise to commit if asked. The promise must survive a participant crash, so it is logged and replicated before the vote.
- **Difference between replication and the WAL?** The WAL gives durability on one node, for crash recovery. Replication ships the same WAL to other nodes to survive the loss of the whole node, for fault tolerance and availability.
