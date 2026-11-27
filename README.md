# Multi-Warehouse Distributed Inventory DBMS

This is a distributed database for an e-commerce company. The company has three warehouses: **MUMBAI**, **PUNE** and **DELHI**.

- Each warehouse is a **shard**, meaning a horizontal fragment of the inventory.
- Each shard is **replicated on 3 storage nodes**.
- A **router** sits in front of the nodes. It does query routing, read load balancing, failure detection, failover, and coordinates Two-Phase Commit (2PC).

The project is written in pure Java 17. It has **no external libraries**: the storage engine, write-ahead log, replication, failover, 2PC, checkpoints and recovery are all implemented from scratch.

| Demonstration topic | Where it is implemented | Demo |
|---|---|---|
| Replication | Primary-backup WAL shipping, ISR, sync / quorum / async modes (`Replicator`, `StorageNode.write`) | `demo/01-replication.sh` |
| Fault tolerance | Heartbeat failure detector, automatic failover, epoch fencing (`ClusterManager`) | `demo/02-fault-tolerance.sh` |
| Availability | Router rides through failovers, reads served by backups, partial (per-warehouse) outage, CAP quorum mode | `demo/03-availability.sh` |
| Distributed transactions | 2PC across warehouses, escrow reservation, presumed abort (`TxCoordinator`) | `demo/04-distributed-transactions.sh` |
| Recovery | Checkpoint + WAL redo, torn-write detection, catch-up / state transfer, 2PC in-doubt resolution, coordinator log recovery | `demo/05-recovery.sh` |
| Load balancing | Round-robin / least-connections / random read routing; stock-aware order allocation (`LoadBalancer`) | `demo/06-load-balancing.sh` |

`docs/DESIGN.md` explains the architecture and algorithms. Use it as the basis for your project report and for viva preparation.

---

## 0. Web demo console (recommended for the presentation)

A browser UI that drives the **real** cluster. Nothing is simulated: every number, role, epoch, LSN and log line on screen comes from the running processes.

```bash
scripts/web.sh            # Windows: scripts\web.bat      (builds first if needed)
# open http://localhost:8080  ->  click "Fresh start"
```

`Fresh start` wipes `data/ logs/ run/`, starts the 9 nodes and the router, waits for the elections and loads `demo/seed.txt`.

| Area | What it shows |
|---|---|
| **Cluster topology** | Router + 3 shards × 3 replicas. Crown = primary. Live epoch / LSN / reads per node. Packets are drawn from observed LSN and read-counter deltas: white = write to primary, ring = WAL shipping to backups, green = load-balanced read, violet = 2PC. A killed node turns amber (missed heartbeats) and then red (DOWN). Per node: ⏻ kill -9 / ▶ restart, and ⋯ for `CRASH`, `DELAY`, `FAIL vote_no / crash_after_prepare`, corrupt WAL, recovery report. ⋯ on the router arms the coordinator failpoints. |
| **Event stream** | Live tail of `logs/router.log` (or any node's log), filterable: failover / 2PC / warnings. |
| **Tabs 01–06** | One per demonstration topic: concept tags, the classes that implement it, a live widget, and the steps from section 6 as buttons, each with an "Expect:" line. |
| **Console** | Any client command (`STATUS`, `CHECK`, `ORDER …`), ↑/↓ history. |

Widgets: **01** replica LSN meters and a write form · **02** failover stopwatch (detection / outage / new primary, from the router's own measurements) and epoch history · **03** browser loadgen with per-second chart, availability % and worst latency · **04** animated 2PC sequence diagram built from the real votes and decision (including coordinator-crash variants) and a 25-customer concurrency grid · **05** recovery report parsed from the node or router log (checkpoint → redo → CRC / torn write → in-doubt → fencing → catch-up / state transfer) · **06** live reads-per-replica bars and a benchmark history table to compare policies.

Keys `1`–`6` switch tabs. Stopping the web console (Ctrl+C) also stops every process it started.

How it works: browsers cannot speak the raw TCP protocol, so `com.inventory.web.WebConsole` (JDK built-in `com.sun.net.httpserver`, still no external libraries) forwards commands to the router, starts and kills the processes exactly like `scripts/` (same pid files and log files), tails logs and runs the concurrent benchmarks. The frontend is static: `web/index.html`, `web/app.css`, `web/app.js`.

| Endpoint | Purpose |
|---|---|
| `POST /api/cmd` (body = command) | forward a client command to the router |
| `GET /api/state` | router `STATUS` + which OS processes are alive |
| `POST /api/proc?action=start\|kill\|corrupt&id=m1` | process control (kill = SIGKILL) |
| `POST /api/cluster?action=start\|stop\|reset\|seed` | whole-cluster control |
| `POST /api/bench?n=25&t=25&cmd=…` | n requests from t concurrent clients |
| `GET /api/logs?id=router&from=<offset>` | incremental log tail |

The terminal workflow below still works and can be mixed with the web console.

---

## 1. Requirements

- **JDK 17 or newer.** Check with `java -version` and `javac -version`.
  - Windows / Mac / Linux: install "Eclipse Temurin 17" or "21" from adoptium.net.
  - Ubuntu: `sudo apt install openjdk-17-jdk`
- About 1 GB of free RAM. Ten small JVMs run at once (9 nodes + router), each using at most 96 MB of heap.
- Ports **7000, 7101-7103, 7201-7203, 7301-7303** must be free on your laptop.

No Maven, Gradle or internet connection is needed.

---

## 2. Project layout

```
config/cluster.conf          cluster topology + settings (ports, replication mode, heartbeat...)
src/com/inventory/
  common/   Config, Net (TCP RPC), Log
  node/     StorageNode (one replica), InventoryState (tables), WriteAheadLog, LogRecord, Replicator
  router/   Router (gateway), ClusterManager (failure detector/failover), LoadBalancer, TxCoordinator (2PC)
  client/   Client (shell, command files, load generator, benchmark)
  web/      WebConsole (HTTP bridge for the browser demo)
web/        index.html, app.css, app.js  (browser demo console)
scripts/    build/start/stop/kill/restart scripts  (.sh for Linux/Mac, .bat for Windows)
demo/       seed.txt + one automated demo script per topic
docs/       DESIGN.md (architecture, algorithms, viva Q&A)
data/       created at runtime: data/<node>/wal.log, snapshot.dat, epoch;  data/router/coordinator.log
logs/       created at runtime (background mode): one log per process
```

---

## 3. Build

```bash
# Linux / Mac / Git-Bash / WSL
./build.sh

# Windows (cmd or PowerShell)
build.bat
```

The compiled classes go into `out/`.

---

## 4. Run the cluster: choose one way

### Option A: one terminal per server (best for a live demo)

This shows each server's log live, and you can crash a server by pressing **Ctrl+C** in its window.

Open **11 terminals**, `cd` into the project folder in each one, and run:

| Terminal | Linux / Mac | Windows |
|---|---|---|
| 1-3 | `scripts/start-node.sh m1` / `m2` / `m3` | `scripts\start-node.bat m1` / `m2` / `m3` |
| 4-6 | `scripts/start-node.sh p1` / `p2` / `p3` | `scripts\start-node.bat p1` ... |
| 7-9 | `scripts/start-node.sh d1` / `d2` / `d3` | `scripts\start-node.bat d1` ... |
| 10 | `scripts/start-router.sh` | `scripts\start-router.bat` |
| 11 (client) | `scripts/client.sh` | `scripts\client.bat` |

Shortcuts:
- **Windows:** `scripts\start-cluster.bat` opens all 10 server windows for you.
- **Linux/Mac:** `tmux` or a terminal with split panes is handy.

A few seconds after the router starts, it elects one PRIMARY per warehouse. Then load the sample catalogue from the client terminal:

```bash
scripts/client.sh -f demo/seed.txt          # Windows: scripts\client.bat -f demo\seed.txt
```

### Option B: everything in the background, one terminal (Linux/Mac/WSL)

```bash
demo/00-setup.sh          # wipe data, build, start 9 nodes + router in background, load catalogue
tail -f logs/router.log   # (optional, another terminal) watch the router
scripts/client.sh         # interactive client
```

Use `scripts/stop-cluster.sh` to stop everything and `scripts/reset.sh` to stop and delete all data.

To run all six demos one after another without pauses: `demo/run-all.sh`

---

## 5. Client commands

Type `HELP` in the client to see the full list.

```
STATUS                                  cluster table: who is primary, epochs, LSNs, up/down
ADD <WH> <SKU> <NAME> <QTY> <PRICE>     new product in one warehouse
RESTOCK <WH> <SKU> <QTY>                single-shard write
STOCK <SKU>                             read from all warehouses (load-balanced replicas)
INVENTORY <WH>                          list one warehouse
BUY <SKU> <QTY> [WH]                    auto-split order across warehouses -> 2PC
ORDER SKU:QTY@WH [SKU:QTY@WH ...]       explicit multi-warehouse order -> 2PC
TRANSFER <SKU> <QTY> <FROM> <TO>        move stock between warehouses -> 2PC
TXLOG [n] / TXSTATUS <id>               coordinator decisions
AUDIT                                   global invariant: on_hand = received - shipped (no stock lost/created)
CHECK                                   replica consistency: same LSN => identical table hash
STATS [reset]                           reads served per replica
LB rr|leastconn|random|primary          load-balancing policy
MODE sync|quorum|async                  replication mode
CHECKPOINT                              snapshot + truncate WAL on all nodes
NODE <id> STATUS | CRASH | DELAY <ms> | FAIL vote_no|crash_after_prepare|none
FAILPOINT crash_after_prepare|crash_after_decision|none     (router crash points)
```

The client also has non-interactive modes:

```bash
scripts/client.sh -c "STOCK LAPTOP"            # one command
scripts/client.sh -f demo/seed.txt             # command file
scripts/client.sh loadgen 30 10                # 30 s of traffic at 10 ops/s, prints availability
scripts/client.sh bench 25 25 BUY HEADPHONE 1  # 25 concurrent requests
```

---

## 6. Step-by-step test plan (manual; works on any OS)

Start the cluster with Option A and load `seed.txt`. Then follow the steps below.

There are three ways to crash a node:
- press **Ctrl+C** in its window,
- run `scripts/kill-node.sh <id>` (Windows: `scripts\kill-node.bat <id>`), or
- type `NODE <id> CRASH` in the client.

To restart a node, run its start command again in its own window.

### Test 1: Replication
1. `STATUS` shows one `*` primary per warehouse, and all nodes have the same LSN.
2. `RESTOCK MUMBAI LAPTOP 5` returns `replicas_acked=2/2`. In the backup windows you will see `replicated #N from m1: RESTOCK ...`.
3. `CHECK` shows identical hashes on all 3 replicas of each warehouse.
4. Run `STOCK LAPTOP` twice. The NODE column changes, which shows that backups serve reads with the new value.
5. Compare modes. Run `MODE async` then `RESTOCK MUMBAI PHONE 1`, and you get `replicas_acked=async`. Run `MODE sync` to switch back.

### Test 2: Fault tolerance
1. Note the MUMBAI primary from `STATUS`, for example `m1`. Kill it with Ctrl+C or `NODE m1 CRASH`.
2. Within about 1.5 s, the router window prints `NODE m1 is DOWN` and then `FAILOVER MUMBAI: primary m1 -> m2 (epoch 2 ...)`.
3. Run `STATUS`. The new primary has a higher epoch. `STOCK PHONE` still shows every acknowledged write.
4. `RESTOCK MUMBAI PHONE 10` works on the new primary.
5. Restart m1. Its window prints a **CRASH RECOVERY** block, then `role = BACKUP, following primary m2`. The old primary does not come back as primary; this is **epoch fencing**.
6. `CHECK` shows all replicas IDENTICAL.

### Test 3: Availability
1. In a spare terminal, run `scripts/client.sh loadgen 40 10`.
2. While it runs, kill the PUNE primary. The loadgen shows one slow second (about 1.1-1.5 s) and **0 errors**. Then kill a second PUNE node. Traffic continues on the single survivor.
3. The final line reports availability (100% in our runs).
4. Kill all three DELHI nodes. `STOCK LAPTOP` shows `DELHI UNAVAILABLE`, but `BUY LAPTOP 3` still commits from the other warehouses.
5. CAP trade-off:
   - With 1 of 3 PUNE nodes alive, run `MODE quorum` and wait 2 s, then `RESTOCK PUNE PEN 5`. The write is rejected with `ERR NO_QUORUM`.
   - Run `MODE sync` and repeat the RESTOCK. The write is accepted.
6. Restart all killed nodes. Run `CHECK` and `AUDIT`.

### Test 4: Distributed transactions (2PC)
1. `STOCK LAPTOP`, then `BUY LAPTOP 15`. The order spans two warehouses: both vote YES, the decision is COMMIT, and the router window shows each 2PC phase.
2. `ORDER PHONE:2@MUMBAI PHONE:500@DELHI`. MUMBAI votes YES, DELHI votes NO, so the whole order is **ABORTED**. `STOCK PHONE` shows MUMBAI's reservation was released.
3. `TRANSFER PHONE 5 MUMBAI PUNE` is an atomic debit plus credit.
4. Inject a participant failure:
   - `NODE <pune-primary> FAIL vote_no`
   - `ORDER PEN:1@MUMBAI PEN:1@PUNE PEN:1@DELHI`
   - The result is ABORT.
5. Concurrency: `scripts/client.sh bench 25 25 BUY HEADPHONE 1`. 25 parallel customers compete for 15 headphones: **exactly 15 commit**, and stock never goes negative.
6. `AUDIT` reports CONSISTENT. `TXLOG` shows the decisions.

### Test 5: Recovery
1. **WAL redo and torn write.**
   - Kill a DELHI backup (e.g. d2) and run a few `RESTOCK DELHI ...` commands.
   - Run `scripts/corrupt-wal.sh d2` (Windows: `scripts\corrupt-wal.bat d2`) and restart d2.
   - Its recovery block shows: checkpoint loaded → N records replayed → `torn write ... truncated` → catch-up from the primary.
   - `CHECK` shows IDENTICAL.
2. **Checkpoint.** Run `CHECKPOINT`. `data/<node>/wal.log` becomes empty and `snapshot.dat` holds the tables.
3. **Too far behind.** Kill a backup and run more than 50 writes, e.g. `bench 60 4 RESTOCK PUNE LAPTOP 1`, then restart it. The primary logs `STATE TRANSFER -> p1 ... shipping full snapshot`.
4. **Participant crash after PREPARE.**
   - `NODE <delhi-primary> FAIL crash_after_prepare`
   - `ORDER PEN:2@MUMBAI PHONE:1@DELHI`
   - It COMMITS, with the vote `YES (already prepared) [@d2, retried after d1 failed]`. The prepare record was durable and replicated, so the new primary knew about it.
5. **Coordinator crash after the decision.**
   - `FAILPOINT crash_after_decision`
   - `ORDER PHONE:1@MUMBAI PEN:3@PUNE`
   - The router dies. After about 5 s, the participants log `T.. stays IN-DOUBT ... (2PC blocking)`.
   - Restart the router. It prints `recovery: re-driving decision COMMIT` and then `finally delivered`.
6. **Coordinator crash before the decision.**
   - `FAILPOINT crash_after_prepare`
   - `ORDER PEN:7@MUMBAI PEN:7@DELHI`
   - Restart the router. It logs `presumed ABORT`, and `STOCK PEN` shows RESERVED = 0.

### Test 6: Load balancing
1. `LB rr`, then `STATS reset`, then `scripts/client.sh bench 90 6 STOCK LAPTOP`, then `STATS`. Each replica serves about 33% of reads.
2. `LB primary` and repeat. 100% of reads go to the primaries; this is the baseline without balancing.
3. `NODE m1 DELAY 300` makes m1 a slow replica.
   - With `LB rr`, latency goes up because m1 still gets a third of the reads.
   - With `LB leastconn`, the router routes around m1 (about 2% of reads) and latency drops.
4. Kill a replica. Reads are redistributed across the remaining replicas automatically.
5. `BUY PEN 6000` allocates from the warehouse with the most stock first, so the load spreads across shards.

---

## 7. Running on several laptops (optional)

1. Edit `config/cluster.conf` and replace `127.0.0.1` with each machine's LAN IP. Use **the same file on every machine**.
2. Start each node on the machine whose IP it has. Nodes bind to all interfaces.
3. Allow the ports through the firewall.

## 8. Troubleshooting

| Problem | Fix |
|---|---|
| `Address already in use` | An old process is still running. Run `scripts/stop-cluster.sh` (or `.bat`), or change the ports in `cluster.conf`. |
| Strange characters like `[32m` on Windows | Use Windows Terminal, or run `set NO_COLOR=1` before starting. |
| `ERR router not reachable` | Start the router (terminal 10). |
| Web console: `Address already in use` on 8080 | Use another port: `scripts/web.sh 8090`. |
| You want a fresh start | Run `scripts/reset.sh` (or `.bat`). This deletes `data/`, `logs/` and `run/`. |
| `UnsupportedClassVersionError` | Your JDK is older than 17. Install JDK 17+ and rebuild. |
