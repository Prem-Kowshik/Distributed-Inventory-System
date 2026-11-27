package com.inventory.node;

import com.inventory.common.Config;
import com.inventory.common.Log;
import com.inventory.common.Net;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A storage node = one replica of one warehouse's database.
 *
 *   java -cp out com.inventory.node.StorageNode config/cluster.conf m1
 *
 * Role is PRIMARY (accepts writes, ships its WAL to the backups) or BACKUP (applies the
 * primary's log, serves reads). The router decides roles using epochs (terms).
 */
public final class StorageNode {
    final Config cfg;
    final Config.NodeInfo me;
    private final Path dir;

    final Object lock = new Object();
    final InventoryState state = new InventoryState();
    private WriteAheadLog wal;
    final List<LogRecord> memLog = new ArrayList<>();   // records since last checkpoint (for catch-up)
    private long baseLsn = 0, baseEpoch = 0;             // position of last checkpoint

    volatile String role = "BACKUP";
    volatile long epoch = 0;
    volatile String primaryId = "-";
    volatile String replMode;
    volatile String failpoint = "none";
    volatile int delayMs = 0;

    private final int checkpointEvery;
    private final boolean fsync;
    private int sinceCheckpoint = 0;
    private Replicator replicator;
    private final AtomicLong reads = new AtomicLong(), writes = new AtomicLong();
    private final Set<String> warnedInDoubt = new HashSet<>();

    StorageNode(Config cfg, String id) {
        this.cfg = cfg;
        this.me = cfg.node(id);
        this.dir = Path.of("data", id);
        this.replMode = cfg.get("replication.mode", "sync");
        this.checkpointEvery = cfg.getInt("checkpoint.every", 50);
        this.fsync = cfg.getBool("fsync", true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: StorageNode <cluster.conf> <nodeId>");
            System.exit(2);
        }
        new StorageNode(Config.load(args[0]), args[1]).start();
    }

    private void start() throws Exception {
        String color = switch (cfg.warehouses().indexOf(me.warehouse) % 3) {
            case 0 -> Log.BLUE; case 1 -> Log.GREEN; default -> Log.YELLOW;
        };
        Log.init(me.id + "@" + me.warehouse, color);
        Files.createDirectories(dir);
        Net.writePid(me.id);
        recover();
        replicator = new Replicator(this, cfg.peersOf(me));
        Net.serve(me.port, this::handle, me.id);
        Log.event("storage node %s for warehouse %s listening on port %d (role=%s, waiting for router)",
                me.id, me.warehouse, me.port, role);

        daemon("repl-heartbeat", () -> { while (true) { Net.sleep(1000); if (isPrimary()) replicator.heartbeat(); } });
        daemon("in-doubt-resolver", () -> { while (true) { Net.sleep(2000); resolveInDoubt(); } });
    }

    // =====================================================================================
    // RECOVERY: checkpoint (snapshot) + REDO of the write-ahead log
    // =====================================================================================
    private void recover() throws IOException {
        long t0 = System.nanoTime();
        Path snap = dir.resolve("snapshot.dat");
        String ckpt = "none";
        if (Files.exists(snap)) {
            state.loadSnapshot(Files.readAllLines(snap));
            baseLsn = state.lastLsn;
            baseEpoch = state.lastEpoch;
            ckpt = "loaded snapshot at lsn " + baseLsn;
        }
        wal = new WriteAheadLog(dir.resolve("wal.log"), fsync);
        List<LogRecord> recs = wal.recover();
        int replayed = 0;
        long first = -1;
        for (LogRecord r : recs) {
            if (r.lsn <= state.lastLsn) continue;                 // already inside the snapshot
            if (r.lsn != state.lastLsn + 1) { Log.warn("WAL gap at lsn %d, stopping replay", r.lsn); break; }
            state.apply(r);
            memLog.add(r);
            if (first < 0) first = r.lsn;
            replayed++;
        }
        sinceCheckpoint = replayed;
        epoch = Math.max(readEpochFile(), state.lastEpoch);
        for (InventoryState.Prepared p : state.prepared.values()) p.preparedAtMs = 0;   // treat as old

        boolean fresh = ckpt.equals("none") && recs.isEmpty() && wal.truncatedBytes == 0;
        if (fresh) {
            Log.info("fresh start: no checkpoint and no WAL found in %s", dir);
            return;
        }
        Log.event("================ CRASH RECOVERY (%s) ================", me.id);
        Log.info("  checkpoint      : %s", ckpt);
        Log.info("  WAL redo        : replayed %d record(s)%s", replayed,
                replayed > 0 ? " (lsn " + first + ".." + state.lastLsn + ")" : "");
        if (wal.truncatedBytes > 0)
            Log.warn("  WAL tail        : %s -> truncated %d corrupt byte(s)", wal.truncationReason, wal.truncatedBytes);
        else
            Log.info("  WAL tail        : clean (all checksums valid)");
        Log.info("  recovered state : %d product(s), lastLsn=%d, epoch=%d, committedTx=%d, abortedTx=%d",
                state.products.size(), state.lastLsn, epoch, state.committed, state.aborted);
        if (state.prepared.isEmpty())
            Log.info("  in-doubt 2PC tx : none");
        else
            Log.warn("  in-doubt 2PC tx : %s  (stock stays RESERVED until the coordinator's decision is known)",
                    state.prepared.keySet());
        Log.info("  recovery took %.1f ms. Rejoining as BACKUP; will catch up from current primary.",
                (System.nanoTime() - t0) / 1e6);
    }

    private long readEpochFile() {
        try {
            Path p = dir.resolve("epoch");
            return Files.exists(p) ? Long.parseLong(Files.readString(p).strip()) : 0;
        } catch (Exception e) { return 0; }
    }

    private void persistEpoch() {
        try {
            Path tmp = dir.resolve("epoch.tmp");
            Files.writeString(tmp, String.valueOf(epoch));
            Files.move(tmp, dir.resolve("epoch"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) { Log.warn("could not persist epoch: %s", e.getMessage()); }
    }

    // =====================================================================================
    // REQUEST DISPATCH
    // =====================================================================================
    private String handle(String req) throws Exception {
        String[] parts = req.split("\n", 2);
        String[] t = parts[0].strip().split("\\s+");
        String cmd = t[0].toUpperCase();
        switch (cmd) {
            case "PING": return ping();
            case "GET": return get(t[1]);
            case "LIST": return list();
            case "ADD": case "RESTOCK": case "ENSURE": return write(String.join(" ", t), true);
            case "PREPARE": return prepare(t[1], t[2]);
            case "COMMIT": case "ABORT": return decide(cmd, t[1]);
            case "REPL": return onRepl(t);
            case "REPLHB": return onReplHeartbeat(t);
            case "INSTALL": return onInstall(t, parts.length > 1 ? parts[1] : "");
            case "FOLLOW": return onFollow(t[1], Long.parseLong(t[2]));
            case "PROMOTE": return onPromote(Long.parseLong(t[1]));
            case "CHECKPOINT": return "OK " + checkpoint();
            case "HASH": synchronized (lock) { return "OK " + state.lastLsn + " " + state.hash(); }
            case "STATUS": return status();
            case "REPLMODE": replMode = t[1].toLowerCase(); Log.event("replication mode set to %s", replMode); return "OK " + replMode;
            case "DELAY": delayMs = Integer.parseInt(t[1]); Log.warn("artificial read delay set to %d ms", delayMs); return "OK delay " + delayMs;
            case "FAIL": failpoint = t[1].toLowerCase(); Log.warn("failpoint armed: %s", failpoint); return "OK failpoint " + failpoint;
            case "CRASH":
                Log.error("CRASH requested -> halting immediately (like kill -9, no cleanup)");
                daemon("crash", () -> { Net.sleep(100); Runtime.getRuntime().halt(137); });
                return "OK crashing " + me.id;
            default: return "ERR unknown command " + cmd;
        }
    }

    boolean isPrimary() { return "PRIMARY".equals(role); }

    long[] lastPos() { synchronized (lock) { return new long[]{state.lastLsn, state.lastEpoch}; } }

    private String ping() {
        synchronized (lock) {
            return "OK " + role + " " + epoch + " " + state.lastLsn + " " + primaryId + " " + state.prepared.size();
        }
    }

    private String get(String sku) {
        reads.incrementAndGet();
        if (delayMs > 0) Net.sleep(delayMs);
        synchronized (lock) {
            InventoryState.Product p = state.products.get(sku.toUpperCase());
            return p == null ? "ERR NOT_FOUND " + sku : "OK " + p.row();
        }
    }

    private String list() {
        reads.incrementAndGet();
        if (delayMs > 0) Net.sleep(delayMs);
        synchronized (lock) {
            StringBuilder sb = new StringBuilder("OK " + state.products.size() + " lsn=" + state.lastLsn);
            for (InventoryState.Product p : state.products.values()) sb.append('\n').append(p.row());
            return sb.toString();
        }
    }

    // =====================================================================================
    // WRITE PATH (primary): validate -> WAL append (fsync) -> apply -> replicate -> ack
    // =====================================================================================
    String write(String payload, boolean checkQuorum) throws IOException {
        List<Future<Boolean>> fs;
        LogRecord rec;
        synchronized (lock) {
            if (!isPrimary()) return "ERR NOT_PRIMARY primary=" + primaryId;
            String[] t = payload.split(" ");
            if (t[0].equals("PREP") && state.prepared.containsKey(t[1])) return "OK lsn=" + state.lastLsn + " (duplicate prepare)";
            String err = state.validate(payload);
            if (err != null) return "ERR " + err;
            if (checkQuorum && replMode.equals("quorum")) {
                int majority = (replicator.followerCount() + 1) / 2 + 1;
                int reachable = replicator.isr.size() + 1;
                if (reachable < majority)
                    return "ERR NO_QUORUM only " + reachable + " of " + (replicator.followerCount() + 1)
                            + " replicas reachable, need " + majority + " (write rejected to stay consistent)";
            }
            rec = new LogRecord(state.lastLsn + 1, epoch, state.lastEpoch, payload);
            wal.append(rec);          // 1. durable in local log (WAL rule)
            state.apply(rec);         // 2. apply to in-memory tables
            memLog.add(rec);
            sinceCheckpoint++;
            fs = replicator.replicate(rec);   // 3. ship to backups (ordered per follower)
        }
        writes.incrementAndGet();
        int acks = 0;
        if (!replMode.equals("async")) {
            for (Future<Boolean> f : fs) {
                try { if (f.get(3, TimeUnit.SECONDS)) acks++; } catch (Exception ignore) { }
            }
        }
        Log.info("write #%d %s -> WAL (fsync) + applied, replicas acked %s", rec.lsn, payload,
                replMode.equals("async") ? "async" : acks + "/" + fs.size());
        maybeCheckpoint();
        return "OK lsn=" + rec.lsn + " replicas_acked=" + (replMode.equals("async") ? "async" : acks + "/" + fs.size())
                + " mode=" + replMode;
    }

    // =====================================================================================
    // 2PC PARTICIPANT
    // =====================================================================================
    private String prepare(String tx, String items) throws IOException {
        synchronized (lock) {
            if (!isPrimary()) return "ERR NOT_PRIMARY primary=" + primaryId;
            if (state.prepared.containsKey(tx)) return "OK VOTE YES (already prepared)";
            String o = state.outcomes.get(tx);
            if (o != null) return "OK VOTE " + (o.equals("COMMIT") ? "YES" : "NO") + " (already " + o + ")";
        }
        if (failpoint.equals("vote_no")) {
            failpoint = "none";
            Log.error("FAILPOINT vote_no: voting NO for %s", tx);
            return "OK VOTE NO injected participant failure";
        }
        String r = write("PREP " + tx + " " + items, true);
        if (r.startsWith("ERR NOT_PRIMARY")) return r;
        if (!r.startsWith("OK")) {
            Log.warn("2PC %s PREPARE %s -> vote NO (%s)", tx, items, r.substring(4));
            return "OK VOTE NO " + r.substring(4);
        }
        Log.info("2PC %s PREPARED %s, stock reserved, logged+replicated -> vote YES", tx, items);
        if (failpoint.equals("crash_after_prepare")) {
            Log.error("FAILPOINT crash_after_prepare: PREPARE is logged & replicated, crashing before replying!");
            Runtime.getRuntime().halt(137);
        }
        return "OK VOTE YES " + r.substring(3);
    }

    private String decide(String decision, String tx) throws IOException {
        synchronized (lock) {
            if (!isPrimary()) return "ERR NOT_PRIMARY primary=" + primaryId;
            if (!state.prepared.containsKey(tx)) {
                String o = state.outcomes.get(tx);
                return "OK " + (o == null ? "nothing to do (not prepared here)" : "already " + o);
            }
        }
        String r = write(decision + " " + tx, true);
        if (r.startsWith("OK")) Log.info("2PC %s %s applied", tx, decision);
        return r;
    }

    /** Primary only: a prepared tx with no decision for a while -> ask the coordinator (termination protocol). */
    private void resolveInDoubt() {
        if (!isPrimary()) return;
        List<String> doubt = new ArrayList<>();
        long now = System.currentTimeMillis();
        synchronized (lock) {
            for (InventoryState.Prepared p : state.prepared.values())
                if (now - p.preparedAtMs > 5000) doubt.add(p.txid);
        }
        for (String tx : doubt) {
            try {
                String r = Net.request(cfg.routerHost, cfg.routerPort, "TXSTATUS " + tx, 300, 2000);
                String[] t = r.split(" ");
                if (t.length >= 2 && (t[1].equals("COMMIT") || t[1].equals("ABORT"))) {
                    String w = decide(t[1], tx);
                    if (w.startsWith("OK lsn"))
                        Log.event("in-doubt %s resolved by asking coordinator -> %s", tx, t[1]);
                    warnedInDoubt.remove(tx);
                }
            } catch (IOException e) {
                if (warnedInDoubt.add(tx))
                    Log.warn("coordinator unreachable: %s stays IN-DOUBT, its stock stays reserved (2PC blocking)", tx);
            }
        }
    }

    // =====================================================================================
    // FOLLOWER SIDE OF REPLICATION
    // =====================================================================================
    /** REPL sender curEpoch lsn recEpoch prevEpoch payload... */
    private String onRepl(String[] t) throws IOException {
        String sender = t[1];
        long cur = Long.parseLong(t[2]), lsn = Long.parseLong(t[3]), recEpoch = Long.parseLong(t[4]), prevEpoch = Long.parseLong(t[5]);
        String payload = String.join(" ", Arrays.copyOfRange(t, 6, t.length));
        String resp;
        synchronized (lock) {
            String fence = fence(sender, cur);
            if (fence != null) return fence;
            if (lsn == state.lastLsn + 1 && prevEpoch == state.lastEpoch) {
                LogRecord r = new LogRecord(lsn, recEpoch, prevEpoch, payload);
                wal.append(r);
                state.apply(r);
                memLog.add(r);
                sinceCheckpoint++;
                resp = "OK " + lsn;
                Log.info("replicated #%d from %s: %s", lsn, sender, payload);
            } else if (lsn <= state.lastLsn && hasRecord(lsn, recEpoch)) {
                resp = "OK dup";
            } else {
                resp = "NEED " + state.lastLsn + " " + state.lastEpoch;
            }
        }
        maybeCheckpoint();
        return resp;
    }

    private String onReplHeartbeat(String[] t) {
        synchronized (lock) {
            String fence = fence(t[1], Long.parseLong(t[2]));
            if (fence != null) return fence;
            long lsn = Long.parseLong(t[3]), ep = Long.parseLong(t[4]);
            if (lsn == state.lastLsn && ep == state.lastEpoch) return "OK in-sync";
            return "NEED " + state.lastLsn + " " + state.lastEpoch;
        }
    }

    private String onInstall(String[] t, String body) throws IOException {
        synchronized (lock) {
            String fence = fence(t[1], Long.parseLong(t[2]));
            if (fence != null) return fence;
            long before = state.lastLsn;
            state.loadSnapshot(Arrays.asList(body.split("\n")));
            writeSnapshotFile();
            wal.reset();
            memLog.clear();
            baseLsn = state.lastLsn;
            baseEpoch = state.lastEpoch;
            sinceCheckpoint = 0;
            Log.event("FULL STATE TRANSFER from %s installed: lsn %d -> %d (%d products)", t[1], before, state.lastLsn, state.products.size());
            return "OK installed " + state.lastLsn;
        }
    }

    /** Epoch fencing: reject anyone with an older epoch, follow anyone with a newer one. Caller holds lock. */
    private String fence(String sender, long cur) {
        if (cur < epoch) return "STALE " + epoch;
        if (cur == epoch && isPrimary()) return "STALE " + epoch;   // two primaries in one epoch is impossible
        if (cur > epoch || !sender.equals(primaryId)) {
            if (isPrimary()) Log.error("stepping down: %s is primary with newer epoch %d", sender, cur);
            role = "BACKUP";
            epoch = cur;
            primaryId = sender;
            persistEpoch();
            Log.event("role = BACKUP, following primary %s (epoch %d)", sender, cur);
        }
        return null;
    }

    private boolean hasRecord(long lsn, long ep) {
        if (lsn < baseLsn) return true;                 // covered by snapshot
        if (lsn == baseLsn) return baseEpoch == ep;
        return memLog.get((int) (lsn - baseLsn - 1)).epoch == ep;
    }

    /**
     * Primary side, lock held. Given a follower's last (lsn, epoch): index into memLog to start shipping from,
     * or -1 if a full snapshot is required (follower diverged, or its position is older than our checkpoint).
     */
    int catchUpPlan(long fLsn, long fEpoch) {
        if (fLsn == state.lastLsn && fEpoch == state.lastEpoch) return memLog.size();
        if (fLsn > state.lastLsn) return -1;
        if (fLsn < baseLsn) return -1;
        if (fLsn == baseLsn) return fEpoch == baseEpoch ? 0 : -1;
        LogRecord r = memLog.get((int) (fLsn - baseLsn - 1));
        return r.epoch == fEpoch ? (int) (fLsn - baseLsn) : -1;
    }

    private String onFollow(String newPrimary, long e) {
        synchronized (lock) {
            if (e < epoch) return "STALE " + epoch;
            boolean changed = !role.equals("BACKUP") || e != epoch || !newPrimary.equals(primaryId);
            if (isPrimary() && !newPrimary.equals(me.id)) Log.error("demoted by router: %s is the primary in epoch %d", newPrimary, e);
            role = "BACKUP";
            epoch = e;
            primaryId = newPrimary;
            persistEpoch();
            if (changed) Log.event("role = BACKUP, following primary %s (epoch %d)", newPrimary, e);
            return "OK following " + newPrimary;
        }
    }

    private String onPromote(long e) throws IOException {
        synchronized (lock) {
            if (e < epoch) return "STALE " + epoch;
            role = "PRIMARY";
            epoch = e;
            primaryId = me.id;
            persistEpoch();
            replicator.isr.clear();
            Log.event("PROMOTED to PRIMARY of %s (epoch %d, lsn %d)", me.warehouse, e, state.lastLsn);
            if (!state.prepared.isEmpty())
                Log.warn("inherited in-doubt 2PC transactions %s - will resolve with coordinator", state.prepared.keySet());
        }
        // An entry in the new epoch: replicating it makes all followers adopt the new epoch/primary
        // and detects + repairs any diverged follower log.
        String r = write("NOOP promoted-" + me.id + "-epoch-" + e, false);
        return "OK " + r;
    }

    void stepDown(long newerEpoch, String seenAt) {
        synchronized (lock) {
            if (newerEpoch > epoch || isPrimary()) {
                Log.error("FENCED: %s reports newer epoch %d > my epoch %d. A new primary exists -> stepping down to BACKUP.",
                        seenAt, newerEpoch, epoch);
                role = "BACKUP";
                epoch = Math.max(epoch, newerEpoch);
                primaryId = "-";
                persistEpoch();
            }
        }
    }

    // =====================================================================================
    // CHECKPOINTING
    // =====================================================================================
    private void maybeCheckpoint() throws IOException {
        boolean due;
        synchronized (lock) { due = sinceCheckpoint >= checkpointEvery; }
        if (due) checkpoint();
    }

    String checkpoint() throws IOException {
        synchronized (lock) {
            writeSnapshotFile();
            wal.reset();
            memLog.clear();
            baseLsn = state.lastLsn;
            baseEpoch = state.lastEpoch;
            sinceCheckpoint = 0;
            Log.event("CHECKPOINT at lsn %d: snapshot written atomically, WAL truncated", baseLsn);
            return "checkpoint at lsn " + baseLsn;
        }
    }

    private void writeSnapshotFile() throws IOException {
        Path tmp = dir.resolve("snapshot.tmp");
        try (FileOutputStream out = new FileOutputStream(tmp.toFile())) {
            out.write((String.join("\n", state.snapshot()) + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            if (fsync) out.getFD().sync();
        }
        Files.move(tmp, dir.resolve("snapshot.dat"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private String status() {
        synchronized (lock) {
            StringBuilder sb = new StringBuilder();
            sb.append("OK node ").append(me.id).append(" warehouse=").append(me.warehouse)
              .append(" role=").append(role).append(" epoch=").append(epoch).append(" primary=").append(primaryId).append('\n');
            sb.append("   lastLsn=").append(state.lastLsn).append(" checkpointLsn=").append(baseLsn)
              .append(" walRecords=").append(memLog.size()).append(" replMode=").append(replMode);
            if (isPrimary()) sb.append(" ISR=").append(new TreeSet<>(replicator.isr));
            sb.append('\n');
            sb.append("   products=").append(state.products.size()).append(" committedTx=").append(state.committed)
              .append(" abortedTx=").append(state.aborted).append(" reads=").append(reads.get())
              .append(" writes=").append(writes.get()).append(" hash=").append(state.hash()).append('\n');
            sb.append("   prepared(in-doubt)=");
            if (state.prepared.isEmpty()) sb.append("none");
            for (InventoryState.Prepared p : state.prepared.values())
                sb.append(p.txid).append('[').append(InventoryState.joinItems(p.items)).append("] ");
            if (!failpoint.equals("none")) sb.append("\n   failpoint=").append(failpoint);
            if (delayMs > 0) sb.append("\n   readDelay=").append(delayMs).append("ms");
            return sb.toString();
        }
    }

    private static void daemon(String name, Runnable r) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        t.start();
    }
}
