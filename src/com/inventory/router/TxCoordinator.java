package com.inventory.router;

import com.inventory.common.Log;
import com.inventory.common.Net;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Two-Phase Commit coordinator with a durable coordinator log (data/router/coordinator.log).
 *
 *   BEGIN T0007 MUMBAI=LAPTOP:-2:O;PUNE=LAPTOP:-1:O      (participants and their work)
 *   DECISION T0007 COMMIT                                 <- the commit point (fsync'd)
 *   END T0007                                             (all participants acknowledged)
 *
 * Presumed abort: a transaction with BEGIN but no DECISION is aborted on recovery.
 * A DECISION without END is re-sent ("re-driven") until every participant acknowledges.
 */
final class TxCoordinator {

    static final class Tx {
        final String id;
        final LinkedHashMap<String, String> parts;     // warehouse -> items
        volatile String decision;
        final Map<String, String> votes = new ConcurrentHashMap<>();
        final Set<String> pending = ConcurrentHashMap.newKeySet();
        long startMs = System.currentTimeMillis(), tookMs;
        String desc = "";
        Tx(String id, LinkedHashMap<String, String> parts) { this.id = id; this.parts = parts; }
    }

    private final Router router;
    private final ClusterManager cm;
    private final Path logPath = Path.of("data", "router", "coordinator.log");
    private FileOutputStream log;
    final Map<String, Tx> txs = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Set<String> active = ConcurrentHashMap.newKeySet();
    private final Set<Tx> redrive = ConcurrentHashMap.newKeySet();
    private int seq = 0;
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "2pc");
        t.setDaemon(true);
        return t;
    });

    TxCoordinator(Router router, ClusterManager cm) {
        this.router = router;
        this.cm = cm;
    }

    // ------------------------------------------------------------------ recovery
    void recover() throws IOException {
        Files.createDirectories(logPath.getParent());
        Set<String> ended = new HashSet<>();
        if (Files.exists(logPath)) {
            for (String l : Files.readAllLines(logPath)) {
                String[] t = l.strip().split(" ");
                if (t.length < 2) continue;
                switch (t[0]) {
                    case "BEGIN": {
                        LinkedHashMap<String, String> parts = new LinkedHashMap<>();
                        for (String p : t[2].split(";")) { String[] kv = p.split("=", 2); parts.put(kv[0], kv[1]); }
                        Tx rt = new Tx(t[1], parts);
                        if (t.length > 3) rt.desc = String.join(" ", Arrays.copyOfRange(t, 3, t.length));
                        txs.put(t[1], rt);
                        seq = Math.max(seq, Integer.parseInt(t[1].substring(1)));
                        break;
                    }
                    case "DECISION": if (txs.containsKey(t[1])) txs.get(t[1]).decision = t[2]; break;
                    case "END": ended.add(t[1]); break;
                    default: break;
                }
            }
        }
        log = new FileOutputStream(logPath.toFile(), true);
        int presumed = 0, redriven = 0;
        for (Tx tx : txs.values()) {
            if (ended.contains(tx.id)) continue;
            if (tx.decision == null) {
                tx.decision = "ABORT";
                logLine("DECISION " + tx.id + " ABORT");
                Log.warn("recovery: %s has BEGIN but no DECISION -> presumed ABORT", tx.id);
                presumed++;
            }
            tx.pending.addAll(tx.parts.keySet());
            redrive.add(tx);
            Log.event("recovery: re-driving decision %s %s to %s", tx.decision, tx.id, tx.parts.keySet());
            redriven++;
        }
        if (!txs.isEmpty())
            Log.info("coordinator log recovered: %d transaction(s), %d presumed-abort, %d to re-drive", txs.size(), presumed, redriven);

        Thread t = new Thread(() -> {
            while (true) {
                Net.sleep(1000);
                for (Tx tx : new ArrayList<>(redrive)) {
                    if (deliver(tx, 1500)) {
                        redrive.remove(tx);
                        logLine("END " + tx.id);
                        Log.ok("2PC %s: decision %s finally delivered to all participants", tx.id, tx.decision);
                    }
                }
            }
        }, "2pc-redrive");
        t.setDaemon(true);
        t.start();
    }

    private synchronized void logLine(String s) {
        try {
            log.write((s + "\n").getBytes(StandardCharsets.UTF_8));
            log.flush();
            log.getFD().sync();
        } catch (IOException e) {
            Log.error("coordinator log write failed: %s", e.getMessage());
        }
    }

    private synchronized String nextId() { return String.format("T%04d", ++seq); }

    // ------------------------------------------------------------------ protocol
    String execute(LinkedHashMap<String, String> parts, String desc) {
        Tx tx = new Tx(nextId(), parts);
        tx.desc = desc;
        txs.put(tx.id, tx);
        active.add(tx.id);
        StringBuilder enc = new StringBuilder();
        for (Map.Entry<String, String> e : parts.entrySet()) {
            if (enc.length() > 0) enc.append(';');
            enc.append(e.getKey()).append('=').append(e.getValue());
        }
        logLine("BEGIN " + tx.id + " " + enc + " " + desc);
        Log.event("2PC %s BEGIN (%s) participants=%s", tx.id, desc, parts.keySet());

        // ---------- Phase 1: PREPARE (in parallel) ----------
        Map<String, Future<String>> fs = new LinkedHashMap<>();
        for (String wh : parts.keySet()) fs.put(wh, pool.submit(() -> prepare(tx, wh)));
        boolean allYes = true;
        for (Map.Entry<String, Future<String>> e : fs.entrySet()) {
            String v;
            try { v = e.getValue().get(8, TimeUnit.SECONDS); } catch (Exception ex) { v = "NO timeout"; }
            tx.votes.put(e.getKey(), v);
            if (!v.startsWith("YES")) allYes = false;
            Log.info("   2PC %s phase-1 vote from %-8s: %s", tx.id, e.getKey(), v);
        }
        if (router.failpoint.equals("crash_after_prepare")) {
            Log.error("FAILPOINT crash_after_prepare: all votes collected, coordinator crashing BEFORE logging a decision");
            Runtime.getRuntime().halt(137);
        }

        // ---------- Decision (commit point) ----------
        tx.decision = allYes ? "COMMIT" : "ABORT";
        logLine("DECISION " + tx.id + " " + tx.decision);
        Log.event("2PC %s DECISION = %s (forced to coordinator log)", tx.id, tx.decision);
        if (router.failpoint.equals("crash_after_decision")) {
            Log.error("FAILPOINT crash_after_decision: decision is durable but NOT sent to participants - crashing");
            Runtime.getRuntime().halt(137);
        }

        // ---------- Phase 2: COMMIT / ABORT ----------
        tx.pending.addAll(parts.keySet());
        boolean done = deliver(tx, 5000);
        if (done) logLine("END " + tx.id);
        else redrive.add(tx);
        active.remove(tx.id);
        tx.tookMs = System.currentTimeMillis() - tx.startMs;

        StringBuilder out = new StringBuilder();
        out.append(tx.decision.equals("COMMIT") ? "OK " : "ABORTED ").append(tx.id).append(' ').append(tx.decision)
           .append(" (").append(desc).append(") in ").append(tx.tookMs).append(" ms");
        for (Map.Entry<String, String> e : parts.entrySet())
            out.append(String.format("%n   %-8s items=%-28s vote=%s", e.getKey(), e.getValue(), tx.votes.get(e.getKey())));
        out.append("\n   phase-2: ").append(done ? "decision acknowledged by all participants"
                : "pending at " + tx.pending + " (will be re-driven in background)");
        Log.event("2PC %s finished: %s in %d ms", tx.id, tx.decision, tx.tookMs);
        return out.toString();
    }

    /** Sends PREPARE to the warehouse's current primary; retries across a failover (PREPARE is idempotent). */
    private String prepare(Tx tx, String wh) {
        String req = "PREPARE " + tx.id + " " + tx.parts.get(wh);
        long end = System.currentTimeMillis() + 8000;
        String lastFailed = null;
        while (System.currentTimeMillis() < end) {
            ClusterManager.NodeState p = cm.awaitPrimary(wh, end - System.currentTimeMillis());
            if (p == null) break;
            try {
                String r = Net.request(p.info, req, 300, 3000);
                if (r.startsWith("ERR NOT_PRIMARY")) { Net.sleep(150); continue; }
                if (r.startsWith("OK VOTE ")) {
                    if (lastFailed != null) Log.ok("2PC %s PREPARE re-sent to new %s primary %s -> %s", tx.id, wh, p.id(), r.substring(8));
                    return r.substring(8) + "  [@" + p.id() + (lastFailed != null ? ", retried after " + lastFailed + " failed" : "") + "]";
                }
                return "NO " + r;
            } catch (IOException e) {
                if (!p.id().equals(lastFailed))
                    Log.warn("2PC %s PREPARE to %s/%s failed (%s); waiting for failover, PREPARE is idempotent -> will retry",
                            tx.id, wh, p.id(), e.getMessage());
                lastFailed = p.id();
                Net.sleep(200);
            }
        }
        return "NO participant " + wh + " unavailable";
    }

    /** Sends the decision to all pending participants until acked or deadline. */
    private boolean deliver(Tx tx, long deadlineMs) {
        List<Future<?>> fs = new ArrayList<>();
        for (String wh : new ArrayList<>(tx.pending)) {
            fs.add(pool.submit(() -> {
                long end = System.currentTimeMillis() + deadlineMs;
                while (System.currentTimeMillis() < end) {
                    ClusterManager.NodeState p = cm.awaitPrimary(wh, end - System.currentTimeMillis());
                    if (p == null) return;
                    try {
                        String r = Net.request(p.info, tx.decision + " " + tx.id, 300, 3000);
                        if (r.startsWith("OK")) { tx.pending.remove(wh); return; }
                    } catch (IOException ignore) { }
                    Net.sleep(200);
                }
            }));
        }
        for (Future<?> f : fs) { try { f.get(deadlineMs + 2000, TimeUnit.MILLISECONDS); } catch (Exception ignore) { } }
        return tx.pending.isEmpty();
    }

    /** Termination protocol: participants that are unsure ask the coordinator. */
    String status(String id) {
        Tx tx = txs.get(id);
        if (tx != null && tx.decision != null) return "OK " + tx.decision;
        if (active.contains(id)) return "OK PENDING";
        return "OK ABORT (presumed: no decision recorded)";
    }

    String history(int n) {
        List<Tx> all;
        synchronized (txs) { all = new ArrayList<>(txs.values()); }
        StringBuilder sb = new StringBuilder("OK last " + Math.min(n, all.size()) + " of " + all.size() + " transaction(s)");
        for (Tx tx : all.subList(Math.max(0, all.size() - n), all.size())) {
            String state = tx.decision == null ? "IN-PROGRESS" : tx.decision;
            if (!tx.pending.isEmpty() && tx.decision != null) state += " (delivering to " + tx.pending + ")";
            sb.append(String.format("%n   %s %-28s %s %s", tx.id, state, tx.parts, tx.desc));
        }
        return sb.toString();
    }
}
