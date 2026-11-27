package com.inventory.node;

import com.inventory.common.Config;
import com.inventory.common.Log;
import com.inventory.common.Net;

import java.util.*;
import java.util.concurrent.*;

/**
 * Primary-side log shipping (primary-backup replication).
 *
 * Each follower has its own single-threaded executor, so records are always shipped in LSN order.
 * The primary tracks the ISR (in-sync replica set). A follower that is behind or has a diverged
 * log answers "NEED lastLsn lastEpoch"; the primary then either ships the missing records
 * (incremental catch-up) or, if its log no longer has them / the follower diverged, ships a full
 * snapshot (state transfer).
 */
final class Replicator {
    private final StorageNode node;
    private final List<Config.NodeInfo> followers;
    private final Map<String, ExecutorService> exec = new HashMap<>();
    final Set<String> isr = ConcurrentHashMap.newKeySet();
    private final Set<String> hbPending = ConcurrentHashMap.newKeySet();

    Replicator(StorageNode node, List<Config.NodeInfo> followers) {
        this.node = node;
        this.followers = followers;
        for (Config.NodeInfo f : followers) {
            exec.put(f.id, Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "repl-" + f.id);
                t.setDaemon(true);
                return t;
            }));
        }
    }

    int followerCount() { return followers.size(); }

    /** Called with node.lock held, so submission order == LSN order. */
    List<Future<Boolean>> replicate(LogRecord r) {
        List<Future<Boolean>> fs = new ArrayList<>();
        for (Config.NodeInfo f : followers) fs.add(exec.get(f.id).submit(() -> ship(f, r)));
        return fs;
    }

    /** Periodic replication heartbeat: lets idle followers (e.g. a restarted node) detect they are behind. */
    void heartbeat() {
        for (Config.NodeInfo f : followers) {
            if (!hbPending.add(f.id)) continue;            // don't queue up heartbeats for a dead follower
            exec.get(f.id).submit(() -> {
                try {
                    long[] pos = node.lastPos();
                    String req = "REPLHB " + node.me.id + " " + node.epoch + " " + pos[0] + " " + pos[1];
                    return handle(f, send(f, req), true);
                } finally {
                    hbPending.remove(f.id);
                }
            });
        }
    }

    private boolean ship(Config.NodeInfo f, LogRecord r) {
        if (!node.isPrimary()) return false;
        return handle(f, send(f, replLine(r)), true);
    }

    private String replLine(LogRecord r) {
        return "REPL " + node.me.id + " " + node.epoch + " " + r.lsn + " " + r.epoch + " " + r.prevEpoch + " " + r.payload;
    }

    private String send(Config.NodeInfo f, String req) {
        try {
            return Net.request(f, req, 300, 3000);
        } catch (Exception e) {
            return null;
        }
    }

    private boolean handle(Config.NodeInfo f, String resp, boolean allowCatchUp) {
        if (resp == null) { markOut(f, "unreachable"); return false; }
        if (resp.startsWith("OK")) { markIn(f); return true; }
        if (resp.startsWith("STALE")) {
            node.stepDown(Long.parseLong(resp.split(" ")[1]), f.id);
            return false;
        }
        if (resp.startsWith("NEED") && allowCatchUp) {
            String[] t = resp.split(" ");
            return catchUp(f, Long.parseLong(t[1]), Long.parseLong(t[2]));
        }
        markOut(f, resp);
        return false;
    }

    private boolean catchUp(Config.NodeInfo f, long fLsn, long fEpoch) {
        List<LogRecord> recs = null;
        List<String> snap = null;
        synchronized (node.lock) {
            int from = node.catchUpPlan(fLsn, fEpoch);
            if (from < 0) snap = node.state.snapshot();
            else recs = new ArrayList<>(node.memLog.subList(from, node.memLog.size()));
        }
        if (snap != null) {
            Log.event("STATE TRANSFER -> %s: its log (lsn %d, epoch %d) is missing or diverged; shipping full snapshot (%d lines)",
                    f.id, fLsn, fEpoch, snap.size());
            String resp = send(f, "INSTALL " + node.me.id + " " + node.epoch + "\n" + String.join("\n", snap));
            return handle(f, resp, false);
        }
        for (LogRecord r : recs) {
            String resp = send(f, replLine(r));
            if (!handle(f, resp, false)) return false;
        }
        if (!recs.isEmpty())
            Log.info("catch-up: shipped %d missing record(s) to %s (lsn %d..%d)",
                    recs.size(), f.id, recs.get(0).lsn, recs.get(recs.size() - 1).lsn);
        markIn(f);
        return true;
    }

    private void markIn(Config.NodeInfo f) {
        if (isr.add(f.id)) Log.ok("replica %s is IN-SYNC  (ISR = %s)", f.id, new TreeSet<>(isr));
    }

    private void markOut(Config.NodeInfo f, String why) {
        if (isr.remove(f.id)) Log.warn("replica %s removed from ISR (%s)  (ISR = %s)", f.id, why, new TreeSet<>(isr));
    }
}
