package com.inventory.router;

import com.inventory.common.Config;
import com.inventory.common.Log;
import com.inventory.common.Net;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Failure detector + failover manager.
 *  - pings every node every heartbeat.ms; a node missing failure.threshold heartbeats is DOWN
 *  - each warehouse has exactly one PRIMARY per epoch; if it fails, the live replica with the
 *    highest LSN is promoted with epoch+1 (epochs are persisted so they never go backwards)
 *  - all other live replicas are told to FOLLOW the primary (this also demotes stale primaries)
 */
final class ClusterManager {

    static final class NodeState {
        final Config.NodeInfo info;
        volatile boolean up = false, everSeen = false;
        volatile int misses = 0;
        volatile String role = "?", primary = "-";
        volatile long epoch = 0, lsn = 0, lastSeen = 0, downSince = 0;
        volatile int prepared = 0;
        final AtomicInteger inflight = new AtomicInteger();
        final AtomicLong served = new AtomicLong();
        NodeState(Config.NodeInfo info) { this.info = info; }
        String id() { return info.id; }
    }

    final Config cfg;
    final Map<String, NodeState> nodes = new LinkedHashMap<>();
    final Map<String, String> primary = new ConcurrentHashMap<>();
    final Map<String, Long> epochs = new ConcurrentHashMap<>();
    private final Set<String> unavailableLogged = ConcurrentHashMap.newKeySet();
    private final Path epochFile = Path.of("data", "router", "epochs");
    final int hbMs, threshold;
    private final ExecutorService pingPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "ping");
        t.setDaemon(true);
        return t;
    });

    ClusterManager(Config cfg) {
        this.cfg = cfg;
        this.hbMs = cfg.getInt("heartbeat.ms", 500);
        this.threshold = cfg.getInt("failure.threshold", 3);
        for (Config.NodeInfo n : cfg.nodes.values()) nodes.put(n.id, new NodeState(n));
    }

    void start() throws IOException {
        Files.createDirectories(epochFile.getParent());
        if (Files.exists(epochFile)) {
            for (String l : Files.readAllLines(epochFile)) {
                String[] t = l.strip().split(" ");
                if (t.length == 2) epochs.put(t[0], Long.parseLong(t[1]));
            }
            Log.info("loaded warehouse epochs %s", epochs);
        }
        Thread t = new Thread(() -> {
            while (true) {
                long t0 = System.currentTimeMillis();
                try { round(); } catch (Exception e) { Log.error("heartbeat round failed: %s", e); }
                Net.sleep(Math.max(10, hbMs - (System.currentTimeMillis() - t0)));
            }
        }, "failure-detector");
        t.setDaemon(true);
        t.start();
    }

    private void round() {
        Map<NodeState, Future<String>> fs = new LinkedHashMap<>();
        for (NodeState n : nodes.values())
            fs.put(n, pingPool.submit(() -> Net.request(n.info, "PING", 250, Math.max(200, hbMs * 3 / 5))));
        for (Map.Entry<NodeState, Future<String>> e : fs.entrySet()) {
            NodeState n = e.getKey();
            String r = null;
            try { r = e.getValue().get(hbMs, TimeUnit.MILLISECONDS); } catch (Exception ignore) { }
            if (r != null && r.startsWith("OK")) {
                String[] t = r.split(" ");
                n.role = t[1]; n.epoch = Long.parseLong(t[2]); n.lsn = Long.parseLong(t[3]);
                n.primary = t[4]; n.prepared = Integer.parseInt(t[5]);
                n.misses = 0;
                n.lastSeen = System.currentTimeMillis();
                if (!n.up) {
                    n.up = true;
                    if (n.everSeen) Log.ok("NODE %s is UP again (was down %.1fs) role=%s lsn=%d",
                            n.id(), (n.lastSeen - n.downSince) / 1000.0, n.role, n.lsn);
                    else Log.ok("NODE %s discovered (%s) role=%s epoch=%d lsn=%d", n.id(), n.info.warehouse, n.role, n.epoch, n.lsn);
                    n.everSeen = true;
                }
            } else {
                n.misses++;
                if (n.up && n.misses >= threshold) {
                    n.up = false;
                    n.downSince = System.currentTimeMillis();
                    Log.error("NODE %s (%s) is DOWN: missed %d heartbeats (~%d ms since last contact)",
                            n.id(), n.info.warehouse, n.misses, n.downSince - n.lastSeen);
                }
            }
        }
        reconcile();
    }

    /** Ensures every warehouse has one healthy primary and all live replicas follow it. */
    synchronized void reconcile() {
        for (String wh : cfg.warehouses()) {
            List<NodeState> alive = alive(wh);
            String p = primary.get(wh);
            long e = epochs.getOrDefault(wh, 0L);
            NodeState ps = p == null ? null : nodes.get(p);
            boolean healthy = ps != null && ps.up && "PRIMARY".equals(ps.role) && ps.epoch == e;

            if (!healthy) {
                long maxEpoch = e;
                for (NodeState n : alive) maxEpoch = Math.max(maxEpoch, n.epoch);
                NodeState claim = null;
                for (NodeState n : alive)
                    if ("PRIMARY".equals(n.role) && n.epoch == maxEpoch && (claim == null || n.lsn > claim.lsn)) claim = n;

                if (p == null && claim != null) {          // router (re)start: adopt the existing primary
                    primary.put(wh, claim.id());
                    epochs.put(wh, claim.epoch);
                    persistEpochs();
                    Log.event("%s: adopted existing primary %s (epoch %d)", wh, claim.id(), claim.epoch);
                } else if (!alive.isEmpty()) {
                    elect(wh, alive, maxEpoch + 1, p);
                } else if (replicas(wh).stream().anyMatch(n -> n.everSeen) && unavailableLogged.add(wh)) {
                    Log.error("WAREHOUSE %s UNAVAILABLE: no live replicas", wh);
                }
            }

            p = primary.get(wh);
            if (p == null || !nodes.get(p).up) continue;
            unavailableLogged.remove(wh);
            e = epochs.get(wh);
            for (NodeState n : alive) {
                if (n.id().equals(p)) continue;
                if ("BACKUP".equals(n.role) && n.epoch == e && p.equals(n.primary)) continue;
                try {
                    String r = Net.request(n.info, "FOLLOW " + p + " " + e, 250, 1000);
                    if (r.startsWith("OK")) { n.role = "BACKUP"; n.epoch = e; n.primary = p; }
                } catch (IOException ignore) { }
            }
        }
    }

    private void elect(String wh, List<NodeState> alive, long newEpoch, String oldPrimary) {
        List<NodeState> cands = new ArrayList<>(alive);
        // most up-to-date replica wins (highest LSN); tie -> lowest id
        cands.sort(Comparator.comparingLong((NodeState n) -> -n.lsn).thenComparing(NodeState::id));
        for (NodeState c : cands) {
            long t0 = System.currentTimeMillis();
            try {
                String r = Net.request(c.info, "PROMOTE " + newEpoch, 300, 5000);
                if (!r.startsWith("OK")) continue;
                primary.put(wh, c.id());
                epochs.put(wh, newEpoch);
                persistEpochs();
                c.role = "PRIMARY"; c.epoch = newEpoch; c.primary = c.id();
                NodeState old = oldPrimary == null ? null : nodes.get(oldPrimary);
                if (old != null && !old.up) {
                    Log.event("FAILOVER %s: primary %s -> %s (epoch %d, lsn %d). Outage ~%d ms (detection + promotion).",
                            wh, oldPrimary, c.id(), newEpoch, c.lsn,
                            System.currentTimeMillis() - old.lastSeen);
                } else {
                    Log.event("ELECTION %s: %s is PRIMARY (epoch %d, lsn %d, promote took %d ms)",
                            wh, c.id(), newEpoch, c.lsn, System.currentTimeMillis() - t0);
                }
                return;
            } catch (IOException e) {
                Log.warn("promote %s failed: %s", c.id(), e.getMessage());
            }
        }
    }

    private void persistEpochs() {
        try {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Long> e : epochs.entrySet()) sb.append(e.getKey()).append(' ').append(e.getValue()).append('\n');
            Path tmp = epochFile.resolveSibling("epochs.tmp");
            Files.writeString(tmp, sb.toString());
            Files.move(tmp, epochFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) { Log.warn("persist epochs failed: %s", e.getMessage()); }
    }

    List<NodeState> alive(String wh) {
        List<NodeState> out = new ArrayList<>();
        for (Config.NodeInfo n : cfg.byWarehouse.get(wh)) {
            NodeState s = nodes.get(n.id);
            if (s.up) out.add(s);
        }
        return out;
    }

    List<NodeState> replicas(String wh) {
        List<NodeState> out = new ArrayList<>();
        for (Config.NodeInfo n : cfg.byWarehouse.get(wh)) out.add(nodes.get(n.id));
        return out;
    }

    NodeState primaryState(String wh) {
        String p = primary.get(wh);
        if (p == null) return null;
        NodeState s = nodes.get(p);
        return s.up && "PRIMARY".equals(s.role) ? s : null;
    }

    /** Blocks (up to timeout) until the warehouse has a healthy primary: this is how writes ride through a failover. */
    NodeState awaitPrimary(String wh, long timeoutMs) {
        long end = System.currentTimeMillis() + timeoutMs;
        while (true) {
            NodeState s = primaryState(wh);
            if (s != null) return s;
            if (System.currentTimeMillis() >= end) return null;
            Net.sleep(50);
        }
    }

    /** Replicas that may serve reads: the primary, plus backups following it in the current epoch. */
    List<NodeState> readable(String wh) {
        String p = primary.get(wh);
        long e = epochs.getOrDefault(wh, -1L);
        List<NodeState> out = new ArrayList<>();
        for (NodeState n : alive(wh)) {
            if ("PRIMARY".equals(n.role) && n.id().equals(p)) out.add(n);
            else if ("BACKUP".equals(n.role) && n.epoch == e && n.primary.equals(p)) out.add(n);
        }
        if (out.isEmpty()) out.addAll(alive(wh));   // no primary: still serve (possibly stale) reads -> availability
        return out;
    }
}
