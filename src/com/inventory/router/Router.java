package com.inventory.router;

import com.inventory.common.Config;
import com.inventory.common.Log;
import com.inventory.common.Net;

import java.io.IOException;
import java.util.*;

/**
 * The router / coordinator process. Clients talk only to the router.
 *   - routes writes to the warehouse primary, reads to replicas (load balancing)
 *   - coordinates multi-warehouse orders and stock transfers with 2PC
 *   - runs the failure detector and failover (ClusterManager)
 *
 *   java -cp out com.inventory.router.Router config/cluster.conf
 */
public final class Router {
    final Config cfg;
    final ClusterManager cm;
    final LoadBalancer lb;
    TxCoordinator tx;
    volatile String failpoint = "none";
    volatile String replMode;

    Router(Config cfg) {
        this.cfg = cfg;
        this.cm = new ClusterManager(cfg);
        this.lb = new LoadBalancer(cm, cfg.get("lb.policy", "rr"));
        this.replMode = cfg.get("replication.mode", "sync");
    }

    public static void main(String[] args) throws Exception {
        new Router(Config.load(args.length > 0 ? args[0] : "config/cluster.conf")).start();
    }

    private void start() throws Exception {
        Log.init("router", Log.MAGENTA);
        Net.writePid("router");
        tx = new TxCoordinator(this, cm);
        tx.recover();
        cm.start();
        Net.serve(cfg.routerPort, this::handle, "router");
        Log.event("router/coordinator listening on port %d | warehouses %s | lb=%s | heartbeat=%dms x%d",
                cfg.routerPort, cfg.warehouses(), lb.policy, cm.hbMs, cm.threshold);
    }

    // ===================================================================== dispatch
    private String handle(String req) throws Exception {
        String[] t = req.strip().split("\\s+");
        String cmd = t[0].toUpperCase();
        switch (cmd) {
            case "HELP": return help();
            case "STATUS": return status();
            case "ADD": need(t, 6, "ADD <WH> <SKU> <NAME> <QTY> <PRICE>");
                return single(wh(t[1]), "ADD " + t[2].toUpperCase() + " " + t[3] + " " + t[4] + " " + t[5]);
            case "RESTOCK": need(t, 4, "RESTOCK <WH> <SKU> <QTY>");
                return single(wh(t[1]), "RESTOCK " + t[2].toUpperCase() + " " + Integer.parseInt(t[3]));
            case "STOCK": need(t, 2, "STOCK <SKU>"); return stock(t[1].toUpperCase());
            case "INVENTORY": need(t, 2, "INVENTORY <WH>"); return inventory(wh(t[1]));
            case "BUY": need(t, 3, "BUY <SKU> <QTY> [PREFERRED_WH]");
                return buy(t[1].toUpperCase(), Integer.parseInt(t[2]), t.length > 3 ? wh(t[3]) : null);
            case "ORDER": need(t, 2, "ORDER <SKU>:<QTY>@<WH> [...]"); return order(Arrays.copyOfRange(t, 1, t.length));
            case "TRANSFER": need(t, 5, "TRANSFER <SKU> <QTY> <FROM_WH> <TO_WH>");
                return transfer(t[1].toUpperCase(), Integer.parseInt(t[2]), wh(t[3]), wh(t[4]));
            case "AUDIT": return audit();
            case "CHECK": return check();
            case "STATS": return stats(t.length > 1 && t[1].equalsIgnoreCase("reset"));
            case "LB": need(t, 2, "LB rr|leastconn|random|primary");
                if (!LoadBalancer.POLICIES.contains(t[1].toLowerCase())) return "ERR policies: " + LoadBalancer.POLICIES;
                lb.policy = t[1].toLowerCase();
                Log.event("load-balancing policy -> %s", lb.policy);
                return "OK load-balancing policy = " + lb.policy;
            case "MODE": need(t, 2, "MODE sync|quorum|async"); return mode(t[1].toLowerCase());
            case "TXLOG": return tx.history(t.length > 1 ? Integer.parseInt(t[1]) : 10);
            case "TXSTATUS": need(t, 2, "TXSTATUS <TXID>"); return tx.status(t[1]);
            case "NODE": need(t, 3, "NODE <nodeId> <command...>"); return forward(t[1], String.join(" ", Arrays.copyOfRange(t, 2, t.length)));
            case "CHECKPOINT": return checkpointAll();
            case "FAILPOINT": need(t, 2, "FAILPOINT crash_after_prepare|crash_after_decision|none");
                failpoint = t[1].toLowerCase();
                Log.warn("router failpoint armed: %s", failpoint);
                return "OK router failpoint = " + failpoint;
            default: return "ERR unknown command '" + t[0] + "'. Type HELP.";
        }
    }

    private static void need(String[] t, int n, String usage) {
        if (t.length < n) throw new IllegalArgumentException("usage: " + usage);
    }

    private String wh(String s) {
        String w = s.toUpperCase();
        if (!cfg.byWarehouse.containsKey(w)) throw new IllegalArgumentException("unknown warehouse " + s + " (have " + cfg.warehouses() + ")");
        return w;
    }

    // ===================================================================== single-warehouse writes
    /** Sends a write to the primary. Retries only when the request was certainly not delivered (connect failure). */
    private String[] callPrimary(String wh, String req, long deadlineMs) throws IOException {
        long end = System.currentTimeMillis() + deadlineMs;
        while (System.currentTimeMillis() < end) {
            ClusterManager.NodeState p = cm.awaitPrimary(wh, end - System.currentTimeMillis());
            if (p == null) break;
            p.inflight.incrementAndGet();
            try {
                String r = Net.request(p.info, req, 300, 5000);
                if (r.startsWith("ERR NOT_PRIMARY")) { Net.sleep(100); continue; }
                return new String[]{p.id(), r};
            } catch (Net.ConnectFailed e) {
                Net.sleep(100);                  // primary just died: wait for the failover and retry
            } finally {
                p.inflight.decrementAndGet();
            }
        }
        throw new IOException("warehouse " + wh + " has no primary (unavailable for writes)");
    }

    private String single(String wh, String req) throws IOException {
        long t0 = System.currentTimeMillis();
        try {
            String[] r = callPrimary(wh, req, 6000);
            return r[1] + "   [" + wh + " primary=" + r[0] + ", " + (System.currentTimeMillis() - t0) + " ms]";
        } catch (java.net.SocketTimeoutException e) {
            return "ERR outcome UNKNOWN (request sent, no reply): " + e.getMessage();
        } catch (IOException e) {
            return "ERR " + e.getMessage();
        }
    }

    // ===================================================================== load-balanced reads
    private String[] read(String wh, String req) {
        for (ClusterManager.NodeState n : lb.order(wh)) {
            n.inflight.incrementAndGet();
            try {
                String r = Net.request(n.info, req, 250, 3000);
                n.served.incrementAndGet();
                return new String[]{n.id(), n.role, r};
            } catch (IOException e) {
                // try next replica
            } finally {
                n.inflight.decrementAndGet();
            }
        }
        return null;
    }

    private String stock(String sku) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("OK stock of %s (reads load-balanced, policy=%s)%n", sku, lb.policy));
        sb.append(String.format("   %-9s %-6s %-8s %6s %9s %10s%n", "WAREHOUSE", "NODE", "ROLE", "QTY", "RESERVED", "AVAILABLE"));
        int total = 0;
        for (String wh : cfg.warehouses()) {
            String[] r = read(wh, "GET " + sku);
            if (r == null) { sb.append(String.format("   %-9s %-6s %s%n", wh, "-", "UNAVAILABLE (all replicas down)")); continue; }
            if (!r[2].startsWith("OK")) { sb.append(String.format("   %-9s %-6s %-8s %s%n", wh, r[0], r[1], "not stocked")); continue; }
            String[] f = r[2].split(" ");   // OK sku name qty reserved available price ...
            total += Integer.parseInt(f[5]);
            sb.append(String.format("   %-9s %-6s %-8s %6s %9s %10s%n", wh, r[0], r[1], f[3], f[4], f[5]));
        }
        sb.append("   TOTAL AVAILABLE = ").append(total);
        return sb.toString();
    }

    private String inventory(String wh) {
        String[] r = read(wh, "LIST");
        if (r == null) return "ERR warehouse " + wh + " unavailable";
        String[] lines = r[2].split("\n");
        StringBuilder sb = new StringBuilder("OK " + wh + " inventory served by " + r[0] + " (" + r[1] + ", " + lines[0].split(" ")[2] + ")\n");
        sb.append(String.format("   %-10s %-16s %6s %9s %10s %10s", "SKU", "NAME", "QTY", "RESERVED", "AVAILABLE", "PRICE"));
        for (int i = 1; i < lines.length; i++) {
            String[] f = lines[i].split(" ");
            sb.append(String.format("%n   %-10s %-16s %6s %9s %10s %10s", f[0], f[1], f[2], f[3], f[4], f[5]));
        }
        return sb.toString();
    }

    // ===================================================================== distributed transactions
    private String buy(String sku, int qty, String preferred) {
        if (qty <= 0) return "ERR qty must be > 0";
        Map<String, Integer> avail = new LinkedHashMap<>();
        List<String> skipped = new ArrayList<>();
        for (String wh : cfg.warehouses()) {
            try {
                String[] r = callPrimary(wh, "GET " + sku, 1500);
                if (r[1].startsWith("OK")) avail.put(wh, Integer.parseInt(r[1].split(" ")[5]));
            } catch (IOException e) { skipped.add(wh); }
        }
        List<String> order = new ArrayList<>(avail.keySet());
        order.sort((a, b) -> avail.get(b) - avail.get(a));            // most stock first
        if (preferred != null && order.remove(preferred)) order.add(0, preferred);
        int left = qty;
        LinkedHashMap<String, String> parts = new LinkedHashMap<>();
        for (String wh : order) {
            int take = Math.min(left, avail.get(wh));
            if (take <= 0) continue;
            parts.put(wh, sku + ":" + (-take) + ":O");
            left -= take;
            if (left == 0) break;
        }
        if (left > 0) {
            int sum = avail.values().stream().mapToInt(Integer::intValue).sum();
            return "ERR insufficient stock for " + sku + ": requested " + qty + ", available " + sum
                    + " across live warehouses" + (skipped.isEmpty() ? "" : " (unavailable: " + skipped + ")");
        }
        return tx.execute(parts, "BUY " + sku + " x" + qty);
    }

    private String order(String[] specs) {
        Map<String, List<String>> byWh = new LinkedHashMap<>();
        for (String s : specs) {
            String[] a = s.split("@");
            String[] b = a[0].split(":");
            if (a.length != 2 || b.length != 2) return "ERR bad item '" + s + "' (want SKU:QTY@WAREHOUSE)";
            int q = Integer.parseInt(b[1]);
            if (q <= 0) return "ERR qty must be > 0";
            byWh.computeIfAbsent(wh(a[1]), k -> new ArrayList<>()).add(b[0].toUpperCase() + ":" + (-q) + ":O");
        }
        LinkedHashMap<String, String> parts = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : byWh.entrySet()) parts.put(e.getKey(), String.join(",", e.getValue()));
        return tx.execute(parts, "ORDER " + String.join(" ", specs));
    }

    private String transfer(String sku, int qty, String from, String to) throws IOException {
        if (from.equals(to)) return "ERR source and destination are the same";
        if (qty <= 0) return "ERR qty must be > 0";
        String[] src = callPrimary(from, "GET " + sku, 3000);
        if (!src[1].startsWith("OK")) return "ERR " + from + " does not stock " + sku;
        String[] f = src[1].split(" ");
        String[] ens = callPrimary(to, "ENSURE " + sku + " " + f[2] + " " + f[6], 3000);   // create catalogue row at destination
        if (!ens[1].startsWith("OK")) return "ERR could not prepare destination: " + ens[1];
        LinkedHashMap<String, String> parts = new LinkedHashMap<>();
        parts.put(from, sku + ":" + (-qty) + ":T");
        parts.put(to, sku + ":" + qty + ":T");
        return tx.execute(parts, "TRANSFER " + sku + " x" + qty + " " + from + "->" + to);
    }

    // ===================================================================== verification
    /** Global invariant: for every SKU, sum(qty) == sum(received) - sum(shipped), and transfers net to zero. */
    private String audit() {
        Map<String, int[]> agg = new TreeMap<>();     // qty, received, shipped, transferNet, reserved
        StringBuilder sb = new StringBuilder();
        List<String> missing = new ArrayList<>();
        boolean localOk = true;
        for (String wh : cfg.warehouses()) {
            String[] r;
            try { r = callPrimary(wh, "LIST", 1500); } catch (IOException e) { missing.add(wh); continue; }
            String[] lines = r[1].split("\n");
            for (int i = 1; i < lines.length; i++) {
                String[] f = lines[i].split(" ");
                int qty = Integer.parseInt(f[2]), res = Integer.parseInt(f[3]), rec = Integer.parseInt(f[6]),
                        shp = Integer.parseInt(f[7]), tn = Integer.parseInt(f[8]);
                if (qty != rec - shp + tn) { localOk = false; sb.append(String.format("%n   VIOLATION at %s for %s", wh, f[0])); }
                int[] a = agg.computeIfAbsent(f[0], k -> new int[5]);
                a[0] += qty; a[1] += rec; a[2] += shp; a[3] += tn; a[4] += res;
            }
        }
        StringBuilder out = new StringBuilder("OK global stock audit (from primaries)");
        out.append(String.format("%n   %-10s %8s %8s %8s %9s %8s  %s", "SKU", "RECEIVED", "SHIPPED", "ON_HAND", "XFER_NET", "RESERVED", "INVARIANT on_hand = received - shipped"));
        boolean allOk = localOk;
        for (Map.Entry<String, int[]> e : agg.entrySet()) {
            int[] a = e.getValue();
            boolean ok = a[0] == a[1] - a[2] && a[3] == 0;
            allOk &= ok;
            out.append(String.format("%n   %-10s %8d %8d %8d %9d %8d  %s", e.getKey(), a[1], a[2], a[0], a[3], a[4],
                    ok ? "OK" : (a[3] != 0 ? "TRANSFER IN FLIGHT / VIOLATION" : "VIOLATION")));
        }
        out.append(sb);
        if (!missing.isEmpty()) out.append("\n   (warehouses unavailable, not audited: ").append(missing).append(")");
        out.append("\n   RESULT: ").append(allOk ? "CONSISTENT - no stock created or lost" : "INCONSISTENT");
        return out.toString();
    }

    /** Replica consistency: replicas at the same LSN must have byte-identical tables. */
    private String check() {
        StringBuilder sb = new StringBuilder("OK replica consistency check");
        boolean all = true;
        for (String wh : cfg.warehouses()) {
            Map<Long, Set<String>> hashesAtLsn = new TreeMap<>();
            StringBuilder line = new StringBuilder();
            long maxLsn = -1;
            for (ClusterManager.NodeState n : cm.replicas(wh)) {
                try {
                    String[] f = Net.request(n.info, "HASH", 250, 2000).split(" ");
                    long lsn = Long.parseLong(f[1]);
                    maxLsn = Math.max(maxLsn, lsn);
                    hashesAtLsn.computeIfAbsent(lsn, k -> new HashSet<>()).add(f[2]);
                    line.append(String.format("  %s%s:lsn=%d,hash=%s", n.id(), n.id().equals(cm.primary.get(wh)) ? "*" : "", lsn, f[2]));
                } catch (Exception e) {
                    line.append("  ").append(n.id()).append(":DOWN");
                }
            }
            boolean ok = hashesAtLsn.values().stream().allMatch(s -> s.size() == 1);
            boolean lag = hashesAtLsn.size() > 1;
            all &= ok;
            sb.append(String.format("%n   %-8s %s%s   -> %s", wh, line, "",
                    !ok ? "DIVERGED!" : lag ? "CONSISTENT (some replica lagging, will catch up)" : "IDENTICAL"));
        }
        sb.append("\n   (* = primary)  RESULT: ").append(all ? "all replicas consistent" : "DIVERGENCE DETECTED");
        return sb.toString();
    }

    // ===================================================================== admin
    private String status() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("OK cluster status | replication=%s | lb=%s | router failpoint=%s%n", replMode, lb.policy, failpoint));
        sb.append(String.format("   %-9s %-5s %-5s %-8s %5s %6s %-8s %8s %8s%n",
                "WAREHOUSE", "NODE", "STATE", "ROLE", "EPOCH", "LSN", "FOLLOWS", "IN-DOUBT", "SERVED"));
        for (String wh : cfg.warehouses()) {
            for (ClusterManager.NodeState n : cm.replicas(wh)) {
                boolean isP = n.id().equals(cm.primary.get(wh));
                sb.append(String.format("   %-9s %-5s %-5s %-8s %5s %6s %-8s %8s %8d%n", wh, n.id() + (isP ? "*" : ""),
                        n.up ? "UP" : "DOWN", n.up ? n.role : "-", n.up ? n.epoch : "-", n.up ? n.lsn : "-",
                        n.up && !"PRIMARY".equals(n.role) ? n.primary : "-", n.up ? n.prepared : "-", n.served.get()));
            }
        }
        sb.append("   (* = primary of warehouse)");
        return sb.toString();
    }

    private String stats(boolean reset) {
        if (reset) {
            for (ClusterManager.NodeState n : cm.nodes.values()) n.served.set(0);
            return "OK read counters reset";
        }
        StringBuilder sb = new StringBuilder("OK reads served per replica (policy=" + lb.policy + ")");
        for (String wh : cfg.warehouses()) {
            long tot = 0;
            for (ClusterManager.NodeState n : cm.replicas(wh)) tot += n.served.get();
            for (ClusterManager.NodeState n : cm.replicas(wh)) {
                long s = n.served.get();
                int pct = tot == 0 ? 0 : (int) Math.round(100.0 * s / tot);
                sb.append(String.format("%n   %-8s %-4s %-8s %5d reads %3d%% %s", wh, n.id(), n.up ? n.role : "DOWN", s, pct,
                        "#".repeat(pct / 4)));
            }
        }
        return sb.toString();
    }

    private String mode(String m) {
        if (!List.of("sync", "quorum", "async").contains(m)) return "ERR modes: sync | quorum | async";
        replMode = m;
        int n = 0;
        for (ClusterManager.NodeState s : cm.nodes.values()) {
            try { if (Net.request(s.info, "REPLMODE " + m, 250, 1000).startsWith("OK")) n++; } catch (IOException ignore) { }
        }
        Log.event("replication mode -> %s (applied on %d nodes)", m, n);
        return "OK replication mode = " + m + " (applied on " + n + " live nodes)";
    }

    private String forward(String node, String cmd) throws IOException {
        Config.NodeInfo n = cfg.node(node);
        try {
            return Net.request(n, cmd, 300, 5000);
        } catch (Net.ConnectFailed e) {
            return "ERR node " + node + " is not reachable";
        }
    }

    private String checkpointAll() {
        StringBuilder sb = new StringBuilder("OK");
        for (ClusterManager.NodeState n : cm.nodes.values()) {
            try { sb.append("\n   ").append(n.id()).append(": ").append(Net.request(n.info, "CHECKPOINT", 250, 3000)); }
            catch (IOException e) { sb.append("\n   ").append(n.id()).append(": DOWN"); }
        }
        return sb.toString();
    }

    private String help() {
        return String.join("\n",
            "OK commands (warehouses: " + cfg.warehouses() + ")",
            "  data:     ADD <WH> <SKU> <NAME> <QTY> <PRICE>     RESTOCK <WH> <SKU> <QTY>",
            "            STOCK <SKU>                             INVENTORY <WH>",
            "  2PC:      BUY <SKU> <QTY> [PREFERRED_WH]          (auto-splits across warehouses)",
            "            ORDER <SKU>:<QTY>@<WH> [...]            TRANSFER <SKU> <QTY> <FROM> <TO>",
            "            TXLOG [n]   TXSTATUS <TXID>",
            "  checks:   STATUS   AUDIT   CHECK   STATS [reset]",
            "  tuning:   LB rr|leastconn|random|primary          MODE sync|quorum|async",
            "            CHECKPOINT",
            "  faults:   NODE <id> CRASH                         NODE <id> STATUS",
            "            NODE <id> FAIL vote_no|crash_after_prepare|none",
            "            NODE <id> DELAY <ms>                    FAILPOINT crash_after_prepare|crash_after_decision|none");
    }
}
