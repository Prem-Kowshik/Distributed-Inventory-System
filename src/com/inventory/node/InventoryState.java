package com.inventory.node;

import java.util.*;
import java.util.zip.CRC32;
import java.nio.charset.StandardCharsets;

/**
 * The warehouse's database: the in-memory tables rebuilt from snapshot + WAL.
 *
 * Tables:
 *   products(sku PK, name, price, qty, reserved, received, shipped, transferNet)
 *   prepared_tx(txid PK, items)      -- 2PC participants' "prepared" state (survives crashes)
 *   tx_outcomes(txid PK, outcome)    -- recent decisions, makes COMMIT/ABORT idempotent
 *
 * The state is a deterministic state machine: applying the same log in the same order
 * on every replica yields identical state (this is what replication relies on).
 *
 * Concurrency control: a 2PC PREPARE does not take long-held row locks; it moves stock into
 * "reserved" (escrow locking). Other orders can still use the remaining "available" stock,
 * and there are no deadlocks.
 */
final class InventoryState {

    static final class Product {
        String sku, name;
        double price;
        int qty, reserved, received, shipped, transferNet;

        int available() { return qty - reserved; }

        String row() {
            return sku + " " + name + " " + qty + " " + reserved + " " + available() + " "
                    + String.format(Locale.ROOT, "%.2f", price) + " " + received + " " + shipped + " " + transferNet;
        }

        String ser() {
            return "P " + sku + " " + name + " " + price + " " + qty + " " + reserved + " "
                    + received + " " + shipped + " " + transferNet;
        }
    }

    /** One line of a distributed transaction at this warehouse: sku:delta:kind (kind O=order, T=transfer). */
    static final class Item {
        final String sku; final int delta; final char kind;
        Item(String sku, int delta, char kind) { this.sku = sku; this.delta = delta; this.kind = kind; }
        @Override public String toString() { return sku + ":" + delta + ":" + kind; }
    }

    static final class Prepared {
        final String txid; final List<Item> items; long preparedAtMs;
        Prepared(String txid, List<Item> items, long at) { this.txid = txid; this.items = items; this.preparedAtMs = at; }
    }

    final TreeMap<String, Product> products = new TreeMap<>();
    final LinkedHashMap<String, Prepared> prepared = new LinkedHashMap<>();
    final LinkedHashMap<String, String> outcomes = new LinkedHashMap<>() {
        @Override protected boolean removeEldestEntry(Map.Entry<String, String> e) { return size() > 5000; }
    };
    long lastLsn = 0, lastEpoch = 0;
    int committed = 0, aborted = 0;

    static List<Item> parseItems(String s) {
        List<Item> out = new ArrayList<>();
        for (String part : s.split(",")) {
            String[] f = part.split(":");
            if (f.length != 3) throw new IllegalArgumentException("bad item '" + part + "' (want SKU:DELTA:KIND)");
            out.add(new Item(f[0], Integer.parseInt(f[1]), f[2].charAt(0)));
        }
        return out;
    }

    /** Primary-side validation BEFORE an operation is logged. Returns null if OK, else the reason. */
    String validate(String payload) {
        String[] t = payload.split(" ");
        try {
            switch (t[0]) {
                case "ADD": {
                    if (t.length != 5) return "usage: ADD sku name qty price";
                    if (products.containsKey(t[1])) return "product " + t[1] + " already exists here (use RESTOCK)";
                    if (Integer.parseInt(t[3]) < 0) return "qty must be >= 0";
                    Double.parseDouble(t[4]);
                    return null;
                }
                case "RESTOCK": {
                    Product p = products.get(t[1]);
                    if (p == null) return "no such product " + t[1];
                    int d = Integer.parseInt(t[2]);
                    if (p.available() + d < 0) return "insufficient available stock for " + t[1];
                    return null;
                }
                case "ENSURE": Double.parseDouble(t[3]); return null;
                case "PREP": {
                    Map<String, Integer> need = new HashMap<>();
                    for (Item it : parseItems(t[2])) {
                        Product p = products.get(it.sku);
                        if (p == null) return "no such product " + it.sku;
                        if (it.delta < 0) need.merge(it.sku, -it.delta, Integer::sum);
                    }
                    for (Map.Entry<String, Integer> e : need.entrySet()) {
                        Product p = products.get(e.getKey());
                        if (p.available() < e.getValue())
                            return "insufficient stock for " + e.getKey() + ": need " + e.getValue() + ", available " + p.available();
                    }
                    return null;
                }
                case "COMMIT": case "ABORT":
                    return prepared.containsKey(t[1]) ? null : "transaction " + t[1] + " is not prepared here";
                case "NOOP": return null;
                default: return "unknown operation " + t[0];
            }
        } catch (RuntimeException e) {
            return "bad request: " + e.getMessage();
        }
    }

    /** Apply a logged record. Must be deterministic and must not fail. */
    void apply(LogRecord r) {
        String[] t = r.payload.split(" ");
        switch (t[0]) {
            case "ADD": {
                Product p = new Product();
                p.sku = t[1]; p.name = t[2]; p.qty = Integer.parseInt(t[3]); p.price = Double.parseDouble(t[4]);
                p.received = p.qty;
                products.put(p.sku, p);
                break;
            }
            case "RESTOCK": {
                Product p = products.get(t[1]);
                int d = Integer.parseInt(t[2]);
                p.qty += d; p.received += d;
                break;
            }
            case "ENSURE": {
                if (!products.containsKey(t[1])) {
                    Product p = new Product();
                    p.sku = t[1]; p.name = t[2]; p.price = Double.parseDouble(t[3]);
                    products.put(p.sku, p);
                }
                break;
            }
            case "PREP": {
                List<Item> items = parseItems(t[2]);
                for (Item it : items) if (it.delta < 0) products.get(it.sku).reserved += -it.delta;
                prepared.put(t[1], new Prepared(t[1], items, System.currentTimeMillis()));
                break;
            }
            case "COMMIT": {
                Prepared px = prepared.remove(t[1]);
                if (px != null) {
                    for (Item it : px.items) {
                        Product p = products.get(it.sku);
                        p.qty += it.delta;
                        if (it.delta < 0) p.reserved -= -it.delta;
                        if (it.kind == 'O') p.shipped += -it.delta;
                        if (it.kind == 'T') p.transferNet += it.delta;
                    }
                }
                outcomes.put(t[1], "COMMIT");
                committed++;
                break;
            }
            case "ABORT": {
                Prepared px = prepared.remove(t[1]);
                if (px != null)
                    for (Item it : px.items) if (it.delta < 0) products.get(it.sku).reserved -= -it.delta;
                outcomes.put(t[1], "ABORT");
                aborted++;
                break;
            }
            default: break; // NOOP
        }
        lastLsn = r.lsn;
        lastEpoch = r.epoch;
    }

    // ---------------- checkpoint / state transfer ----------------

    List<String> snapshot() {
        List<String> out = new ArrayList<>();
        out.add("META " + lastLsn + " " + lastEpoch + " " + committed + " " + aborted);
        for (Product p : products.values()) out.add(p.ser());
        for (Prepared px : prepared.values()) out.add("X " + px.txid + " " + joinItems(px.items));
        for (Map.Entry<String, String> e : outcomes.entrySet()) out.add("O " + e.getKey() + " " + e.getValue());
        return out;
    }

    void loadSnapshot(List<String> lines) {
        products.clear(); prepared.clear(); outcomes.clear();
        for (String l : lines) {
            if (l.isBlank()) continue;
            String[] t = l.strip().split(" ");
            switch (t[0]) {
                case "META":
                    lastLsn = Long.parseLong(t[1]); lastEpoch = Long.parseLong(t[2]);
                    committed = Integer.parseInt(t[3]); aborted = Integer.parseInt(t[4]);
                    break;
                case "P": {
                    Product p = new Product();
                    p.sku = t[1]; p.name = t[2]; p.price = Double.parseDouble(t[3]);
                    p.qty = Integer.parseInt(t[4]); p.reserved = Integer.parseInt(t[5]);
                    p.received = Integer.parseInt(t[6]); p.shipped = Integer.parseInt(t[7]);
                    p.transferNet = Integer.parseInt(t[8]);
                    products.put(p.sku, p);
                    break;
                }
                case "X": prepared.put(t[1], new Prepared(t[1], parseItems(t[2]), 0)); break;
                case "O": outcomes.put(t[1], t[2]); break;
                default: break;
            }
        }
    }

    /** Fingerprint of the table contents, used to prove replicas are identical. */
    String hash() {
        CRC32 c = new CRC32();
        for (Product p : products.values()) c.update((p.ser() + "\n").getBytes(StandardCharsets.UTF_8));
        for (Prepared px : prepared.values()) c.update((px.txid + joinItems(px.items) + "\n").getBytes(StandardCharsets.UTF_8));
        return String.format("%08x", c.getValue());
    }

    static String joinItems(List<Item> items) {
        StringBuilder sb = new StringBuilder();
        for (Item it : items) { if (sb.length() > 0) sb.append(','); sb.append(it); }
        return sb.toString();
    }
}
