package com.inventory.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Parses config/cluster.conf:
 * <pre>
 *   router &lt;host&gt; &lt;port&gt;
 *   node   &lt;WAREHOUSE&gt; &lt;nodeId&gt; &lt;host&gt; &lt;port&gt;
 *   key=value
 * </pre>
 */
public final class Config {
    public static final class NodeInfo {
        public final String id, warehouse, host;
        public final int port;
        NodeInfo(String id, String warehouse, String host, int port) {
            this.id = id; this.warehouse = warehouse; this.host = host; this.port = port;
        }
        @Override public String toString() { return id + "(" + warehouse + " " + host + ":" + port + ")"; }
    }

    public final Map<String, NodeInfo> nodes = new LinkedHashMap<>();
    public final Map<String, List<NodeInfo>> byWarehouse = new LinkedHashMap<>();
    public String routerHost = "127.0.0.1";
    public int routerPort = 7000;
    private final Properties props = new Properties();

    public static Config load(String path) throws IOException {
        Config c = new Config();
        int lineNo = 0;
        for (String raw : Files.readAllLines(Path.of(path))) {
            lineNo++;
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) continue;
            if (line.contains("=")) {
                int i = line.indexOf('=');
                c.props.setProperty(line.substring(0, i).strip(), line.substring(i + 1).strip());
                continue;
            }
            String[] t = line.split("\\s+");
            if (t[0].equalsIgnoreCase("router") && t.length == 3) {
                c.routerHost = t[1];
                c.routerPort = Integer.parseInt(t[2]);
            } else if (t[0].equalsIgnoreCase("node") && t.length == 5) {
                NodeInfo n = new NodeInfo(t[2], t[1].toUpperCase(), t[3], Integer.parseInt(t[4]));
                c.nodes.put(n.id, n);
                c.byWarehouse.computeIfAbsent(n.warehouse, k -> new ArrayList<>()).add(n);
            } else {
                throw new IOException(path + ":" + lineNo + ": cannot parse '" + raw + "'");
            }
        }
        return c;
    }

    public NodeInfo node(String id) {
        NodeInfo n = nodes.get(id);
        if (n == null) throw new IllegalArgumentException("unknown node id '" + id + "' (see cluster.conf)");
        return n;
    }

    public List<String> warehouses() { return new ArrayList<>(byWarehouse.keySet()); }

    public List<NodeInfo> peersOf(NodeInfo me) {
        List<NodeInfo> out = new ArrayList<>();
        for (NodeInfo n : byWarehouse.get(me.warehouse)) if (!n.id.equals(me.id)) out.add(n);
        return out;
    }

    public String get(String k, String def) { return props.getProperty(k, def); }
    public int getInt(String k, int def) { return Integer.parseInt(props.getProperty(k, String.valueOf(def))); }
    public boolean getBool(String k, boolean def) { return Boolean.parseBoolean(props.getProperty(k, String.valueOf(def))); }
}
