package com.inventory.web;

import com.inventory.common.Config;
import com.inventory.common.Net;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Web demo console. A browser cannot speak the cluster's raw TCP protocol, so this small
 * HTTP server (JDK built-in com.sun.net.httpserver, still no external libraries) sits
 * between the browser and the router:
 *
 *   browser --HTTP/JSON--> WebConsole --TCP text RPC--> Router --> storage nodes
 *
 * It also starts / kill -9s / restarts node and router processes (exactly like the
 * scripts/ folder does), tails their log files, and runs concurrent benchmarks.
 *
 *   java -cp out com.inventory.web.WebConsole [config/cluster.conf] [httpPort=8080]
 *
 * Must be run from the project root (same as every other process: data/, logs/, run/).
 */
public final class WebConsole {
    private final Config cfg;
    private final String conf;
    private final Path webDir = Path.of("web");
    private final List<String> ids = new ArrayList<>();
    private final Map<String, Process> children = new ConcurrentHashMap<>();
    private static final List<String> JAVA_OPTS =
            List.of("-Xms16m", "-Xmx96m", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1");

    WebConsole(String conf) throws IOException {
        this.conf = conf;
        this.cfg = Config.load(conf);
        ids.addAll(cfg.nodes.keySet());
        ids.add("router");
    }

    public static void main(String[] args) throws Exception {
        String conf = args.length > 0 ? args[0] : "config/cluster.conf";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        new WebConsole(conf).start(port);
    }

    private void start(int port) throws IOException {
        if (!Files.isDirectory(webDir)) throw new IOException("web/ folder not found - run from the project root");
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 128);
        http.setExecutor(Executors.newCachedThreadPool(r -> { Thread t = new Thread(r, "web"); t.setDaemon(true); return t; }));
        http.createContext("/api/", this::api);
        http.createContext("/", this::files);
        http.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> children.values().forEach(Process::destroyForcibly)));
        System.out.println("==============================================================");
        System.out.println("  Distributed Inventory web console:  http://localhost:" + port);
        System.out.println("  router expected at " + cfg.routerHost + ":" + cfg.routerPort + "   (Ctrl+C stops the console");
        System.out.println("  and every process the console started)");
        System.out.println("==============================================================");
    }

    // =========================================================================== routing
    private void api(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        Map<String, String> q = query(ex);
        String out;
        try {
            switch (path) {
                case "/api/cmd":     out = cmd(body(ex)); break;
                case "/api/state":   out = state(); break;
                case "/api/proc":    out = proc(q.get("action"), q.get("id")); break;
                case "/api/cluster": out = cluster(q.get("action")); break;
                case "/api/bench":   out = bench(Integer.parseInt(q.getOrDefault("n", "25")),
                                                 Integer.parseInt(q.getOrDefault("t", "25")), q.get("cmd")); break;
                case "/api/logs":    out = logs(q.get("id"), Long.parseLong(q.getOrDefault("from", "-1"))); break;
                default: send(ex, 404, "application/json", "{\"error\":\"no such endpoint\"}"); return;
            }
            send(ex, 200, "application/json", out);
        } catch (Exception e) {
            send(ex, 500, "application/json", "{\"error\":" + js(e.toString()) + "}");
        }
    }

    // =========================================================================== router RPC
    private String send(String cmd) {
        try {
            return Net.request(cfg.routerHost, cfg.routerPort, cmd, 800, 25000);
        } catch (Net.ConnectFailed e) {
            return "ERR router not reachable (is it running?)";
        } catch (IOException e) {
            return "ERR " + e.getMessage();
        }
    }

    private String cmd(String c) {
        c = c.strip();
        if (c.isEmpty()) return "{\"resp\":\"ERR empty command\",\"ms\":0}";
        long t0 = System.nanoTime();
        String r = send(c);
        return "{\"resp\":" + js(r) + ",\"ms\":" + (System.nanoTime() - t0) / 1_000_000 + "}";
    }

    /** One poll = router STATUS (null when the router is down) + which OS processes are alive. */
    private String state() {
        String status = null;
        try { status = Net.request(cfg.routerHost, cfg.routerPort, "STATUS", 300, 1500); } catch (IOException ignore) { }
        StringBuilder sb = new StringBuilder("{\"status\":").append(status == null ? "null" : js(status)).append(",\"procs\":{");
        boolean first = true;
        for (String id : ids) {
            if (!first) sb.append(',');
            first = false;
            sb.append(js(id)).append(':').append(alive(id));
        }
        return sb.append("}}").toString();
    }

    // =========================================================================== processes
    private Optional<ProcessHandle> handle(String id) {
        try {
            Path f = Path.of("run", id + ".pid");
            if (!Files.exists(f)) return Optional.empty();
            long pid = Long.parseLong(Files.readString(f).strip());
            return ProcessHandle.of(pid).filter(ProcessHandle::isAlive)
                    .filter(h -> h.info().command().map(c -> c.contains("java")).orElse(true));
        } catch (Exception e) { return Optional.empty(); }
    }

    private boolean alive(String id) { return handle(id).isPresent(); }

    private void check(String id) {
        if (id == null || !ids.contains(id)) throw new IllegalArgumentException("unknown process " + id);
    }

    private String startProc(String id) throws IOException {
        check(id);
        if (alive(id)) return "already running";
        Files.createDirectories(Path.of("logs"));
        Files.createDirectories(Path.of("run"));
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        cmd.addAll(JAVA_OPTS);
        cmd.add("-cp"); cmd.add("out");
        if (id.equals("router")) { cmd.add("com.inventory.router.Router"); cmd.add(conf); }
        else { cmd.add("com.inventory.node.StorageNode"); cmd.add(conf); cmd.add(id); }
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("NO_COLOR", "1");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(new File("logs", id + ".log")));
        Process p = pb.start();
        children.put(id, p);
        // the process writes run/<id>.pid itself; write it too so the UI sees it immediately
        Files.writeString(Path.of("run", id + ".pid"), String.valueOf(p.pid()));
        return "started " + id + " (pid " + p.pid() + ")";
    }

    /** kill -9: no shutdown hooks, no flushing - a power failure, as in scripts/kill-node.sh */
    private String killProc(String id) throws IOException {
        check(id);
        Optional<ProcessHandle> h = handle(id);
        Files.deleteIfExists(Path.of("run", id + ".pid"));
        children.remove(id);
        if (h.isEmpty()) return id + " was not running";
        h.get().destroyForcibly();
        try { h.get().onExit().get(3, TimeUnit.SECONDS); } catch (Exception ignore) { }
        return "killed " + id + " (pid " + h.get().pid() + ", SIGKILL)";
    }

    /** Same as scripts/corrupt-wal.sh: append half a record (no checksum, no newline) = torn write. */
    private String corrupt(String id) throws IOException {
        check(id);
        if (id.equals("router")) throw new IllegalArgumentException("router has no WAL");
        if (alive(id)) throw new IllegalStateException("stop " + id + " first (a torn write happens during a crash)");
        Path wal = Path.of("data", id, "wal.log");
        Files.createDirectories(wal.getParent());
        Files.writeString(wal, "999\t9\t9\tRESTOCK LAPTOP 50", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return "appended a torn record to " + wal;
    }

    private String proc(String action, String id) throws IOException {
        String r;
        switch (String.valueOf(action)) {
            case "start":   r = startProc(id); break;
            case "kill":    r = killProc(id); break;
            case "corrupt": r = corrupt(id); break;
            default: throw new IllegalArgumentException("action must be start|kill|corrupt");
        }
        return "{\"msg\":" + js(r) + "}";
    }

    private String cluster(String action) throws Exception {
        StringBuilder msg = new StringBuilder();
        switch (String.valueOf(action)) {
            case "start":
                for (String id : ids) msg.append(startProc(id)).append('\n');
                break;
            case "stop":
                for (String id : ids) msg.append(killProc(id)).append('\n');
                break;
            case "reset":
                for (String id : ids) killProc(id);
                for (String d : List.of("data", "logs", "run")) deleteTree(Path.of(d));
                msg.append("cluster stopped and data/, logs/, run/ wiped");
                break;
            case "seed":
                for (String raw : Files.readAllLines(Path.of("demo", "seed.txt"))) {
                    String l = raw.strip();
                    if (l.isEmpty() || l.startsWith("#") || l.startsWith("!")) continue;
                    msg.append("> ").append(l).append('\n').append(send(l)).append('\n');
                }
                break;
            default: throw new IllegalArgumentException("action must be start|stop|reset|seed");
        }
        return "{\"msg\":" + js(msg.toString().strip()) + "}";
    }

    private static void deleteTree(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (var s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(x -> { try { Files.delete(x); } catch (IOException ignore) { } });
        }
    }

    // =========================================================================== bench
    /** Same as `Client bench`: n requests from `threads` concurrent clients. */
    private String bench(int n, int threads, String cmd) throws InterruptedException {
        if (cmd == null || cmd.isBlank()) throw new IllegalArgumentException("cmd required");
        n = Math.min(Math.max(n, 1), 2000);
        threads = Math.min(Math.max(threads, 1), 64);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger ok = new AtomicInteger(), ab = new AtomicInteger(), er = new AtomicInteger();
        List<Long> lats = Collections.synchronizedList(new ArrayList<>());
        List<String> outcomes = Collections.synchronizedList(new ArrayList<>());
        Map<String, Integer> errors = new ConcurrentHashMap<>();
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            pool.submit(() -> {
                long s = System.currentTimeMillis();
                String r = send(cmd);
                lats.add(System.currentTimeMillis() - s);
                String o;
                if (r.startsWith("OK")) { ok.incrementAndGet(); o = "OK"; }
                else if (r.startsWith("ABORTED")) { ab.incrementAndGet(); o = "ABORTED"; }
                else { er.incrementAndGet(); o = "ERR"; errors.merge(r.split("\n")[0].replaceAll("\\d+", "#"), 1, Integer::sum); }
                outcomes.add(idx + ":" + o);
            });
        }
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.MINUTES);
        long wall = System.currentTimeMillis() - t0;
        List<Long> s = new ArrayList<>(lats);
        Collections.sort(s);
        long avg = s.stream().mapToLong(Long::longValue).sum() / Math.max(1, s.size());
        long p95 = s.isEmpty() ? 0 : s.get((int) Math.min(s.size() - 1, Math.max(0, Math.ceil(s.size() * 0.95) - 1)));
        long max = s.isEmpty() ? 0 : s.get(s.size() - 1);
        StringBuilder e = new StringBuilder("{");
        errors.forEach((k, v) -> { if (e.length() > 1) e.append(','); e.append(js(k)).append(':').append(v); });
        e.append('}');
        StringBuilder oc = new StringBuilder("[");
        for (String x : outcomes) { if (oc.length() > 1) oc.append(','); oc.append(js(x.split(":")[1])); }
        oc.append(']');
        return String.format("{\"cmd\":%s,\"n\":%d,\"threads\":%d,\"ok\":%d,\"aborted\":%d,\"err\":%d,"
                        + "\"avg\":%d,\"p95\":%d,\"max\":%d,\"wall\":%d,\"rps\":%.1f,\"errors\":%s,\"outcomes\":%s}",
                js(cmd), n, threads, ok.get(), ab.get(), er.get(), avg, p95, max, wall, 1000.0 * n / Math.max(1, wall), e, oc);
    }

    // =========================================================================== logs
    /** Incremental tail. from=-1 -> last ~64 KB. Returns new lines and the next offset. ANSI colours stripped. */
    private String logs(String id, long from) throws IOException {
        check(id);
        Path f = Path.of("logs", id + ".log");
        if (!Files.exists(f)) return "{\"next\":0,\"lines\":[]}";
        try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "r")) {
            long len = raf.length();
            if (from < 0) from = Math.max(0, len - 65536);
            if (from > len) from = 0;                                   // file was truncated / wiped
            long toRead = Math.min(len - from, 1 << 20);
            byte[] b = new byte[(int) toRead];
            raf.seek(from);
            raf.readFully(b);
            String txt = new String(b, StandardCharsets.UTF_8);
            int lastNl = txt.lastIndexOf('\n');
            if (lastNl < 0) return "{\"next\":" + from + ",\"lines\":[]}";
            String complete = txt.substring(0, lastNl);
            long next = from + complete.getBytes(StandardCharsets.UTF_8).length + 1;
            StringBuilder sb = new StringBuilder("{\"next\":").append(next).append(",\"lines\":[");
            boolean first = true;
            for (String l : complete.split("\n")) {
                l = l.replaceAll("\u001b\\[[0-9;]*m", "").stripTrailing();
                if (l.isEmpty() || l.startsWith("Picked up ")) continue;
                if (!first) sb.append(',');
                first = false;
                sb.append(js(l));
            }
            return sb.append("]}").toString();
        }
    }

    // =========================================================================== static files
    private void files(HttpExchange ex) throws IOException {
        String p = ex.getRequestURI().getPath();
        if (p.equals("/")) p = "/index.html";
        Path f = webDir.resolve(p.substring(1)).normalize();
        if (!f.startsWith(webDir) || !Files.isRegularFile(f)) { send(ex, 404, "text/plain", "not found"); return; }
        String n = f.getFileName().toString();
        String type = n.endsWith(".html") ? "text/html; charset=utf-8" : n.endsWith(".css") ? "text/css; charset=utf-8"
                : n.endsWith(".js") ? "application/javascript; charset=utf-8" : n.endsWith(".svg") ? "image/svg+xml"
                : "application/octet-stream";
        byte[] b = Files.readAllBytes(f);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("Cache-Control", "no-cache");
        ex.sendResponseHeaders(200, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    // =========================================================================== helpers
    private static void send(HttpExchange ex, int code, String type, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    private static String body(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) { return new String(in.readAllBytes(), StandardCharsets.UTF_8); }
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> m = new HashMap<>();
        String q = ex.getRequestURI().getRawQuery();
        if (q == null) return m;
        for (String kv : q.split("&")) {
            int i = kv.indexOf('=');
            if (i > 0) m.put(URLDecoder.decode(kv.substring(0, i), StandardCharsets.UTF_8),
                             URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
        }
        return m;
    }

    static String js(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        return sb.append('"').toString();
    }
}
