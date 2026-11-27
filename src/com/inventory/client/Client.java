package com.inventory.client;

import com.inventory.common.Config;
import com.inventory.common.Log;
import com.inventory.common.Net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Command-line client. Talks only to the router.
 *
 *   Client                         interactive shell
 *   Client -c "STOCK LAPTOP"       one command
 *   Client -f demo/seed.txt        run a command file (# lines are printed as headings)
 *   Client loadgen [sec] [ops/s]   continuous traffic; prints availability per second
 *
 *   --config path   (default config/cluster.conf)
 */
public final class Client {
    private static String host;
    private static int port;

    public static void main(String[] args) throws Exception {
        List<String> a = new ArrayList<>(Arrays.asList(args));
        String conf = "config/cluster.conf";
        int ci = a.indexOf("--config");
        if (ci >= 0) { conf = a.get(ci + 1); a.remove(ci + 1); a.remove(ci); }
        Config cfg = Config.load(conf);
        host = cfg.routerHost;
        port = cfg.routerPort;

        if (a.isEmpty()) { interactive(); return; }
        switch (a.get(0)) {
            case "-c": System.out.println(send(String.join(" ", a.subList(1, a.size())))); break;
            case "-f": runFile(a.get(1)); break;
            case "loadgen":
                loadgen(a.size() > 1 ? Integer.parseInt(a.get(1)) : 30,
                        a.size() > 2 ? Integer.parseInt(a.get(2)) : 10,
                        a.size() > 3 ? a.get(3) : "PEN");
                break;
            case "bench":
                bench(Integer.parseInt(a.get(1)), Integer.parseInt(a.get(2)), String.join(" ", a.subList(3, a.size())));
                break;
            default: System.out.println(send(String.join(" ", a)));
        }
    }

    static String send(String cmd) {
        try {
            return Net.request(host, port, cmd, 1000, 20000);
        } catch (Net.ConnectFailed e) {
            return "ERR router not reachable at " + host + ":" + port + " (is it running?)";
        } catch (IOException e) {
            return "ERR " + e.getMessage();
        }
    }

    private static String colorize(String resp) {
        if (resp.startsWith("OK")) return Log.c(Log.GREEN, resp.substring(0, Math.min(2, resp.length()))) + resp.substring(2);
        if (resp.startsWith("ABORTED")) return Log.c(Log.YELLOW, "ABORTED") + resp.substring(7);
        if (resp.startsWith("ERR")) return Log.c(Log.RED, "ERR") + resp.substring(3);
        return resp;
    }

    private static void interactive() throws IOException {
        System.out.println("Distributed Inventory client -> router " + host + ":" + port + "   (HELP for commands, EXIT to quit)");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print(Log.c(Log.CYAN, "inventory> "));
            System.out.flush();
            String line = in.readLine();
            if (line == null) break;
            line = line.strip();
            if (line.isEmpty()) continue;
            if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) break;
            System.out.println(colorize(send(line)));
        }
    }

    private static void runFile(String path) throws Exception {
        for (String raw : Files.readAllLines(Path.of(path))) {
            String line = raw.strip();
            if (line.isEmpty()) continue;
            if (line.startsWith("#")) { System.out.println(Log.c(Log.BOLD + Log.CYAN, line)); continue; }
            if (line.startsWith("!sleep")) { Net.sleep(Long.parseLong(line.split("\\s+")[1])); continue; }
            System.out.println(Log.c(Log.BOLD, "> " + line));
            System.out.println(colorize(send(line)));
        }
    }

    /** Sends the same command n times from `threads` concurrent clients; prints outcome counts and latency. */
    private static void bench(int n, int threads, String cmd) throws InterruptedException {
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.atomic.AtomicInteger ok = new java.util.concurrent.atomic.AtomicInteger(),
                ab = new java.util.concurrent.atomic.AtomicInteger(), er = new java.util.concurrent.atomic.AtomicInteger();
        List<Long> lats = Collections.synchronizedList(new ArrayList<>());
        Map<String, Integer> errors = new java.util.concurrent.ConcurrentHashMap<>();
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                long s = System.currentTimeMillis();
                String r = send(cmd);
                lats.add(System.currentTimeMillis() - s);
                if (r.startsWith("OK")) ok.incrementAndGet();
                else if (r.startsWith("ABORTED")) ab.incrementAndGet();
                else { er.incrementAndGet(); errors.merge(r.split("\n")[0].replaceAll("\\d+", "#"), 1, Integer::sum); }
            });
        }
        pool.shutdown();
        pool.awaitTermination(5, java.util.concurrent.TimeUnit.MINUTES);
        List<Long> s = new ArrayList<>(lats);
        Collections.sort(s);
        long wall = System.currentTimeMillis() - t0;
        System.out.printf("bench '%s' x%d with %d concurrent clients: %s=%d %s=%d %s=%d | latency avg=%dms p95=%dms max=%dms | %.0f req/s%n",
                cmd, n, threads, Log.c(Log.GREEN, "OK"), ok.get(), Log.c(Log.YELLOW, "ABORTED"), ab.get(), Log.c(Log.RED, "ERR"), er.get(),
                s.stream().mapToLong(Long::longValue).sum() / Math.max(1, s.size()),
                s.isEmpty() ? 0 : s.get((int) Math.min(s.size() - 1, Math.ceil(s.size() * 0.95) - 1)),
                s.isEmpty() ? 0 : s.get(s.size() - 1), 1000.0 * n / Math.max(1, wall));
        errors.forEach((k, v) -> System.out.println("   " + v + " x " + k));
    }

    /** 70% STOCK reads, 30% BUY of 1 unit (a 2PC write). Shows how the system behaves during failures. */
    private static void loadgen(int seconds, int rate, String sku) {
        System.out.printf("loadgen: %d s at ~%d ops/s, 70%% STOCK %s / 30%% BUY %s 1  (Ctrl+C to stop)%n", seconds, rate, sku, sku);
        Random rnd = new Random(42);
        long start = System.currentTimeMillis(), interval = Math.max(1, 1000 / rate);
        long ok = 0, err = 0, aborted = 0, maxLat = 0;
        long secOk = 0, secErr = 0, secMax = 0, secSum = 0;
        long nextReport = start + 1000;
        String lastErr = null;
        while (System.currentTimeMillis() - start < seconds * 1000L) {
            long t0 = System.currentTimeMillis();
            boolean write = rnd.nextInt(10) < 3;
            String r = send(write ? "BUY " + sku + " 1" : "STOCK " + sku);
            long lat = System.currentTimeMillis() - t0;
            if (r.startsWith("OK")) { ok++; secOk++; }
            else if (r.startsWith("ABORTED")) { aborted++; secOk++; }
            else { err++; secErr++; lastErr = r.split("\n")[0]; }
            maxLat = Math.max(maxLat, lat);
            secMax = Math.max(secMax, lat);
            secSum += lat;
            long now = System.currentTimeMillis();
            if (now >= nextReport) {
                long n = secOk + secErr;
                String line = String.format("[t=%3ds] ok=%-3d err=%-3d avg=%4dms max=%5dms %s",
                        (now - start) / 1000, secOk, secErr, n == 0 ? 0 : secSum / n, secMax,
                        secErr > 0 ? "<- " + lastErr : secMax > 1000 ? "<- slow: failover in progress, request waited for new primary" : "");
                System.out.println(secErr > 0 ? Log.c(Log.RED, line) : secMax > 1000 ? Log.c(Log.YELLOW, line) : line);
                secOk = secErr = secMax = secSum = 0;
                nextReport = now + 1000;
            }
            long sleep = interval - (System.currentTimeMillis() - t0);
            if (sleep > 0) Net.sleep(sleep);
        }
        long total = ok + err + aborted;
        System.out.println(Log.c(Log.BOLD, String.format(
                "loadgen done: %d requests, %d ok, %d aborted (business: out of stock), %d errors -> availability %.2f%%, worst latency %d ms",
                total, ok, aborted, err, total == 0 ? 0 : 100.0 * (ok + aborted) / total, maxLat)));
    }
}
