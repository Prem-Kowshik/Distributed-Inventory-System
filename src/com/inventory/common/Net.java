package com.inventory.common;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Minimal text RPC over TCP. One request per connection:
 * client writes the request (one or more lines) and half-closes; server writes the
 * response and closes. Easy to debug and robust to crashes.
 */
public final class Net {
    private Net() {}

    /** Thrown when the TCP connection could not be established: the request was definitely NOT delivered. */
    public static final class ConnectFailed extends IOException {
        public ConnectFailed(String msg, Throwable cause) { super(msg, cause); }
    }

    public interface Handler { String handle(String request) throws Exception; }

    public static String request(String host, int port, String req, int connectMs, int readMs) throws IOException {
        Socket s = new Socket();
        try {
            try {
                s.connect(new InetSocketAddress(host, port), connectMs);
            } catch (IOException e) {
                throw new ConnectFailed(host + ":" + port + " " + e.getMessage(), e);
            }
            s.setSoTimeout(readMs);
            s.setTcpNoDelay(true);
            OutputStream os = s.getOutputStream();
            os.write((req.endsWith("\n") ? req : req + "\n").getBytes(StandardCharsets.UTF_8));
            os.flush();
            s.shutdownOutput();
            byte[] b = s.getInputStream().readAllBytes();
            String resp = new String(b, StandardCharsets.UTF_8).stripTrailing();
            // servers always answer with at least one line; EOF without data = the process died mid-request
            if (resp.isEmpty()) throw new IOException("connection closed without a reply (server crashed?)");
            return resp;
        } finally {
            try { s.close(); } catch (IOException ignore) { }
        }
    }

    public static String request(Config.NodeInfo n, String req, int connectMs, int readMs) throws IOException {
        return request(n.host, n.port, req, connectMs, readMs);
    }

    public static void serve(int port, Handler h, String threadName) throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(port));   // all interfaces: works across laptops on a LAN too
        ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, threadName + "-worker");
            t.setDaemon(true);
            return t;
        });
        Thread acceptor = new Thread(() -> {
            while (true) {
                try {
                    Socket s = ss.accept();
                    pool.submit(() -> handleConn(s, h));
                } catch (IOException e) {
                    Log.error("accept failed: %s", e.getMessage());
                }
            }
        }, threadName + "-accept");
        acceptor.start();
    }

    private static void handleConn(Socket s, Handler h) {
        try (s) {
            s.setSoTimeout(15000);
            String req = new String(s.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
            String resp;
            try {
                resp = h.handle(req);
            } catch (Exception e) {
                resp = "ERR " + e.getClass().getSimpleName() + ": " + e.getMessage();
            }
            OutputStream os = s.getOutputStream();
            os.write((resp + "\n").getBytes(StandardCharsets.UTF_8));
            os.flush();
        } catch (IOException ignore) {
            // client went away
        }
    }

    public static void writePid(String name) {
        try {
            Path dir = Path.of("run");
            Files.createDirectories(dir);
            Files.writeString(dir.resolve(name + ".pid"), String.valueOf(ProcessHandle.current().pid()));
        } catch (IOException e) {
            Log.warn("could not write pid file: %s", e.getMessage());
        }
    }

    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
