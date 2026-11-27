package com.inventory.router;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Read load balancing across the replicas of a warehouse.
 * Writes always go to the primary; reads can go to any in-sync replica.
 *   rr        - round robin
 *   leastconn - fewest in-flight requests (adapts to slow nodes)
 *   random    - uniform random
 *   primary   - no balancing (all reads to primary) - baseline for comparison
 */
final class LoadBalancer {
    static final List<String> POLICIES = List.of("rr", "leastconn", "random", "primary");
    volatile String policy;
    private final ClusterManager cm;
    private final Map<String, AtomicInteger> rr = new ConcurrentHashMap<>();

    LoadBalancer(ClusterManager cm, String policy) {
        this.cm = cm;
        this.policy = policy;
    }

    /** Ordered list of replicas to try (first = chosen by policy, rest = fallbacks). */
    List<ClusterManager.NodeState> order(String wh) {
        List<ClusterManager.NodeState> c = new ArrayList<>(cm.readable(wh));
        if (c.isEmpty()) return c;
        c.sort(Comparator.comparing(ClusterManager.NodeState::id));
        switch (policy) {
            case "primary": {
                ClusterManager.NodeState p = cm.primaryState(wh);
                if (p != null) { c.remove(p); c.add(0, p); }
                return c;
            }
            case "leastconn": {
                c.sort(Comparator.comparingInt((ClusterManager.NodeState n) -> n.inflight.get())
                        .thenComparingLong(n -> n.served.get()));
                return c;
            }
            case "random":
                Collections.rotate(c, ThreadLocalRandom.current().nextInt(c.size()));
                return c;
            default: { // rr
                int i = Math.floorMod(rr.computeIfAbsent(wh, k -> new AtomicInteger()).getAndIncrement(), c.size());
                Collections.rotate(c, -i);
                return c;
            }
        }
    }
}
