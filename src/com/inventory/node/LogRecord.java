package com.inventory.node;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * One write-ahead-log entry. Encoded on disk as a single line:
 * <pre>lsn \t epoch \t prevEpoch \t payload \t crc32</pre>
 * lsn       – log sequence number (dense, starts at 1)
 * epoch     – primary term in which the record was created (used for divergence detection / fencing)
 * prevEpoch – epoch of record lsn-1 (lets a follower verify its log matches the primary's, Raft-style)
 * payload   – logical operation, e.g. "PREP T0003 LAPTOP:-2:O"
 */
final class LogRecord {
    final long lsn, epoch, prevEpoch;
    final String payload;

    LogRecord(long lsn, long epoch, long prevEpoch, String payload) {
        this.lsn = lsn; this.epoch = epoch; this.prevEpoch = prevEpoch; this.payload = payload;
    }

    String encode() {
        String body = lsn + "\t" + epoch + "\t" + prevEpoch + "\t" + payload;
        return body + "\t" + crc(body);
    }

    /** @return the record, or null if the line is corrupt (bad checksum / malformed). */
    static LogRecord decode(String line) {
        int i = line.lastIndexOf('\t');
        if (i < 0) return null;
        String body = line.substring(0, i);
        if (!crc(body).equals(line.substring(i + 1))) return null;
        String[] f = body.split("\t", 4);
        if (f.length != 4) return null;
        try {
            return new LogRecord(Long.parseLong(f[0]), Long.parseLong(f[1]), Long.parseLong(f[2]), f[3]);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static String crc(String s) {
        CRC32 c = new CRC32();
        c.update(s.getBytes(StandardCharsets.UTF_8));
        return Long.toHexString(c.getValue());
    }

    @Override public String toString() { return "#" + lsn + "(e" + epoch + ") " + payload; }
}
