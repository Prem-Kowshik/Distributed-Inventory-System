package com.inventory.node;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only, checksummed, fsync'd write-ahead log.
 * Every state change is appended here BEFORE it is applied in memory (WAL rule),
 * so the in-memory state can always be rebuilt by replaying the log (REDO).
 */
final class WriteAheadLog {
    private final Path path;
    private final boolean fsync;
    private FileOutputStream out;

    long truncatedBytes = 0;
    String truncationReason = null;

    WriteAheadLog(Path path, boolean fsync) {
        this.path = path;
        this.fsync = fsync;
    }

    /** Reads every valid record; a torn/corrupt tail (crash mid-write) is detected via CRC and truncated. */
    List<LogRecord> recover() throws IOException {
        List<LogRecord> recs = new ArrayList<>();
        if (Files.exists(path)) {
            byte[] data = Files.readAllBytes(path);
            int pos = 0, valid = 0, recNo = 0;
            while (pos < data.length) {
                int nl = indexOf(data, (byte) '\n', pos);
                if (nl < 0) {
                    truncationReason = "torn write: incomplete last record (" + (data.length - pos) + " bytes, no newline)";
                    break;
                }
                recNo++;
                String line = new String(data, pos, nl - pos, StandardCharsets.UTF_8);
                LogRecord r = LogRecord.decode(line);
                if (r == null) {
                    truncationReason = "checksum mismatch at WAL record #" + recNo;
                    break;
                }
                if (!recs.isEmpty() && r.lsn != recs.get(recs.size() - 1).lsn + 1) {
                    truncationReason = "LSN gap at WAL record #" + recNo;
                    break;
                }
                recs.add(r);
                pos = nl + 1;
                valid = pos;
            }
            if (valid < data.length) {
                truncatedBytes = data.length - valid;
                try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
                    ch.truncate(valid);
                    ch.force(true);
                }
            }
        }
        out = new FileOutputStream(path.toFile(), true);
        return recs;
    }

    synchronized void append(LogRecord r) throws IOException {
        out.write((r.encode() + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
        if (fsync) out.getFD().sync();   // durability: record is on disk before we acknowledge
    }

    /** Called after a checkpoint: everything up to the snapshot LSN is no longer needed. */
    synchronized void reset() throws IOException {
        out.close();
        out = new FileOutputStream(path.toFile(), false);
        if (fsync) out.getFD().sync();
    }

    private static int indexOf(byte[] a, byte b, int from) {
        for (int i = from; i < a.length; i++) if (a[i] == b) return i;
        return -1;
    }
}
