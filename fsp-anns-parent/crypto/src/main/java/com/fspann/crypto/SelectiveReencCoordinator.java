package com.fspann.crypto;

import com.fspann.common.*;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SelectiveReencCoordinator {
    private final IndexService index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keys;
    private final ReencryptionTracker tracker;
    private final MeterRegistry meters;
    private final Path resultsDir;
    private final StorageSizer sizer;
    private final AtomicLong cumulativeUnique = new AtomicLong();

    public SelectiveReencCoordinator(IndexService index, CryptoService crypto, KeyLifeCycleService keys,
                                     ReencryptionTracker tracker, MeterRegistry meters,
                                     Path resultsDir, StorageSizer sizer) {
        this.index = index; this.crypto = crypto; this.keys = keys; this.tracker = tracker;
        this.meters = meters; this.resultsDir = resultsDir; this.sizer = sizer;
    }

    /**
     * Execute selective re-encryption for a known target version.
     * Called by ForwardSecureANNSystem after forced rotation and before finalizeRotation().
     */
    private ReencryptReport runSelective(
            int targetVer,
            Set<String> ids
    ) {
        long beforeBytes = bytesOnDisk();

        if (ids == null || ids.isEmpty()) {
            writeSummary(0, 0, 0, 0L, beforeBytes, targetVer);
            return new ReencryptReport(0, 0, 0L, 0L, beforeBytes);
        }

        int reenc = 0;
        int alreadyCurrent = 0;
        long timeMs = 0L;
        long afterBytes = beforeBytes;

        if (keys instanceof SelectiveReencryptor capable) {
            long t0 = System.currentTimeMillis();
            ReencryptReport rep =
                    capable.reencryptTouched(ids, targetVer, sizer);
            long t1 = System.currentTimeMillis();

            reenc = rep.reencryptedCount();
            alreadyCurrent = Math.max(0, ids.size() - reenc);
            timeMs = (t1 - t0);
            afterBytes = rep.bytesAfter();

            inc("reenc.done.reencrypted", reenc);
            inc("reenc.done.skipped.sameVersion", alreadyCurrent);
            inc("reenc.run.count", 1);

        }

        writeSummary(
                ids.size(),
                reenc,
                alreadyCurrent,
                timeMs,
                afterBytes,
                targetVer
        );

        return new ReencryptReport(
                ids.size(),
                reenc,
                timeMs,
                afterBytes - beforeBytes,
                afterBytes
        );
    }

    public ReencryptReport runOnceWithVersion(
            int targetVersion,
            Set<String> idsToReencrypt
    ) {
        return runSelective(targetVersion, idsToReencrypt);
    }

    private long bytesOnDisk() { return (sizer != null ? sizer.bytesOnDisk() : 0L); }

    // ---------- CSV output (keeps test expectations: Mode at col[2], BytesAfter at col[11]) ----------
    private void writeSummary(int touched, int reenc, int alreadyCurrent,
                              long timeMs, long bytesAfter, int targetVer) {
        try {
            Path csv = resultsDir.resolve("reencrypt_metrics.csv");

            if (!Files.exists(csv)) {
                Files.createDirectories(resultsDir);
                String header = "RowType,QueryID,Mode,Touched,NewUnique,CumulativeUnique,"
                        +"Reencrypted,AlreadyCurrent,Retried,TimeMs,BytesDelta,BytesAfter,TargetVersion\n";
                Files.writeString(csv, header, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }

            long newUnique = touched;
            long cumulative = cumulativeUnique.addAndGet(touched);

            String row = String.format(
                    Locale.ROOT,
                    "SUMMARY,end,end,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
                    touched,
                    newUnique,
                    cumulative,
                    reenc,
                    alreadyCurrent,
                    0,                 // Retried placeholder
                    timeMs,
                    0L,                // BytesDelta (optional)
                    bytesAfter,
                    targetVer
            );

            Files.writeString(csv, row, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignore) {}
    }

    private void inc(String name, double v) {
        if (meters != null) meters.counter(name).increment(v);
    }
}
