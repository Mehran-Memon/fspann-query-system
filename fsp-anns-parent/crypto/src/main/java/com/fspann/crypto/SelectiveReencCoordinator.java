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
    private final int batchSize = Integer.getInteger("reenc.batchSize", 2000);
    private final int minTouched = Integer.getInteger("reenc.minTouched", 1 );

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
    public void runOnceWithVersion(int targetVer) {
        Set<String> ids = tracker.drainAll();
        inc("reenc.touched.unique", ids.size());

        long before = bytesOnDisk();
        if (ids.isEmpty()) {
            writeSummary(0, 0, 0, 0L, before, targetVer);
            return;
        }

        int reenc = 0;
        int sameVer = 0;

        if (keys instanceof SelectiveReencryptor capable) {
            ReencryptReport rep = capable.reencryptTouched(ids, targetVer, sizer);

            reenc   = rep.reencryptedCount();
            sameVer = Math.max(0, ids.size() - reenc);

            inc("reenc.done.reencrypted",        reenc);
            inc("reenc.done.skipped.sameVersion", sameVer);
            inc("reenc.run.count",               1);

            writeSummary(
                    ids.size(),
                    reenc,
                    sameVer,
                    rep.timeMs(),
                    rep.bytesAfter(),
                    targetVer
            );

        } else {
            // Should never occur in the ideal system
            writeSummary(ids.size(), 0, ids.size(), 0L, before, targetVer);
        }
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
