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
    private final int minTouched = Integer.getInteger("reenc.minTouched", 10_000);

    private final Path resultsDir;
    private final StorageSizer sizer;
    private final AtomicLong cumulativeUnique = new AtomicLong();

    public SelectiveReencCoordinator(IndexService index, CryptoService crypto, KeyLifeCycleService keys,
                                     ReencryptionTracker tracker, MeterRegistry meters,
                                     Path resultsDir, StorageSizer sizer) {
        this.index = index; this.crypto = crypto; this.keys = keys; this.tracker = tracker;
        this.meters = meters; this.resultsDir = resultsDir; this.sizer = sizer;
    }

    /** Called at shutdown when -Dreenc.mode=end. */
    public void runOnceIfNeeded() {
        Set<String> ids = tracker.drainAll();
        inc("reenc.touched.unique", ids.size());

        long after0 = bytesOnDisk();
        if (ids.isEmpty()) { writeSummary(0, 0, 0, 0L, after0, /*targetVer*/ keys.getCurrentVersion().getVersion()); return; }
        if (ids.size() < minTouched) { writeSummary(ids.size(), 0, 0, 0L, after0, keys.getCurrentVersion().getVersion()); return; }

        int targetVer = keys.getCurrentVersion().getVersion();

        int reenc = 0, sameVer = 0, missing = 0;
        long dtMs, delta, after;

        if (keys instanceof SelectiveReencryptor capable) {
            var rep = capable.reencryptTouched(ids, targetVer, sizer);
            reenc = rep.reencryptedCount();
            dtMs  = rep.timeMs();
            delta = rep.bytesDelta();
            after = rep.bytesAfter();
            sameVer = Math.max(0, ids.size() - reenc);
        } else {
            // Fallback path (no dependency on key module)
            long t0 = System.nanoTime();
            long before = bytesOnDisk();

            List<String> batch = new ArrayList<>(batchSize);
            for (String id : ids) {
                EncryptedPoint p = index.getEncryptedPoint(id);
                if (p == null) { missing++; continue; }
                if (p.getVersion() >= targetVer) { sameVer++; continue; }
                batch.add(id);
                if (batch.size() == batchSize) { reenc += reencryptBatch(batch, targetVer); batch.clear(); }
            }
            if (!batch.isEmpty()) reenc += reencryptBatch(batch, targetVer);

            after = bytesOnDisk();
            dtMs  = Math.round((System.nanoTime() - t0) / 1_000_000.0);
            delta = Math.max(0L, after - before);
        }

        inc("reenc.done.reencrypted", reenc);
        inc("reenc.done.skipped.sameVersion", sameVer);
        inc("reenc.done.skipped.missing", missing);
        inc("reenc.run.count", 1);

        writeSummary(ids.size(), reenc, sameVer, dtMs, bytesOnDisk(), targetVer);
    }

    private int reencryptBatch(List<String> ids, int targetVer) {
        int ok = 0;
        for (String id : ids) {
            EncryptedPoint p = index.getEncryptedPoint(id);
            if (p == null) continue;
            try {
                byte[] newIv = crypto.generateIV();
                EncryptedPoint updated = crypto.reEncrypt(p, keys.getCurrentVersion().getKey(), newIv);
                // Ensure version stamp
                if (updated.getVersion() < targetVer) {
                    updated = new EncryptedPoint(
                            updated.getId(), updated.getShardId(), updated.getIv(), updated.getCiphertext(),
                            targetVer, updated.getVectorLength(), updated.getBuckets());
                }
                index.updateCachedPoint(updated);
                ok++;
            } catch (Exception ignore) {}
        }
        return ok;
    }

    private long bytesOnDisk() { return (sizer != null ? sizer.bytesOnDisk() : 0L); }

    // ---------- CSV output (keeps test expectations: Mode at col[2], BytesAfter at col[11]) ----------
    private void writeSummary(int touched, int reenc, int alreadyCurrent, long timeMs, long bytesAfter, int targetVer) {
        try {
            Path csv = resultsDir.resolve("reencrypt_metrics.csv");
            if (!Files.exists(csv)) {
                Files.createDirectories(resultsDir);
                String header = String.join(",",
                        "RowType","QueryID","Mode","Touched","TouchedUniqueSoFar","TouchedCumulativeUnique",
                        "Reencrypted","AlreadyCurrent","Retried","TimeMs","BytesDelta","BytesAfter","TargetVersion") + "\n";
                Files.writeString(csv, header, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            long uniq = touched;                 // we drained the tracker
            long cum  = cumulativeUnique.addAndGet(touched);
            String row = String.join(",",
                    "SUMMARY","end","end",           // RowType, QueryID, Mode
                    Integer.toString(touched),
                    Long.toString(uniq),
                    Long.toString(cum),
                    Integer.toString(reenc),
                    Integer.toString(alreadyCurrent),
                    "0",                             // Retried (not tracked)
                    Long.toString(timeMs),
                    "0",                             // BytesDelta (optional: wire actual delta if available)
                    Long.toString(bytesAfter),
                    Integer.toString(targetVer)
            ) + "\n";
            Files.writeString(csv, row, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignore) {}
    }

    private void inc(String name, double v) {
        if (meters != null) meters.counter(name).increment(v);
    }
}
