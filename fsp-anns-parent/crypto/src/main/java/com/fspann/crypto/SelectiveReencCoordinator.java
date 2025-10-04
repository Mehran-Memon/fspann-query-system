package com.fspann.crypto;

import com.fspann.common.*;
import io.micrometer.core.instrument.MeterRegistry;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SelectiveReencCoordinator {
    private final IndexService index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keys;
    private final ReencryptionTracker tracker;
    private final MeterRegistry meters;
    private final int batchSize;
    private final int minTouched;

    public SelectiveReencCoordinator(IndexService index, CryptoService crypto, KeyLifeCycleService keys,
                                     ReencryptionTracker tracker, MeterRegistry meters) {
        this.index = index; this.crypto = crypto; this.keys = keys; this.tracker = tracker; this.meters = meters;
        this.batchSize = Integer.getInteger("reenc.batchSize", 2000);
        this.minTouched = Integer.getInteger("reenc.minTouched", 10_000);
    }

    /** Called from ForwardSecureANNSystem.shutdown() when -Dreenc.mode=end. */
    public void runOnceIfNeeded() {
        Set<String> ids = tracker.drainAll();
        inc("reenc.touched.unique", ids.size());
        if (ids.size() < minTouched) return;

        int currentVer = keys.getCurrentVersion().getVersion();
        SecretKey currentKey = keys.getCurrentVersion().getKey();

        int reencOk = 0, missing = 0, sameVer = 0;
        List<String> batch = new ArrayList<>(batchSize);

        for (String id : ids) {
            EncryptedPoint p = index.getEncryptedPoint(id);
            if (p == null) { missing++; continue; }
            if (p.getVersion() == currentVer) { sameVer++; continue; }

            batch.add(id);
            if (batch.size() == batchSize) {
                reencOk += reencryptBatch(batch, currentKey, currentVer);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) reencOk += reencryptBatch(batch, currentKey, currentVer);

        inc("reenc.done.reencrypted", reencOk);
        inc("reenc.done.skipped.sameVersion", sameVer);
        inc("reenc.done.skipped.missing", missing);
        inc("reenc.run.count", 1);
    }

    private int reencryptBatch(List<String> ids, SecretKey currentKey, int currentVer) {
        int ok = 0;
        for (String id : ids) {
            EncryptedPoint p = index.getEncryptedPoint(id);
            if (p == null || p.getVersion() == currentVer) continue;
            try {
                // Delegate decrypt+re-encrypt to CryptoService (it knows how to fetch old keys)
                byte[] newIv = crypto.generateIV();
                EncryptedPoint updated = crypto.reEncrypt(p, currentKey, newIv);
                index.updateCachedPoint(updated);
                ok++;
            } catch (Exception ignore) {
                // Optionally: inc("reenc.failed", 1);
            }
        }
        return ok;
    }

    private void inc(String name, double v) {
        if (meters != null) meters.counter(name).increment(v);
    }
}
