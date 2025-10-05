package com.fspann.key;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.fspann.common.StorageSizer;

/**
 * KeyRotationServiceImpl
 * -----------------------------------------------------------------------------
 * Implements KeyLifeCycleService:
 *  - Current/previous/specific version access
 *  - Operation/time-based rotation (with ability to pin/activate a version)
 *  - Full re-encryption pass that respects forward security (re-inserts with current key)
 *
 * Notes:
 *  - When a version is "forced" (pinned), auto-rotation is disabled.
 *  - Re-encryption enumerates points via metadata manager; falls back gracefully if unsupported.
 */
public class KeyRotationServiceImpl implements KeyLifeCycleService, SelectiveReencryptor {
    private static final Logger logger = LoggerFactory.getLogger(KeyRotationServiceImpl.class);

    private final KeyManager keyManager;
    private final KeyRotationPolicy policy;
    private final String rotationMetaDir;
    private final RocksDBMetadataManager metadataManager;

    private volatile KeyVersion forcedVersion = null;

    private CryptoService cryptoService;
    private IndexService indexService;

    private volatile Instant lastRotation = Instant.now();
    private final AtomicInteger operationCount = new AtomicInteger(0);
    private volatile boolean frozen = false;

    public KeyRotationServiceImpl(KeyManager keyManager,
                                  KeyRotationPolicy policy,
                                  String rotationMetaDir,
                                  RocksDBMetadataManager metadataManager,
                                  CryptoService cryptoService) {
        this.keyManager = Objects.requireNonNull(keyManager, "keyManager");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.rotationMetaDir = Objects.requireNonNull(rotationMetaDir, "rotationMetaDir");
        this.metadataManager = Objects.requireNonNull(metadataManager, "metadataManager");
        this.cryptoService = cryptoService;
    }

    @Override
    public KeyVersion getCurrentVersion() {
        KeyVersion fv = forcedVersion;
        return (fv != null) ? fv : keyManager.getCurrentVersion();
    }

    @Override
    public void rotateIfNeeded() {
        if (forcedVersion != null || frozen || Boolean.getBoolean("skip.rotation")) return;
        boolean opsExceeded  = operationCount.get() >= policy.getMaxOperations();
        boolean timeExceeded = Duration.between(lastRotation, Instant.now()).toMillis() >= policy.getMaxIntervalMillis();
        if (!(opsExceeded || timeExceeded)) return;
        logger.info("Initiating key rotation (no re-encryption)...");
        rotateKeyOnly(); // << no reEncryptAll here
    }

    @Override
    public KeyVersion getPreviousVersion() { return keyManager.getPreviousVersion(); }

    @Override
    public KeyVersion getVersion(int version) {
        SecretKey key = keyManager.getSessionKey(version);
        if (key == null) throw new IllegalArgumentException("Unknown key version: " + version);
        return new KeyVersion(version, key);
    }

    @Override
    public void reEncryptAll() {
        logger.info("Starting re-encryption pass (forward-secure)");

        if (cryptoService == null || metadataManager == null || indexService == null) {
            logger.warn("Re-encryption skipped: services not initialized");
            return;
        }

        int total = 0;
        Set<String> seen = new HashSet<>();

        try {
            // Prefer enumerating via metadata manager
            List<EncryptedPoint> all = metadataManager.getAllEncryptedPoints();
            for (EncryptedPoint original : all) {
                if (original == null || !seen.add(original.getId())) continue;

                // Decrypt with the point's version key, then re-insert (encrypts with current key)
                KeyVersion pointVer = getVersion(original.getVersion());
                double[] raw = cryptoService.decryptFromPoint(original, pointVer.getKey());

                indexService.insert(original.getId(), raw); // index layer will persist & account ops
                total++;
            }

            EncryptedPointBuffer buf = indexService.getPointBuffer();
            if (buf != null) buf.flushAll();

            // Optional cleanup (stale metadata keys etc.)
            try { metadataManager.cleanupStaleMetadata(seen); } catch (UnsupportedOperationException ignore) {}

            logger.info("Re-encryption completed for {} vectors", total);
        } catch (UnsupportedOperationException | NoSuchMethodError e) {
            logger.warn("Metadata enumeration not supported; skipping re-encryption pass.");
        } catch (Exception e) {
            logger.error("Re-encryption pipeline failed", e);
        }
    }

    /** Internal helper used by scheduled/automatic rotation. Returns updated points if you later want to stream events. */
    public synchronized List<EncryptedPoint> rotateIfNeededAndReturnUpdated() {
        rotateIfNeeded();
        return Collections.emptyList(); // no proactive migration
    }

    /** Manual rotation endpoint. */
    public synchronized KeyVersion rotateKey() {
        logger.debug("Manual key rotation requested");
        try {
            KeyVersion newVersion = keyManager.rotateKey();
            lastRotation = Instant.now();
            operationCount.set(0);
            logger.info("Manual key rotation complete: v{}", newVersion.getVersion());
            reEncryptAll();
            return newVersion;
        } catch (Exception e) {
            logger.error("Manual key rotation failed", e);
            throw e;
        }
    }

    public void setCryptoService(CryptoService cryptoService) { this.cryptoService = cryptoService; }
    public void setIndexService(IndexService indexService) { this.indexService = indexService; }
    public void incrementOperation() { operationCount.incrementAndGet(); }
    public void setLastRotationTime(long timestamp) { this.lastRotation = Instant.ofEpochMilli(timestamp); }
    public void freezeRotation(boolean freeze) { this.frozen = freeze; }

    /** Pin the active key version; disables auto-rotation until cleared. */
    public synchronized boolean activateVersion(int version) {
        try {
            SecretKey key = keyManager.getSessionKey(version);
            if (key == null) {
                logger.warn("activateVersion({}) failed: version not found in keystore", version);
                return false;
            }
            this.forcedVersion = new KeyVersion(version, key);
            this.operationCount.set(0);
            this.lastRotation = Instant.now();
            logger.info("Activated key version v{}", version);
            return true;
        } catch (Exception e) {
            logger.error("activateVersion({}) failed", version, e);
            return false;
        }
    }

    /** Unpin any forced version and return to automatic rotation mode. */
    public synchronized void clearActivatedVersion() {
        this.forcedVersion = null;
        this.operationCount.set(0);
        this.lastRotation = Instant.now();
        logger.info("Cleared forced key version; auto-rotation re-enabled");
    }

    // -------------------------------------------------------------------------
    // NEW: Selective re-encryption for "touched" points only (post-query).
    // -------------------------------------------------------------------------
    /**
     * Re-encrypt only the touched IDs under the target version.
     * - Loads each point via metadata manager.
     * - If the stored version is older than targetVersion, decrypts with its source key and re-encrypts
     *   under the target key, then saves and updates the index cache. Tag/placement remain unchanged.
     * - Measures wall-clock ms and storage delta/after via the provided StorageSizer.
     *
     * @param touchedIds    unique IDs touched during the query (subset union). Duplicates are ignored.
     * @param targetVersion key version to re-encrypt to (typically current).
     * @param sizer         storage sizer for bytes delta; may be null to skip byte metrics.
     * @return              ReencryptReport with counts, time, bytes delta/after.
     */
    public ReencryptReport reencryptTouched(Collection<String> touchedIds, int targetVersion, StorageSizer sizer) {
        if (touchedIds == null || touchedIds.isEmpty()) {
            long after = (sizer != null ? sizer.bytesOnDisk() : 0L);
            return new ReencryptReport(0, 0, 0L, 0L, after);
        }
        if (cryptoService == null || metadataManager == null || indexService == null) {
            logger.warn("Selective re-encryption skipped: services not initialized");
            long after = (sizer != null ? sizer.bytesOnDisk() : 0L);
            return new ReencryptReport(touchedIds.size(), 0, 0L, 0L, after);
        }

        long t0 = System.nanoTime();
        long before = (sizer != null ? sizer.bytesOnDisk() : 0L);

        // Defensive: ensure uniqueness and deterministic processing order
        LinkedHashSet<String> ids = new LinkedHashSet<>(touchedIds);
        int reenc = 0;

        // Resolve target key once (and verify it exists)
        SecretKey targetKey;
        try {
            targetKey = getVersion(targetVersion).getKey();
        } catch (Exception e) {
            logger.error("Target key version {} not available; aborting selective re-encryption", targetVersion);
            long after = (sizer != null ? sizer.bytesOnDisk() : before);
            long dtMs = Math.round((System.nanoTime() - t0) / 1_000_000.0);
            return new ReencryptReport(ids.size(), 0, dtMs, Math.max(0L, after - before), after);
        }

        for (String id : ids) {
            if (id == null) continue;

            EncryptedPoint ep;
            try {
                ep = metadataManager.loadEncryptedPoint(id);
            } catch (IOException | ClassNotFoundException e) {
                // unreadable point → skip
                continue;
            }
            if (ep == null) continue;

            // Skip if already up-to-date
            if (ep.getVersion() >= targetVersion) continue;

            try {
                // Decrypt under its own stored version key
                SecretKey srcKey = getVersion(ep.getVersion()).getKey();
                double[] vec = cryptoService.decryptFromPoint(ep, srcKey);

                // Re-encrypt under target key. If encrypt() uses "current" implicitly,
                // we re-stamp to the targetVersion below to be explicit.
                EncryptedPoint ep2 = cryptoService.encrypt(id, vec);
                if (ep2.getVersion() != targetVersion) {
                    ep2 = new EncryptedPoint(
                            ep2.getId(),
                            ep2.getShardId(),
                            ep2.getIv(),
                            ep2.getCiphertext(),
                            targetVersion,
                            ep2.getVectorLength(),
                            ep2.getBuckets()
                    );
                }

                // Persist & update cache; do NOT change index placement or tags here.
                metadataManager.saveEncryptedPoint(ep2);
                indexService.updateCachedPoint(ep2);

                reenc++;
            } catch (Exception e) {
                // best-effort: skip bad points
                logger.debug("Selective re-encryption failed for id={}", id, e);
            }
        }

        // Ensure newly written points are durably persisted
        try {
            EncryptedPointBuffer buf = indexService.getPointBuffer();
            if (buf != null) buf.flushAll();
        } catch (Exception ignore) {}

        long after = (sizer != null ? sizer.bytesOnDisk() : before);
        long dtMs = Math.round((System.nanoTime() - t0) / 1_000_000.0);

        logger.info("Selective re-encryption: touched={}, re-encrypted={}, time={}ms, delta={}B, after={}B",
                ids.size(), reenc, dtMs, Math.max(0L, after - before), after);

        return new ReencryptReport(ids.size(), reenc, dtMs, Math.max(0L, after - before), after);
    }

    /** Rotate key with NO re-encryption (used by auto & one-shot bump). */
    public synchronized KeyVersion rotateKeyOnly() {
        KeyVersion newVersion = keyManager.rotateKey();
        lastRotation = Instant.now();
        operationCount.set(0);
        logger.info("Key rotation complete (no re-encryption): v{}", newVersion.getVersion());
        return newVersion;
    }

    /** Force a single bump now (no re-encryption). Returns true if rotated. */
    public synchronized boolean forceRotateNow() {
        rotateKeyOnly();
        return true;
    }

}
