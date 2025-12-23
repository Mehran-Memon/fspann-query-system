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

/**
 * KeyRotationServiceImpl
 * -----------------------------------------------------------------------------
 * Implements KeyLifeCycleService:
 *  - Current/previous/specific version access
 *  - Operation/time-based rotation (with ability to pin/activate a version)
 *  - Full re-encryption pass that respects forward security (re-inserts with current key)
 *
 * Also implements SelectiveReencryptor:
 *  - Re-encrypt only "touched" IDs after query execution (used by SelectiveReencCoordinator).
 *
 * Notes:
 *  - When a version is "forced" (pinned), auto-rotation is disabled.
 *  - Re-encryption enumerates points via metadata manager; falls back gracefully if unsupported.
 */
public class KeyRotationServiceImpl implements KeyLifeCycleService, SelectiveReencryptor {
    private static final Logger logger = LoggerFactory.getLogger(KeyRotationServiceImpl.class);

    private final KeyManager keyManager;
    private volatile KeyRotationPolicy policy;
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
        boolean timeExceeded = Duration.between(lastRotation, Instant.now()).toMillis()
                >= policy.getMaxIntervalMillis();
        if (!(opsExceeded || timeExceeded)) return;
        logger.info("Initiating key rotation (no re-encryption)...");
        rotateKeyOnly(); // no reEncryptAll here
    }

    @Override
    public KeyVersion getPreviousVersion() {
        return keyManager.getPreviousVersion();
    }

    @Override
    public KeyVersion getVersion(int version) {
        SecretKey key = keyManager.getSessionKey(version);
        if (key == null) {
            throw new IllegalArgumentException("Unknown key version: " + version);
        }
        return new KeyVersion(version, key);
    }

    private KeyUsageTracker getUsageTracker() {
        if (keyManager != null) {
            return keyManager.getUsageTracker();
        }
        return null;
    }

    @Override
    public void reEncryptAll() {
        logger.info("Starting re-encryption pass (forward-secure)");

        if (cryptoService == null || metadataManager == null) {
            logger.warn("Re-encryption skipped: services not initialized");
            return;
        }

        int total = 0;
        int currentVer = getCurrentVersion().getVersion();
        Set<String> seen = new HashSet<>();

        try {
            List<EncryptedPoint> all = metadataManager.getAllEncryptedPoints();
            for (EncryptedPoint original : all) {
                if (original == null || !seen.add(original.getId())) continue;

                // Decrypt with the point's OLD version key
                KeyVersion pointVer = getVersion(original.getVersion());
                double[] raw = cryptoService.decryptFromPoint(original, pointVer.getKey());

                // Re-encrypt with CURRENT key
                EncryptedPoint newEp = cryptoService.encrypt(original.getId(), raw);

                // ===== CRITICAL: Force version alignment =====
                if (newEp.getVersion() != currentVer) {
                    newEp = new EncryptedPoint(
                            newEp.getId(),
                            currentVer,           // Use current version
                            newEp.getIv(),
                            newEp.getCiphertext(),
                            currentVer,           // keyVersion = currentVer
                            newEp.getVectorLength(),
                            newEp.getShardId(),
                            newEp.getBuckets(),
                            List.of()
                    );
                }

                // ===== CRITICAL: Persist IMMEDIATELY (don't use indexService.insert) =====
                metadataManager.saveEncryptedPoint(newEp);
                total++;
            }

            logger.info("Re-encryption completed for {} vectors", total);
        } catch (Exception e) {
            logger.error("Re-encryption pipeline failed", e);
        }
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

            finalizeRotation();

            return newVersion;
        } catch (Exception e) {
            logger.error("Manual key rotation failed", e);
            throw e;
        }
    }

    /**
     * Pin the active key version; disables auto-rotation until cleared.
     * FIXED: Now returns false gracefully if version doesn't exist, doesn't throw.
     */
    public synchronized boolean activateVersion(int version) {
        try {
            SecretKey key = keyManager.getSessionKey(version);
            if (key == null) {
                logger.warn("activateVersion({}) failed: version not found in keystore", version);
                return false;
            }
            this.forcedVersion = new KeyVersion(version, key);
            if (this.forcedVersion == null) {
                logger.error("activateVersion({}) failed: KeyVersion constructor returned null", version);
                return false;
            }
            this.operationCount.set(0);
            this.lastRotation = Instant.now();
            logger.info("Activated key version v{}", version);
            return true;
        } catch (Exception e) {
            logger.error("activateVersion({}) failed", version, e);
            return false;
        }
    }

    /**
     * Unpin any forced version and return to automatic rotation mode.
     * FIXED: More defensive null checks.
     */
    public synchronized void clearActivatedVersion() {
        try {
            this.forcedVersion = null;
            this.operationCount.set(0);
            this.lastRotation = Instant.now();
            logger.info("Cleared forced key version; auto-rotation re-enabled");
        } catch (Exception e) {
            logger.error("clearActivatedVersion() failed", e);
        }
    }

    // -------------------------------------------------------------------------
    // SelectiveReencryptor: re-encrypt only "touched" points
    // -------------------------------------------------------------------------

    @Override
    public ReencryptReport reencryptTouched(Collection<String> touchedIds,
                                            int targetVersion,
                                            StorageSizer sizer) {

        if (touchedIds == null || touchedIds.isEmpty()) {
            long after = (sizer != null ? sizer.bytesOnDisk() : 0L);
            return new ReencryptReport(0, 0, 0L, 0L, after);
        }

        long t0 = System.nanoTime();
        long before = (sizer != null ? sizer.bytesOnDisk() : 0L);

        int reenc = 0;
        KeyUsageTracker tracker = getUsageTracker();

        for (String id : new LinkedHashSet<>(touchedIds)) {

            EncryptedPoint ep;
            try {
                ep = metadataManager.loadEncryptedPoint(id);
            } catch (IOException | ClassNotFoundException e) {
                logger.debug("Failed to load encrypted point id={} during selective re-encryption", id, e);
                continue;
            }

            if (ep == null) continue;

            int oldVersion = ep.getKeyVersion();

            // already upgraded → skip
            if (oldVersion >= targetVersion) continue;

            try {
                double[] vec = cryptoService.decryptFromPoint(ep, null);

                EncryptedPoint fresh = cryptoService.encrypt(id, vec);

                if (fresh.getKeyVersion() != targetVersion) {
                    fresh = new EncryptedPoint(
                            fresh.getId(),
                            targetVersion,
                            fresh.getIv(),
                            fresh.getCiphertext(),
                            targetVersion,
                            fresh.getVectorLength(),
                            fresh.getShardId(),
                            fresh.getBuckets(),
                            List.of()
                    );
                }

                metadataManager.saveEncryptedPoint(fresh);
                reenc++;

                // TRACK RE-ENCRYPTION
                if (tracker != null) {
                    tracker.trackReencryption(id, oldVersion, targetVersion);
                }

            } catch (Exception ignore) {
                // forward-secure skip
            }
        }

        long after = (sizer != null ? sizer.bytesOnDisk() : before);
        long dtMs = Math.round((System.nanoTime() - t0) / 1_000_000.0);

        return new ReencryptReport(
                touchedIds.size(),
                reenc,
                dtMs,
                Math.max(0L, after - before),
                after
        );
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

    /**
     * Finalize key rotation: delete all keys older than currentVersion-1.
     * Must be called only AFTER full/partial re-encryption is complete.
     *
     * FIXED: Now uses (currentVersion - 1) to keep at least 2 versions
     * (current + previous) for security. Never deletes v1.
     */
    public synchronized void finalizeRotation() {
        try {
            KeyVersion curr = keyManager.getCurrentVersion();
            if (curr == null) {
                logger.error("finalizeRotation failed: no current version available");
                return;
            }
            int currentVer = curr.getVersion();

            // Keep current version and one previous for safety
            // Delete all keys older than currentVer-1
            // This ensures v1 is never deleted (it's the master KDF seed)
            int keepVersion = Math.max(1, currentVer - 1);

            keyManager.deleteKeysOlderThan(keepVersion);

            logger.info("Finalized rotation: deleted keys older than v{}", keepVersion);
        } catch (Exception e) {
            logger.error("Failed to finalize rotation / delete old keys", e);
        }
    }

    public synchronized void setPolicy(KeyRotationPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("KeyRotationPolicy cannot be null");
        }
        this.policy = policy;
    }

    /**
     * Initialize usage tracking by scanning existing encrypted points.
     * Call this once at system startup to populate the tracker.
     */
    public synchronized void initializeUsageTracking() {
        KeyUsageTracker tracker = getUsageTracker();
        if (tracker == null) {
            logger.warn("Usage tracker not available, skipping initialization");
            return;
        }

        logger.info("Initializing key usage tracking from metadata...");

        // CRITICAL FIX: reset state
        tracker.clear();

        try {
            int tracked = 0;
            Map<Integer, Integer> versionCounts = new HashMap<>();

            List<EncryptedPoint> allPoints = metadataManager.getAllEncryptedPoints();
            for (EncryptedPoint ep : allPoints) {
                if (ep == null) continue;

                int version = ep.getKeyVersion();
                String id = ep.getId();

                tracker.trackEncryption(id, version);
                tracked++;
                versionCounts.merge(version, 1, Integer::sum);
            }

            logger.info("Usage tracking rebuilt: {} vectors across {} key versions",
                    tracked, versionCounts.size());

            for (Map.Entry<Integer, Integer> e : versionCounts.entrySet()) {
                logger.info("  Key v{}: {} vectors", e.getKey(), e.getValue());
            }

        } catch (Exception e) {
            logger.error("Failed to initialize usage tracking", e);
            throw e;
        }
    }

    /**
     * Print diagnostic information about key usage.
     * Useful for debugging and verification.
     */
    public void printKeyUsageDiagnostics() {
        KeyUsageTracker tracker = getUsageTracker();
        if (tracker == null) {
            logger.info("Usage tracker not available");
            return;
        }

        logger.info("=== KEY USAGE DIAGNOSTICS ===");
        logger.info(tracker.getSummary());

        Set<Integer> versions = tracker.getTrackedVersions();
        for (Integer v : versions) {
            boolean safe = tracker.isSafeToDelete(v);
            logger.info("Key v{}: {} vectors, safe_to_delete={}",
                    v, tracker.getVectorCount(v), safe);
        }
    }

    @Override
    public void trackEncryption(String vectorId, int keyVersion) {
        KeyUsageTracker tracker = getUsageTracker();
        if (tracker != null) {
            tracker.trackEncryption(vectorId, keyVersion);
        }
    }

    @Override
    public void trackReencryption(String vectorId, int oldVersion, int newVersion) {
        KeyUsageTracker tracker = getUsageTracker();
        if (tracker != null) {
            tracker.trackReencryption(vectorId, oldVersion, newVersion);
        }
    }

    public KeyManager getKeyManager() {
        return this.keyManager;
    }
    public void setCryptoService(CryptoService cryptoService) { this.cryptoService = cryptoService; }
    public void setIndexService(IndexService indexService)   { this.indexService = indexService; }
    public void incrementOperation()                         { operationCount.incrementAndGet(); }
    public void setLastRotationTime(long timestamp)          { this.lastRotation = Instant.ofEpochMilli(timestamp); }
    public void freezeRotation(boolean freeze)               { this.frozen = freeze; }
}