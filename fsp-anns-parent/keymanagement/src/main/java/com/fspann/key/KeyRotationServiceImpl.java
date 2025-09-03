package com.fspann.key;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Paths;
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
 * Notes:
 *  - When a version is "forced" (pinned), auto-rotation is disabled.
 *  - Re-encryption enumerates points via metadata manager; falls back gracefully if unsupported.
 */
public class KeyRotationServiceImpl implements KeyLifeCycleService {
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
        if (forcedVersion != null) return;               // pinned → never rotate
        if (Boolean.getBoolean("skip.rotation")) return; // runtime flag
        rotateIfNeededAndReturnUpdated();
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
        if (forcedVersion != null) return Collections.emptyList();
        boolean opsExceeded  = operationCount.get() >= policy.getMaxOperations();
        boolean timeExceeded = Duration.between(lastRotation, Instant.now()).toMillis() >= policy.getMaxIntervalMillis();
        if (!(opsExceeded || timeExceeded)) return Collections.emptyList();

        logger.info("Initiating key rotation...");
        KeyVersion newVer = keyManager.rotateKey();
        operationCount.set(0);
        lastRotation = Instant.now();

        try {
            String fileName = Paths.get(rotationMetaDir, "rotation_" + newVer.getVersion() + ".meta").toString();
            PersistenceUtils.saveObject(newVer, fileName, rotationMetaDir);
        } catch (IOException e) {
            logger.error("Failed to persist new key version", e);
            throw new RuntimeException(e);
        }

        reEncryptAll(); // forward security: old keys can be discarded after this
        return Collections.emptyList();
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

}
