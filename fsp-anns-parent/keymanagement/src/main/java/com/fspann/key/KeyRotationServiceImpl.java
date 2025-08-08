package com.fspann.key;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class KeyRotationServiceImpl implements KeyLifeCycleService {
    private static final Logger logger = LoggerFactory.getLogger(KeyRotationServiceImpl.class);

    private final KeyManager keyManager;
    private final KeyRotationPolicy policy;
    private final String rotationMetaDir;
    private final RocksDBMetadataManager metadataManager;
    private CryptoService cryptoService;
    private IndexService indexService;

    private volatile Instant lastRotation = Instant.now();
    private final AtomicInteger operationCount = new AtomicInteger(0);

    public KeyRotationServiceImpl(KeyManager keyManager,
                                  KeyRotationPolicy policy,
                                  String rotationMetaDir,
                                  RocksDBMetadataManager metadataManager,
                                  CryptoService cryptoService) {
        this.keyManager = keyManager;
        this.policy = policy;
        this.rotationMetaDir = rotationMetaDir;
        this.metadataManager = metadataManager;
        this.cryptoService = cryptoService;
    }

    @Override
    public KeyVersion getCurrentVersion() {
        return keyManager.getCurrentVersion();
    }

    @Override
    public void rotateIfNeeded() {
        rotateIfNeededAndReturnUpdated();
    }

    @Override
    public KeyVersion getPreviousVersion() {
        return keyManager.getPreviousVersion();
    }

    @Override
    public KeyVersion getVersion(int version) {
        SecretKey key = keyManager.getSessionKey(version);
        if (key == null) throw new IllegalArgumentException("Unknown key version: " + version);
        return new KeyVersion(version, key);
    }

    @Override
    public void reEncryptAll() {
        logger.info("Starting manual re-encryption of all vectors");

        if (cryptoService == null || metadataManager == null || indexService == null) {
            logger.warn("Re-encryption skipped: services not initialized");
            return;
        }

        int total = 0;
        Set<String> seen = new HashSet<>();

        try {
            // Let the metadata manager enumerate all stored points (ids only)
            // If you don't have such a method, you can keep the Files.walk() variant you had.
            List<EncryptedPoint> all = metadataManager.getAllEncryptedPoints();
            for (EncryptedPoint original : all) {
                if (original == null || !seen.add(original.getId())) continue;

                // Decrypt with the key that matches the point's version
                KeyVersion pointVer = getVersion(original.getVersion());
                double[] raw = cryptoService.decryptFromPoint(original, pointVer.getKey());

                // Re-insert with same id; service will encrypt w/ CURRENT key + recompute buckets
                indexService.insert(original.getId(), raw);

                total++;
            }

            // If your service buffers writes, flush now
            EncryptedPointBuffer buf = indexService.getPointBuffer();
            if (buf != null) buf.flushAll();

            // Optional: drop any stale metadata no longer in the active set
            metadataManager.cleanupStaleMetadata(seen);

            logger.info("Re-encryption completed for {} vectors", total);
        } catch (Exception e) {
            logger.error("Re-encryption pipeline failed", e);
            // Intentionally do not throw; partial progress is acceptable, and next pass will pick up leftovers.
        }
    }

    public synchronized List<EncryptedPoint> rotateIfNeededAndReturnUpdated() {
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

        reEncryptAll();
        return Collections.emptyList();
    }

    public synchronized KeyVersion rotateKey() {
        logger.debug("Manual key rotation requested");
        try {
            KeyVersion newVersion = keyManager.rotateKey();
            lastRotation = Instant.now();
            operationCount.set(0);
            logger.info("Manual key rotation complete: version={}", newVersion.getVersion());

            reEncryptAll();

            return newVersion;
        } catch (Exception e) {
            logger.error("Manual key rotation failed", e);
            throw e;
        }
    }

    public void setCryptoService(CryptoService cryptoService) {
        this.cryptoService = cryptoService;
    }

    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }

    public void incrementOperation() {
        operationCount.incrementAndGet();
    }

    public void setLastRotationTime(long timestamp) {
        this.lastRotation = Instant.ofEpochMilli(timestamp);
    }
}
