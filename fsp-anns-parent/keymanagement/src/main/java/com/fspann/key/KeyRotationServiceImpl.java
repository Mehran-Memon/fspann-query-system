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
import java.util.concurrent.ConcurrentHashMap;
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

        int totalReEncrypted = 0;
        Set<String> seen = new HashSet<>();
        Path baseDir = Paths.get(metadataManager.getPointsBaseDir());
        Map<String, Map<String, String>> pendingMetadata = new HashMap<>();

        try (Stream<Path> stream = Files.walk(baseDir)) {
            List<Path> pointFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point"))
                    .toList();

            for (Path file : pointFiles) {
                try {
                    EncryptedPoint original = PersistenceUtils.loadObject(file.toString(), baseDir.toString(), EncryptedPoint.class);
                    if (original == null || !seen.add(original.getId())) continue;

                    byte[] newIv = cryptoService.generateIV();
                    EncryptedPoint updated = cryptoService.reEncrypt(original, getCurrentVersion().getKey(), newIv);
                    double[] rawVec = cryptoService.decryptFromPoint(updated, getCurrentVersion().getKey());
                    int newShard = indexService.getShardIdForVector(rawVec);

                    EncryptedPoint reindexed = new EncryptedPoint(
                            updated.getId(), updated.getVectorLength(),
                            updated.getCiphertext(), updated.getIv(),
                            newShard, getCurrentVersion().getVersion(), null
                    );

                    Map<String, String> meta = Map.of(
                            "version", String.valueOf(reindexed.getVersion()),
                            "shardId", String.valueOf(reindexed.getShardId())
                    );
                    metadataManager.updateVectorMetadata(reindexed.getId(), meta);
                    pendingMetadata.put(reindexed.getId(), meta);

                    EncryptedPointBuffer buffer = indexService.getPointBuffer();
                    if (buffer != null) {
                        buffer.add(reindexed);
                    } else {
                        Path versionDir = baseDir.resolve("v" + reindexed.getVersion());
                        Files.createDirectories(versionDir);
                        Path updatedFile = versionDir.resolve(reindexed.getId() + ".point");
                        PersistenceUtils.saveObject(reindexed, updatedFile.toString(), baseDir.toString());
                    }

                    indexService.insert(reindexed);
                    totalReEncrypted++;

                    if (totalReEncrypted % 10000 == 0) {
                        metadataManager.batchUpdateVectorMetadata(pendingMetadata);
                        pendingMetadata.clear();
                        logger.info("Re-encrypted {} points so far", totalReEncrypted);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to re-encrypt {}: {}", file, e.getMessage());
                }
            }

            if (!pendingMetadata.isEmpty()) {
                metadataManager.batchUpdateVectorMetadata(pendingMetadata);
                logger.debug("Final batch update for {} metadata entries", pendingMetadata.size());
            }

            EncryptedPointBuffer buffer = indexService.getPointBuffer();
            if (buffer != null) {
                buffer.flushAll();
                logger.debug("Flushed point buffer after re-encryption");
            }

            indexService.clearCache();
            metadataManager.cleanupStaleMetadata(seen);

        } catch (IOException e) {
            logger.error("Failed walking encrypted-points dir", e);
        }

        logger.info("Re-encryption completed for {} vectors", totalReEncrypted);
    }

    public synchronized List<EncryptedPoint> rotateIfNeededAndReturnUpdated() {
        boolean opsExceeded = operationCount.get() >= policy.getMaxOperations();
        boolean timeExceeded = Duration.between(lastRotation, Instant.now()).toMillis() >= policy.getMaxIntervalMillis();

        if (!(opsExceeded || timeExceeded)) {
            return Collections.emptyList();
        }

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
        return metadataManager.getAllEncryptedPoints();
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
