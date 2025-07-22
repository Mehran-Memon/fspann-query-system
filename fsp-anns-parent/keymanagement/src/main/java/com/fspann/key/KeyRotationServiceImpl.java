package com.fspann.key;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import com.fspann.crypto.CryptoService;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.common.IndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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

    private Instant lastRotation = Instant.now();
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

    public synchronized List<EncryptedPoint> rotateIfNeededAndReturnUpdated() {
        boolean opsExceeded = operationCount.get() >= policy.getMaxOperations();
        boolean timeExceeded = Duration.between(lastRotation, Instant.now()).toMillis() >= policy.getMaxIntervalMillis();

        if (!(opsExceeded || timeExceeded)) return Collections.emptyList();

        KeyVersion newVer = keyManager.rotateKey();
        operationCount.set(0);
        lastRotation = Instant.now();

        try {
            String fileName = Paths.get(rotationMetaDir, "rotation_" + newVer.getVersion() + ".meta").toString();
            PersistenceUtils.saveObject(newVer, fileName);
        } catch (IOException e) {
            throw new RuntimeException("Key rotation persistence failed", e);
        }

        List<EncryptedPoint> reEncrypted = new ArrayList<>();
        for (EncryptedPoint pt : metadataManager.getAllEncryptedPoints()) {
            try {
                EncryptedPoint updated = cryptoService.reEncrypt(pt, newVer.getKey(), cryptoService.generateIV());
                metadataManager.saveEncryptedPoint(updated);
                Map<String, String> newMetadata = new HashMap<>();
                newMetadata.put("version", String.valueOf(updated.getVersion()));
                newMetadata.put("shardId", String.valueOf(updated.getShardId()));
                metadataManager.updateVectorMetadata(updated.getId(), newMetadata);
                reEncrypted.add(updated);
            } catch (IOException e) {
                logger.warn("Skipping point {} (v={}) due to save failure: {}", pt.getId(), pt.getVersion(), e.getMessage());
            }
        }

        return reEncrypted;
    }

    @Override
    public void rotateIfNeeded() {
        rotateIfNeededAndReturnUpdated();
    }

    @Override
    public void reEncryptAll() {
        logger.info("Starting manual re-encryption of all vectors");

        if (cryptoService == null || metadataManager == null || indexService == null) {
            logger.warn("Re-encryption skipped: cryptoService, metadataManager, or indexService not initialized");
            return;
        }

        int totalReEncrypted = 0;
        Set<String> seen = new HashSet<>();
        Path baseDir = Paths.get(metadataManager.getPointsBaseDir());

        try (Stream<Path> stream = Files.walk(baseDir)) {
            List<Path> pointFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point"))
                    .toList();

            for (Path file : pointFiles) {
                try {
                    // 1) Load original point
                    EncryptedPoint original = PersistenceUtils.loadObject(file.toString());
                    if (original == null || !seen.add(original.getId())) continue;

                    // 2) Re-encrypt under new key & IV
                    byte[] newIv = cryptoService.generateIV();
                    EncryptedPoint updated = cryptoService.reEncrypt(
                            original,
                            getCurrentVersion().getKey(),
                            newIv
                    );

                    // 3) Determine new shard from the decrypted vector
                    double[] rawVec = cryptoService.decryptFromPoint(updated, getCurrentVersion().getKey());
                    int newShard = indexService.getShardIdForVector(rawVec);

                    // 4) Build final reindexed point
                    EncryptedPoint reindexed = new EncryptedPoint(
                            updated.getId(),
                            newShard,
                            updated.getIv(),
                            updated.getCiphertext(),
                            updated.getVersion(),
                            updated.getVectorLength()
                    );

                    // 5) Update metadata in RocksDB before touching the file
                    Map<String, String> meta = Map.of(
                            "version", String.valueOf(reindexed.getVersion()),
                            "shardId", String.valueOf(reindexed.getShardId())
                    );
                    metadataManager.putVectorMetadata(reindexed.getId(), meta);

                    // 6) Atomically overwrite the .point file via a .tmp swap
                    Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
                    PersistenceUtils.saveObject(reindexed, tmp.toString());  // ← object, then path
                    Files.move(
                            tmp,
                            file,
                            StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING
                    );

                    logger.info(
                            "Re-encrypted point {}: v{}→v{}, shard {}→{}",
                            reindexed.getId(),
                            original.getVersion(),
                            reindexed.getVersion(),
                            original.getShardId(),
                            reindexed.getShardId()
                    );

                    // 7) Reinstate into in-memory index
                    indexService.insert(reindexed);
                    totalReEncrypted++;

                    // 8) Throttle & occasional GC
                    if (totalReEncrypted % 100 == 0) {
                        logger.info("🌀 Re-encrypted {} points so far…", totalReEncrypted);
                        System.gc();
                        Thread.sleep(50);
                    }

                } catch (Exception e) {
                    logger.warn("Failed to re-encrypt {}: {}", file, e.getMessage());
                }
            }

            // 9) Final cleanup
            indexService.clearCache();
            metadataManager.cleanupStaleMetadata(seen);

        } catch (IOException e) {
            logger.error("Failed walking encrypted-points dir", e);
        }

        logger.info("Re-encryption completed for {} vectors", totalReEncrypted);
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

    public void incrementOperation() {
        operationCount.incrementAndGet();
    }

    public void setCryptoService(CryptoService cryptoService) {
        this.cryptoService = cryptoService;
    }

    public KeyVersion rotateKey() {
        KeyVersion newVersion = keyManager.rotateKey();
        lastRotation = Instant.now();
        operationCount.set(0);
        return newVersion;
    }

    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }
}