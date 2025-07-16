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
                EncryptedPoint updated = cryptoService.reEncrypt(pt, newVer.getKey());
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
        logger.info("\uD83D\uDD10 Starting manual re-encryption of all vectors");

        if (cryptoService == null || metadataManager == null || indexService == null) {
            logger.warn("Re-encryption skipped: cryptoService, metadataManager, or indexService not initialized");
            return;
        }

        int totalReEncrypted = 0;
        Set<String> seen = new HashSet<>();

        try (Stream<Path> stream = Files.walk(Paths.get(metadataManager.getPointsBaseDir()))) {
            List<Path> pointFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point"))
                    .toList();

            for (Path file : pointFiles) {
                try {
                    EncryptedPoint pt;
                    try {
                        pt = PersistenceUtils.loadObject(file.toString());
                        if (pt == null || !seen.add(pt.getId())) continue;
                    } catch (Exception e) {
                        logger.warn("Skipping unreadable or corrupt point file: {}", file);
                        continue;
                    }

                    // Re-encrypt
                    EncryptedPoint updated = cryptoService.reEncrypt(pt, getCurrentVersion().getKey());

                    // Recompute shardId from decrypted vector
                    double[] rawVec = cryptoService.decryptFromPoint(updated, getCurrentVersion().getKey());
                    int newShardId = indexService.getShardIdForVector(rawVec);

                    // Reconstruct point with new shard
                    EncryptedPoint reindexed = new EncryptedPoint(
                            updated.getId(),
                            newShardId,
                            updated.getIv(),
                            updated.getCiphertext(),
                            updated.getVersion(),
                            updated.getVectorLength()
                    );

                    // Save metadata and point
                    Map<String, String> metadata = Map.of(
                            "version", String.valueOf(reindexed.getVersion()),
                            "shardId", String.valueOf(reindexed.getShardId())
                    );
                    // Save metadata FIRST — always before saving .point
                    metadataManager.putVectorMetadata(reindexed.getId(), metadata);
                    // Then save .point file
                    metadataManager.saveEncryptedPoint(reindexed);

                    logger.info("Re-encrypting point {}: old version {}, new version {}, old shard {}, new shard {}",
                            pt.getId(), pt.getVersion(), reindexed.getVersion(), pt.getShardId(), reindexed.getShardId());
                    EncryptedPoint reloaded = metadataManager.loadEncryptedPoint(reindexed.getId());
                    if (!Objects.equals(reindexed.getCiphertext(), reloaded.getCiphertext())) {
                        logger.error("Re-encrypted point {} ciphertext mismatch", reindexed.getId());
                    }                    if (reloaded.getVersion() != reindexed.getVersion()) {
                        logger.warn("Version mismatch on reload: {} vs {}", reloaded.getVersion(), reindexed.getVersion());
                    }
                    System.out.printf("📎 Point %s loaded from disk with version %d\n", reloaded.getId(), reloaded.getVersion());
                    Map<String, String> checkMeta = metadataManager.getVectorMetadata(reindexed.getId());
                    if (!Objects.equals(checkMeta.get("version"), String.valueOf(reloaded.getVersion()))) {
                        logger.error("Metadata mismatch after save/merge: {} vs {}", checkMeta.get("version"), reloaded.getVersion());
                    }

                    indexService.insert(reindexed);
                    totalReEncrypted++;

                    if (totalReEncrypted % 100 == 0) {
                        logger.info("Re-encrypted {} points so far...", totalReEncrypted);
                        System.gc();
                        Thread.sleep(50);
                    }



                } catch (Exception e) {
                    logger.warn("Failed to re-encrypt from file {}: {}", file, e.getMessage());
                }
            }
            indexService.clearCache();
            metadataManager.cleanupStaleMetadata(seen);

        } catch (IOException e) {
            logger.error("Failed to walk encrypted points directory", e);
        }

        logger.info("✅ Re-encryption completed for {} vectors", totalReEncrypted);
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