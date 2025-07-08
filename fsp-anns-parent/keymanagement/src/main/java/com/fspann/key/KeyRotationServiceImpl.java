package com.fspann.key;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.common.IndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
                metadataManager.mergeVectorMetadata(updated.getId(), newMetadata);
                reEncrypted.add(updated);
            } catch (IOException e) {
                logger.warn("Skipping point {} (v={}) due to save failure: {}", pt.getId(), pt.getVersion(), e.getMessage());
            }
        }

        return reEncrypted;
    }

    @Override
    public void rotateIfNeeded() {
        rotateIfNeededAndReturnUpdated(); // delegates and discards result
    }

    @Override
    public void reEncryptAll() {
        logger.info("🔐 Starting manual re-encryption of all vectors");

        if (cryptoService == null || metadataManager == null || indexService == null) {
            logger.warn("Re-encryption skipped: cryptoService, metadataManager, or indexService not initialized");
            return;
        }

        List<EncryptedPoint> allPoints = metadataManager.getAllEncryptedPoints();
        List<EncryptedPoint> reEncrypted = new ArrayList<>();

        for (EncryptedPoint pt : allPoints) {
            try {
                EncryptedPoint updated = cryptoService.reEncrypt(pt, getCurrentVersion().getKey());

                // ✅ Step 1: Save re-encrypted point to disk
                metadataManager.saveEncryptedPoint(updated);

                // ✅ Step 2: Only update metadata if save succeeded
                Map<String, String> metadata = new HashMap<>();
                metadata.put("version", String.valueOf(updated.getVersion()));
                metadata.put("shardId", String.valueOf(updated.getShardId()));
                metadataManager.mergeVectorMetadata(updated.getId(), metadata);

                logger.info("Re-encrypted + saved + merged metadata for {} → {}", updated.getId(), metadata);

                // ✅ Step 3: Reinsert into index
                indexService.insert(updated);

                reEncrypted.add(updated);

            } catch (Exception e) {
                logger.warn("Failed to re-encrypt/save point {} (v={}): {}", pt.getId(), pt.getVersion(), e.getMessage());
            }
        }

        logger.info("✅ Re-encryption completed for {} vectors", reEncrypted.size());
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
