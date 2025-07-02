package com.fspann.key;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.common.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyRotationServiceImpl implements KeyLifeCycleService {
    private static final Logger logger = LoggerFactory.getLogger(KeyRotationServiceImpl.class);

    private final KeyManager keyManager;
    private final KeyRotationPolicy policy;
    private final String rotationMetaDir;
    private final MetadataManager metadataManager;
    private CryptoService cryptoService;

    private Instant lastRotation = Instant.now();
    private final AtomicInteger operationCount = new AtomicInteger(0);

    public KeyRotationServiceImpl(KeyManager keyManager,
                                  KeyRotationPolicy policy,
                                  String rotationMetaDir,
                                  MetadataManager metadataManager,
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
            EncryptedPoint updated = cryptoService.reEncrypt(pt, newVer.getKey());
            metadataManager.saveEncryptedPoint(updated);
            reEncrypted.add(updated);
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

        if (cryptoService == null || metadataManager == null) {
            logger.warn("Re-encryption skipped: cryptoService or metadataManager not initialized");
            return;
        }

        try {
            List<EncryptedPoint> allPoints = metadataManager.getAllEncryptedPoints();
            List<EncryptedPoint> reEncrypted = new ArrayList<>();

            for (EncryptedPoint pt : allPoints) {
                try {
                    EncryptedPoint updated = cryptoService.reEncrypt(pt, getCurrentVersion().getKey());
                    reEncrypted.add(updated);
                } catch (AesGcmCryptoService.CryptoException e) {
                    logger.warn("Skipping point {} (v={}) due to re-encryption failure: {}",
                            pt.getId(), pt.getVersion(), e.getMessage());
                }
            }


            // Save updated points and metadata
            for (EncryptedPoint pt : reEncrypted) {
                metadataManager.saveEncryptedPoint(pt);
                metadataManager.putVectorMetadata(pt.getId(), String.valueOf(pt.getShardId()), String.valueOf(pt.getVersion()));
            }

            logger.info("Re-encryption completed for {} vectors", reEncrypted.size());

        } catch (Exception e) {
            logger.error("Manual re-encryption failed", e);
            throw new RuntimeException("Manual re-encryption failed", e);
        }
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
        KeyVersion newVersion = keyManager.rotateKey();  // Also persists key
        lastRotation = Instant.now();
        operationCount.set(0);
        return newVersion;
    }

}
