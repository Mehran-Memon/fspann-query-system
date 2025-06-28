package com.fspann.key;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import com.fspann.crypto.CryptoService;
import com.fspann.common.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
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

    @Override
    public synchronized void rotateIfNeeded() {
        boolean opsExceeded = operationCount.get() >= policy.getMaxOperations();
        boolean timeExceeded = Duration.between(lastRotation, Instant.now()).toMillis() >= policy.getMaxIntervalMillis();
        logger.debug("Checking key rotation: ops={}, timeSinceLast={}ms",
                operationCount.get(), Duration.between(lastRotation, Instant.now()).toMillis());

        if (opsExceeded || timeExceeded) {
            KeyVersion newVer = keyManager.rotateKey();
            operationCount.set(0);
            lastRotation = Instant.now();

            try {
                String fileName = Paths.get(rotationMetaDir, "rotation_" + newVer.getVersion() + ".meta").toString();
                PersistenceUtils.saveObject(newVer, fileName);
            } catch (IOException e) {
                logger.error("Failed to persist rotated key version {}", newVer.getVersion(), e);
                throw new RuntimeException("Key rotation persistence failed", e);
            }

            // 🔐 Automatically re-encrypt all points
            try {
                logger.info("Starting automatic re-encryption for forward security");
                List<EncryptedPoint> allPoints = metadataManager.getAllEncryptedPoints();
                for (EncryptedPoint pt : allPoints) {
                    EncryptedPoint updated = cryptoService.reEncrypt(pt, newVer.getKey());
                    metadataManager.saveEncryptedPoint(updated);
                }
                logger.info("Re-encryption completed for {} vectors", allPoints.size());
            } catch (Exception e) {
                logger.error("Automatic re-encryption failed", e);
                throw new RuntimeException("Re-encryption after key rotation failed", e);
            }
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

}
