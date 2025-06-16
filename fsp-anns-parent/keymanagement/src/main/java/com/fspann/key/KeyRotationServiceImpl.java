package com.fspann.key;

import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyRotationServiceImpl implements KeyLifeCycleService {
    private static final Logger logger = LoggerFactory.getLogger(KeyRotationServiceImpl.class);
    private final KeyManager keyManager;
    private final KeyRotationPolicy policy;
    private Instant lastRotation = Instant.now();
    private final AtomicInteger operationCount = new AtomicInteger(0);
    private final String rotationMetaDir;

    public KeyRotationServiceImpl(KeyManager keyManager, KeyRotationPolicy policy, String rotationMetaDir) {
        this.keyManager = keyManager;
        this.policy = policy;
        this.rotationMetaDir = rotationMetaDir;

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


}