package com.fspann.key;

import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyRotationServiceImpl implements KeyLifeCycleService {
    private static final Logger logger = LoggerFactory.getLogger(KeyRotationServiceImpl.class);
    private final KeyManager keyManager;
    private final KeyRotationPolicy policy;
    private Instant lastRotation = Instant.now();
    private final AtomicInteger operationCount = new AtomicInteger(0);

    public KeyRotationServiceImpl(KeyManager keyManager, KeyRotationPolicy policy) {
        this.keyManager = keyManager;
        this.policy = policy;
    }

    @Override
    public KeyVersion getCurrentVersion() {
        return keyManager.getCurrentVersion();
    }

    @Override
    public synchronized void rotateIfNeeded() {
        boolean opsExceeded = operationCount.get() >= policy.getMaxOperations();
        boolean timeExceeded = Duration.between(lastRotation, Instant.now()).toMillis() >= policy.getMaxIntervalMillis();
        if (opsExceeded || timeExceeded) {
            KeyVersion newVer = keyManager.rotateKey();
            operationCount.set(0);
            lastRotation = Instant.now();
            try {
                PersistenceUtils.saveObject(newVer, "rotation_" + newVer.getVersion() + ".meta");
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