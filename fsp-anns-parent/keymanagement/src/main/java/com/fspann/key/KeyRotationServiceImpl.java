package com.fspann.key;

import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Implements KeyLifecycleService by applying KeyRotationPolicy to KeyManager.
 */
public class KeyRotationServiceImpl implements KeyLifeCycleService {
    private final KeyManager keyManager;
    private final KeyRotationPolicy policy;
    private Instant lastRotation = Instant.now();
    private int operationCount = 0;

    public KeyRotationServiceImpl(KeyManager keyManager, KeyRotationPolicy policy) {
        this.keyManager = keyManager;
        this.policy = policy;
    }

    @Override
    public KeyVersion getCurrentVersion() {
        return keyManager.getCurrentVersion();
    }

    @Override
    public void rotateIfNeeded() {
        boolean opsExceeded = operationCount >= policy.getMaxOperations();
        boolean timeExceeded = Duration.between(lastRotation, Instant.now())
                .toMillis() >= policy.getMaxIntervalMillis();
        if (opsExceeded || timeExceeded) {
            KeyVersion newVer = keyManager.rotateKey();
            operationCount = 0;
            lastRotation = Instant.now();

            // Persist the new KeyVersion (must be Serializable)
            try {
                PersistenceUtils.saveObject(newVer, "rotation_" + newVer.getVersion() + ".meta");
            } catch (IOException ignored) {}
        }
    }

    public void incrementOperation() {
        operationCount++;
    }
}
