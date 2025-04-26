package com.fspann.keymanagement;

import javax.crypto.SecretKey;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyVersionManager {
    private final KeyManager keyManager;
    private final AtomicInteger timeVersion;
    private final int rotationInterval;
    private final AtomicInteger operationCount;

    // Constructor initializes with KeyManager and rotationInterval
    public KeyVersionManager(KeyManager keyManager, int rotationInterval) {
        this.keyManager = keyManager;
        this.timeVersion = new AtomicInteger(1); // Start versioning from v1
        this.operationCount = new AtomicInteger(0);  // Initialize the operation count
        this.rotationInterval = rotationInterval; // Set the rotation interval (provided via constructor)
    }

    // Get the KeyManager instance
    public KeyManager getKeyManager() {
        return keyManager;  // Return the KeyManager instance stored in the class
    }

    // Get the current key (versioned key)
    public SecretKey getCurrentKey() {
        return keyManager.getCurrentKey();  // Use KeyManager's method to get the current key
    }

    // Get the previous key (previous versioned key)
    public SecretKey getPreviousKey() {
        return keyManager.getPreviousKey();  // Use KeyManager's method to get the previous key
    }

    // Rotate keys (increase the version)
    public void rotateKeys() {
        timeVersion.incrementAndGet();  // Increment version number in KeyManager
    }

    // Method to check if key rotation is needed
    public boolean needsRotation() {
        return operationCount.get() >= rotationInterval;  // Return true if operation count exceeds the interval
    }

    // Increment operation count for each operation
    public void incrementOperationCount() {
        operationCount.incrementAndGet();  // Increase the operation count
    }

    // Get current version (returns the current version of the key)
    public int getTimeVersion() {
        return timeVersion.get();  // Return the current key version
    }
}
