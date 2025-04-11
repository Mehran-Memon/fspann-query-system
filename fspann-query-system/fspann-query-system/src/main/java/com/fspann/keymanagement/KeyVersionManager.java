package java.com.fspann.keymanagement;

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
        String keyVersion = "key_v" + timeVersion.get();
        return keyManager.getSessionKey(keyVersion);  // Retrieve session key based on version
    }

    // Get the previous key (previous versioned key)
    public SecretKey getPreviousKey() {
        int previousVersion = timeVersion.get() - 1;
        if (previousVersion > 0) {
            String keyVersion = "key_v" + previousVersion;
            return keyManager.getSessionKey(keyVersion);  // Retrieve the previous session key
        }
        return null; // No previous key for version 0
    }

    // Rotate keys (increase the version)
    public void rotateKeys() {
        timeVersion.incrementAndGet();  // Increment version number
    }

    // Method to check if key rotation is needed
    public boolean needsRotation(int operationCount) {
        return operationCount >= rotationInterval;  // Return true if operation count exceeds the interval
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
