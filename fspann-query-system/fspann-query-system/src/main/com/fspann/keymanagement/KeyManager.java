package com.fspann.keymanagement;

import com.fspann.encryption.EncryptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);
    private final SecretKey masterKey;
    private final Map<String, SecretKey> keyStore;
    private final AtomicInteger timeVersion;
    private final int rotationInterval; // The threshold to rotate keys
    private final AtomicInteger operationCount; // Track operations

    // Constructor with rotation interval
    public KeyManager(int rotationInterval) {
        this.rotationInterval = rotationInterval;
        this.timeVersion = new AtomicInteger(1); // Start versioning from v1
        this.masterKey = generateMasterKey();
        this.keyStore = new ConcurrentHashMap<>();
        this.operationCount = new AtomicInteger(0);  // Initialize operation count

        // Generate and store the initial key
        generateAndStoreKey("key_v1");
    }

    // Method to generate master key (AES-256)
    private SecretKey generateMasterKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256, new SecureRandom());
            return keyGen.generateKey();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize master key: " + e.getMessage(), e);
        }
    }

    // Method to generate and store keys based on version
    private void generateAndStoreKey(String keyVersion) {
        try {
            SecretKey key = deriveKey(timeVersion.get());
            keyStore.put(keyVersion, key);
            logger.info("Generated and stored: {}", keyVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate and store key for version: " + keyVersion, e);
        }
    }

    // Derive a new key using HMAC (key derivation)
    private SecretKey deriveKey(int version) {
        try {
            return HmacKeyDerivationUtils.deriveKey(masterKey, "key", version);
        } catch (Exception e) {
            throw new RuntimeException("Failed to derive key for version " + version, e);
        }
    }

    // Get the current key (versioned key)
    public SecretKey getCurrentKey() {
        String keyVersion = "key_v" + timeVersion.get();
        return keyStore.get(keyVersion);
    }

    // Get the previous key (previous versioned key)
    public SecretKey getPreviousKey() {
        int previousVersion = timeVersion.get() - 1;
        if (previousVersion > 0) {
            String keyVersion = "key_v" + previousVersion;
            return keyStore.get(keyVersion);
        }
        return null; // No previous key for version 0
    }

    // Rotate all keys (increase version and generate new keys)
    public void rotateAllKeys(List<String> ids, Map<String, byte[]> encryptedDataMap) throws Exception {
        int currentVersion = timeVersion.getAndIncrement(); // Increment version
        generateAndStoreKey("key_v" + currentVersion);  // Store new key for the new version
        logger.info("Rotated keys: Version {} -> {}", currentVersion, "key_v" + currentVersion);

        // Re-encrypt the data with the new key
        SecretKey oldKey = keyStore.get("key_v" + (currentVersion - 1)); // Get the previous key
        SecretKey newKey = getCurrentKey();  // Get the new key

        for (String id : ids) {
            byte[] encryptedData = encryptedDataMap.get(id);
            // Re-encrypt the encrypted data with the new key version
            byte[] newEncryptedData = EncryptionUtils.reEncryptData(encryptedData, oldKey, newKey);
            encryptedDataMap.put(id, newEncryptedData);
        }
    }

    // Method to get session key from context (for version retrieval)
    public SecretKey getSessionKey(String context) {
        try {
            String[] parts = context.split("_");
            int version = Integer.parseInt(parts[1]);
            return deriveKey(version);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid session key context: " + context);
        }
    }

    // Register a key for a specific ID (used in SecureLSHIndex or other parts)
    public void registerKey(String id, SecretKey currentKey) {
        keyStore.put(id, currentKey);  // Store key against the id (or vector)
        logger.debug("Registered key for vector: {}", id);
    }

    // Remove a key for a specific ID (cleanup)
    public void removeKey(String id) {
        keyStore.remove(id);
        logger.debug("Removed key for vector: {}", id);
    }

    // Get current key version
    public int getTimeVersion() {
        return timeVersion.get();
    }

    // Increment operation count and return if rotation is needed
    public boolean needsRotation() {
        return operationCount.get() >= rotationInterval;
    }

    // Increment the operation count
    public void incrementAndGet() {
        operationCount.incrementAndGet();
    }

    // HMAC-based key derivation function for versioned keys
    public static class HmacKeyDerivationUtils {
        private static final String HMAC_ALGO = "HmacSHA256";

        public static SecretKey deriveKey(SecretKey baseKey, String info, int version) throws Exception {
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(baseKey);
            String context = info + "-" + version;
            byte[] derivedBytes = mac.doFinal(context.getBytes(StandardCharsets.UTF_8));

            // Truncate or pad to 256-bit AES key
            byte[] keyBytes = new byte[32];
            System.arraycopy(derivedBytes, 0, keyBytes, 0, Math.min(32, derivedBytes.length));

            return new SecretKeySpec(keyBytes, "AES");
        }
    }
}
