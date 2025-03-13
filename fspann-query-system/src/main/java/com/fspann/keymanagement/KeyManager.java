package com.fspann.keymanagement;

import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages cryptographic keys for forward-secure, privacy-preserving ANN queries.
 * Supports a master key and session-specific keys with automatic rotation.
 */
public class KeyManager {

    private SecretKey masterKey;
    private Map<String, SecretKey> sessionKeys;  // Keyed by context (e.g., bucket ID or time epoch)
    private final AtomicInteger timeEpoch;      // Tracks key rotation epochs
    private final int rotationInterval;         // Number of operations or time units between rotations

    public KeyManager(int rotationInterval) {
        this.sessionKeys = new HashMap<>();
        this.timeEpoch = new AtomicInteger(0);
        this.rotationInterval = rotationInterval; // e.g., rotate every 1000 operations
        initializeMasterKey();
    }

    private void initializeMasterKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256, new SecureRandom());
            this.masterKey = keyGen.generateKey();
            generateSessionKey("epoch_0"); // Initial session key
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize master key: " + e.getMessage());
        }
    }

    /**
     * Generates a new session key derived from the master key for a given context.
     * @param context Identifier (e.g., "bucketId" or "epoch_<number>").
     * @return The generated SecretKey.
     */
    public SecretKey generateSessionKey(String context) throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256, new SecureRandom());
        SecretKey sessionKey = keyGen.generateKey();
        sessionKeys.put(context, sessionKey);
        return sessionKey;
    }

    /**
     * Retrieves the current session key for a given context.
     * @param context Identifier (e.g., "bucketId" or "epoch_<number>").
     * @return The SecretKey, or null if not found.
     */
    public SecretKey getSessionKey(String context) {
        return sessionKeys.get(context);
    }

    /**
     * Gets the current epoch's session key (default context).
     * @return The current epoch's SecretKey.
     */
    public SecretKey getCurrentKey() {
        return getSessionKey("epoch_" + timeEpoch.get());
    }

    /**
     * Gets the previous epoch's session key for re-encryption.
     * @return The previous epoch's SecretKey, or null if no previous key exists.
     */
    public SecretKey getPreviousKey() {
        int prevEpoch = timeEpoch.get() - 1;
        return prevEpoch >= 0 ? getSessionKey("epoch_" + prevEpoch) : null;
    }

    /**
     * Rotates the session key for the current epoch, ensuring forward security.
     * @param context Context for the key rotation (e.g., "epoch_<number>").
     * @param encryptedData Data to re-encrypt with the new key.
     * @return The new SecretKey.
     */
    public SecretKey rotateKey(String context, byte[] encryptedData) throws Exception {
        int currentEpoch = timeEpoch.getAndIncrement();
        SecretKey oldKey = getSessionKey(context);
        if (oldKey == null) {
            throw new IllegalArgumentException("No key found for context: " + context);
        }

        // Generate new session key
        SecretKey newKey = generateSessionKey("epoch_" + currentEpoch);

        // Re-encrypt data with the new key for forward security
        if (encryptedData != null) {
            byte[] reEncryptedData = EncryptionUtils.reEncrypt(encryptedData, oldKey, newKey);
            // Update the data store with re-encrypted data (assumed to be handled by caller)
        }

        // Optionally remove old key for strict forward security (commented for debugging)
        // sessionKeys.remove(context);

        return newKey;
    }

    /**
     * Checks if key rotation is needed based on operation count or time.
     * @param operationCount Number of operations since last rotation.
     * @return True if rotation is needed.
     */
    public boolean needsRotation(int operationCount) {
        return operationCount >= rotationInterval;
    }

    /**
     * Rotates all session keys for a given set of contexts.
     * @param contexts List of contexts to rotate.
     * @param encryptedDataMap Map of context to encrypted data for re-encryption.
     */
    public void rotateAllKeys(List<String> contexts, Map<String, byte[]> encryptedDataMap) throws Exception {
        for (String context : contexts) {
            byte[] encryptedData = encryptedDataMap.get(context);
            rotateKey(context, encryptedData);
        }
    }
}