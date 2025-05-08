package com.fspann.keymanagement;

import com.fspann.encryption.EncryptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.fspann.query.EncryptedPoint;

public class KeyManager {
    private final Map<String, SecretKey> keyStore;
    private final AtomicInteger timeVersion;
    private final int rotationInterval;
    private final AtomicInteger operationCount;
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);
    private SecretKey masterKey;
    private static final int NUM_SHARDS = 32;
    private final Map<Integer, Integer> shardVersion = new ConcurrentHashMap<>();
    private final Map<Integer, NavigableMap<Integer, SecretKey>> shardKeys = new ConcurrentHashMap<>();
    private int CountOperation;
    private int maxOperationsBeforeRotation = 1000; // Number of operations after which key rotation is triggered
    private Map<String, EncryptedPoint> encryptedDataStore;


    // Constructor to load keys from file or create new keys if not found
    public KeyManager(String keysFilePath, int rotationInterval) throws IOException {
        this.keyStore = new ConcurrentHashMap<>();
        this.rotationInterval = rotationInterval;
        this.operationCount = new AtomicInteger(0);
        this.timeVersion = new AtomicInteger(1);  // Default starting version
        this.encryptedDataStore = new ConcurrentHashMap<>(); // Initialize encryptedDataStore

        File keysFile = new File(keysFilePath);
        if (keysFile.exists()) {
            // Load existing keys from file
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(keysFile))) {
                Map<String, SecretKey> loadedKeys = (Map<String, SecretKey>) ois.readObject();
                this.keyStore.putAll(loadedKeys); // Populate keyStore with loaded keys
            } catch (Exception e) {
                throw new IOException("Failed to load keys from file", e);
            }
        } else {
            // If the keys file does not exist, create new keys
            logger.info("Keys file not found, generating new keys...");
            this.masterKey = generateMasterKey();
            generateAndStoreKey("key_v" + timeVersion.get()); // Generate and store the first key
        }
    }

    // Helper method to derive a key from the master key
    private SecretKey deriveKey(SecretKey base, String info, int version) {
        try {
            return HmacKeyDerivationUtils.deriveKey(base, info, version);
        } catch (Exception e) {
            throw new RuntimeException("Key derivation failed: " + info, e);
        }
    }

    // Method to save keys to file
    public void saveKeys(String keysFilePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(keysFilePath))) {
            oos.writeObject(this.keyStore);
        }
    }

    // Method to rotate shard and generate new keys
    public void rotateShard(int shardId) {
        int v = shardVersion.compute(shardId, (s, oldV) -> oldV == null ? 1 : oldV + 1);
        SecretKey newK = deriveKey(masterKey, "shard-" + shardId, v);

        shardKeys.computeIfAbsent(shardId, x -> new TreeMap<>()).put(v, newK);
        logger.info("Shard {} rotated to v{}", shardId, v);
    }

    // Lookup method to get the session key for a given shard
    public SecretKey getShardKey(int shardId, int version) {
        NavigableMap<Integer, SecretKey> m = shardKeys.get(shardId);
        return (m == null) ? null : m.get(version);
    }

    // Method to generate the master key (AES-256)
    private SecretKey generateMasterKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256, new SecureRandom());
            return keyGen.generateKey();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize master key: " + e.getMessage(), e);
        }
    }

    public SecretKey getSessionKey(int version) {
        try {
            return keyStore.get("key_v" + version);  // Retrieve the key for that version
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid session key version: " + version, e);
        }
    }

    // Method to generate and store keys based on version
    public void generateAndStoreKey(String keyVersion) {
        try {
            SecretKey key = HmacKeyDerivationUtils.deriveKey(masterKey, "key", timeVersion.get());
            keyStore.put(keyVersion, key);
            logger.info("Generated and stored: {}", keyVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate and store key for version: " + keyVersion, e);
        }
    }

    // Get current key (versioned key)
    public SecretKey getCurrentKey() {
        return getSessionKey(timeVersion.get());  // This will call the int-based getSessionKey method
    }

    // Get the previous key
    public SecretKey getPreviousKey() {
        int previousVersion = timeVersion.get() - 1;
        if (previousVersion > 0) {
            return getSessionKey(previousVersion);  // This will call the int-based getSessionKey method
        }
        return null;  // No previous key for version 0
    }

    // Method to get session key from context
    public SecretKey getSessionKey(String context) {
        try {
            String[] parts = context.split("_");
            if (parts.length > 1) {
                int version = Integer.parseInt(parts[1].substring(1));  // Extract version number
                return getSessionKey(version);  // Call the overloaded version of getSessionKey
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid session key context: " + context, e);
        }
        return null;
    }
    // Method to rotate keys when necessary based on operation count
    public void rotateKeysIfNeeded() {
        if (operationCount.get() >= maxOperationsBeforeRotation) {
            rotateKeys();
            operationCount.set(0); // Reset counter after key rotation
        }
    }

    // Method to perform the key rotation
    private void rotateKeys() {
        logger.info("Rotating keys...");
        generateAndStoreKey("key_v" + timeVersion.incrementAndGet()); // Increment version and generate new key
    }

    // Method to rehash index when keys are rotated
    public void rehashIndexForNewKeys() {
        logger.info("Rehashing index due to key rotation...");

        // Create a temporary list to hold the data that needs to be re-encrypted
        List<Map.Entry<String, EncryptedPoint>> entriesToReEncrypt = new ArrayList<>();

        // Collect all entries from encryptedDataStore for re-encryption
        for (Map.Entry<String, EncryptedPoint> entry : encryptedDataStore.entrySet()) {
            entriesToReEncrypt.add(entry);
        }

        // Now, iterate over the collected entries and re-encrypt
        for (Map.Entry<String, EncryptedPoint> entry : entriesToReEncrypt) {
            EncryptedPoint ep = entry.getValue();
            try {
                // Re-encrypt the data using the new key
                ep.reEncrypt(this, "key_v" + getTimeVersion()); // Use the current version for re-encryption
            } catch (Exception e) {
                logger.error("Failed to re-encrypt EncryptedPoint for entry: {}", ep.getPointId(), e);
            }
        }
    }

    // Getter for key store
    public Map<String, SecretKey> getKeyStore() {
        return keyStore;
    }

    // Getter for current time version of the key
    public int getTimeVersion() {
        return timeVersion.get();
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
