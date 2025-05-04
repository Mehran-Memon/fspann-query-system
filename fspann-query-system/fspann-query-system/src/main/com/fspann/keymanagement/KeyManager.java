package com.fspann.keymanagement;

import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyManager {
    private final Map<String, SecretKey> keyStore;
    private final AtomicInteger timeVersion;
    private final int rotationInterval;
    private final AtomicInteger operationCount;
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);
    private SecretKey masterKey;
    private static final int NUM_SHARDS = 32;     // tune for load
    private final Map<Integer, Integer> shardVersion = new ConcurrentHashMap<>();
    private final Map<Integer, NavigableMap<Integer,SecretKey>> shardKeys
            = new ConcurrentHashMap<>();

    // Constructor to initialize from existing keys
    public KeyManager(Map<String, SecretKey> keys, int rotationInterval) {
        this.keyStore = new ConcurrentHashMap<>(keys);  // Initialize with existing keys
        this.timeVersion = new AtomicInteger(keys.size() > 0 ? keys.size() : 1);  // Set the version based on the existing keys
        this.rotationInterval = rotationInterval;
        this.operationCount = new AtomicInteger(0);
        logger.info("Keys loaded. Current version: v{}", timeVersion.get());
    }

    // Constructor to generate a new master key if no existing keys are provided
    public KeyManager(int rotationInterval) {
        this.keyStore = new ConcurrentHashMap<>();
        this.timeVersion = new AtomicInteger(1);  // Default starting version
        this.masterKey = generateMasterKey();
        this.rotationInterval = rotationInterval;
        this.operationCount = new AtomicInteger(0);

        // Generate and store the initial key
        generateAndStoreKey("key_v" + timeVersion.get());
    }


    // ── public helper
    /* ---------- helper: derive shard key from masterKey ---------- */
    private SecretKey deriveKey(SecretKey base, String info, int version) {
        try {
            return HmacKeyDerivationUtils.deriveKey(base, info, version);
        } catch (Exception e) {
            throw new RuntimeException("Key derivation failed: "+info, e);
        }
    }

    /* ---------- called once per shard rotation ---------- */
    public void rotateShard(int shardId) {
        int v = shardVersion.compute(shardId,
                (s, oldV) -> oldV == null ? 1 : oldV + 1);

        SecretKey newK = deriveKey(masterKey, "shard-" + shardId, v);

        shardKeys
                .computeIfAbsent(shardId, x -> new TreeMap<>())
                .put(v, newK);
        logger.info("Shard {} rotated to v{}", shardId, v);
    }

    /* ---------- lookup helper used by tests & index ---------- */
    public SecretKey getShardKey(int shardId, int version) {
        NavigableMap<Integer,SecretKey> m = shardKeys.get(shardId);
        return (m == null) ? null : m.get(version);
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
    public void generateAndStoreKey(String keyVersion) {
        try {
            SecretKey key = HmacKeyDerivationUtils.deriveKey(masterKey, "key", timeVersion.get());
            keyStore.put(keyVersion, key);
            logger.info("Generated and stored: {}", keyVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate and store key for version: " + keyVersion, e);
        }
    }

    // Method to retrieve the current session key for a specific version
    public SecretKey getSessionKey(int version) {
        try {
            return keyStore.get("key_v" + version);  // Retrieve the key for that version
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid session key version: " + version, e);
        }
    }

    // Get the current key (versioned key)
    public SecretKey getCurrentKey() {
        return getSessionKey(timeVersion.get());
    }

    // Method to get the previous key (previous versioned key)
    public SecretKey getPreviousKey() {
        int previousVersion = timeVersion.get() - 1;
        if (previousVersion > 0) {
            return getSessionKey(previousVersion);  // Retrieve the previous session key
        }
        return null; // No previous key for version 0
    }

    // Method to get session key from context (for version retrieval)
    public SecretKey getSessionKey(String context) {
        try {
            String[] parts = context.split("_");
            if (parts.length > 1) {
                int version = Integer.parseInt(parts[1].substring(1));  // Extract numeric part after "v" (e.g., "v1" -> 1)
                return getSessionKey(version);  // Retrieve the key for that version
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid session key context: " + context, e);
        }
        return null;  // Return null if no valid session key context is found
    }

    // Return the key store
    public Map<String, SecretKey> getKeyStore() {
        return keyStore;  // Return the current key store
    }

    // Get current key version
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
