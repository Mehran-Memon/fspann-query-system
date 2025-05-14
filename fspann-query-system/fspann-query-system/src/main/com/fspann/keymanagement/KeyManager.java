package com.fspann.keymanagement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.fspann.index.DimensionContext;

public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);

    // Constants
    private static final String KEY_ALGORITHM = "AES";
    private static final String KEY_DERIVATION_ALGO = "HmacSHA256";
    private static final int KEY_SIZE = 256;
    private static final int NUM_SHARDS = 32;
    private static final long KEY_EXPIRATION_DAYS = 30;
    private static final long KEY_EXPIRATION_MILLIS = TimeUnit.DAYS.toMillis(KEY_EXPIRATION_DAYS);

    // Rotation policy & state
    private final KeyRotationPolicy rotationPolicy;
    private volatile long lastRotationTime = System.currentTimeMillis();
    private final AtomicInteger timeVersion = new AtomicInteger(1);
    private final AtomicInteger operationCount = new AtomicInteger(0);
    private volatile boolean rotationInProgress = false;
    private final Object rotationLock = new Object();

    // Trigger key rotation if policy dictates
    private int keyRotationCount = 0;
    private static final int MAX_KEY_ROTATIONS = 500;

    // Master & session keys
    private SecretKey masterKey;
    private final Map<String, SecretKey> keyStore = new ConcurrentHashMap<>();
    private String currentKey;  // The current encryption key (base64 encoded for easy storage)

    // Shard keys & versions
    private final Map<Integer, Integer> shardVersion = new ConcurrentHashMap<>();
    private final Map<Integer, NavigableMap<Integer, SecretKey>> shardKeys = new ConcurrentHashMap<>();

    // Usage tracking
    private final Map<SecretKey, KeyUsage> keyUsage = new ConcurrentHashMap<>();


    public KeyManager(String keysFilePath, KeyRotationPolicy rotationPolicy) throws IOException {
        this.rotationPolicy = Objects.requireNonNull(rotationPolicy);
        initialize(keysFilePath);
    }

    // Core key retrieval
    public SecretKey getSessionKey(int version) {
        SecretKey key = keyStore.get("key_v" + version);
        if (key == null) {
            throw new IllegalArgumentException("Invalid session key version: " + version);
        }
        trackKeyUsage(key);
        return key;
    }

    public SecretKey getPreviousKey() {
        int prev = timeVersion.get() - 1;
        return prev > 0 ? getSessionKey(prev) : null;
    }

    public SecretKey getCurrentKey() {
        SecretKey key = getSessionKey(timeVersion.get());
        trackKeyUsage(key);
        rotateKeysIfNeeded();
        return key;
    }

    // Key generation & storage
    public void generateAndStoreKey(String keyVersion) {
        try {
            SecretKey key = HmacKeyDerivationUtils.deriveKey(masterKey, "key", timeVersion.get());
            keyStore.put(keyVersion, key);
            logger.info("Generated and stored session key: {}", keyVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to derive session key: " + keyVersion, e);
        }
    }

    // Shard key retrieval
    public SecretKey getShardKey(int shardId, int version) {
        NavigableMap<Integer, SecretKey> map = shardKeys.get(shardId);
        return map != null ? map.get(version) : null;
    }

    // Rotate all shards under rotation lock
    private void rotateAllShards() {
        for (Integer shardId : shardVersion.keySet()) {
            performShardRotation(shardId);
        }
    }


    public void rotateKeysIfNeeded() {
        if (keyRotationCount < MAX_KEY_ROTATIONS && !rotationInProgress && shouldRotateKeys()) {
            synchronized (rotationLock) {
                if (keyRotationCount < MAX_KEY_ROTATIONS && !rotationInProgress && shouldRotateKeys()) {
                    rotationInProgress = true;
                    try {
                        performKeyRotation();
                        keyRotationCount++;  // Increment rotation counter
                    } finally {
                        rotationInProgress = false;
                    }
                }
            }
        } else {
            logger.info("Max key rotations reached. Skipping further rotations.");
        }
    }

    private void performKeyRotation() {
        logger.info("Initiating key rotation...");
        int newVersion = timeVersion.incrementAndGet();
        try {
            generateAndStoreKey("key_v" + newVersion);
            rotateAllShards();
            lastRotationTime = System.currentTimeMillis();
            operationCount.set(0);
            logger.info("Key rotation completed. Current version: {}", newVersion);
        } catch (Exception e) {
            timeVersion.decrementAndGet();
            throw new KeyRotationException("Key rotation failed", e);
        }
    }

    private boolean shouldRotateKeys() {
        long currentTime = System.currentTimeMillis();
        boolean operationLimitReached = operationCount.get() >= rotationPolicy.getMaxOperations();
        boolean timeLimitExceeded = (currentTime - lastRotationTime) >= rotationPolicy.getMaxIntervalMillis();
        return operationLimitReached || timeLimitExceeded;
    }


    // Shard rotation helpers
    private void performShardRotation(int shardId) {
        int newVersion = shardVersion.compute(shardId, (id, oldV) -> (oldV == null ? 1 : oldV + 1));
        SecretKey newKey = deriveKey(masterKey, "shard-" + shardId, newVersion);
        shardKeys.computeIfAbsent(shardId, id -> new TreeMap<>()).put(newVersion, newKey);
        logger.info("Shard {} rotated to version {}", shardId, newVersion);
    }

    // Initialization & persistence
    private void initialize(String keysFilePath) throws IOException {
        File file = new File(keysFilePath);
        if (file.exists()) loadKeys(file);
        else {
            masterKey = generateSecureKey();
            generateAndStoreKey("key_v" + timeVersion.get());
            saveKeys(keysFilePath);
            logger.info("Initialized new key store and master key");
        }
    }

    private void loadKeys(File file) throws IOException {
        try {
            byte[] encrypted = Files.readAllBytes(file.toPath());
            byte[] plain = decryptKeyData(encrypted);
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(plain))) {
                @SuppressWarnings("unchecked")
                Map<String, SecretKey> loaded = (Map<String, SecretKey>) ois.readObject();
                keyStore.putAll(loaded);
                int maxVer = loaded.keySet().stream()
                        .map(k -> k.replace("key_v", ""))
                        .mapToInt(Integer::parseInt)
                        .max().orElse(1);
                timeVersion.set(maxVer);
                logger.info("Loaded keys up to version {}", maxVer);
            }
        } catch (Exception e) {
            throw new IOException("Failed to load keys from file", e);
        }

    }

    public void saveKeys(String keysFilePath) throws IOException {
        synchronized (rotationLock) {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(keyStore);
                }
                byte[] encrypted = encryptKeyData(bos.toByteArray());
                Files.write(Paths.get(keysFilePath), encrypted);
            } catch (Exception e) {
                throw new IOException("Failed to save keys", e);
            }
        }
    }

    // Encryption utils
    private byte[] encryptKeyData(byte[] data) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        byte[] iv = new byte[12]; new SecureRandom().nextBytes(iv);
        cipher.init(Cipher.ENCRYPT_MODE, masterKey, new GCMParameterSpec(128, iv));
        byte[] ct = cipher.doFinal(data);
        ByteBuffer buf = ByteBuffer.allocate(iv.length + ct.length);
        buf.put(iv).put(ct);
        return buf.array();
    }

    private byte[] decryptKeyData(byte[] data) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(data);
        byte[] iv = new byte[12]; buf.get(iv);
        byte[] ct = new byte[buf.remaining()]; buf.get(ct);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, masterKey, new GCMParameterSpec(128, iv));
        return cipher.doFinal(ct);
    }

    private SecretKey generateSecureKey() {
        try {
            KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGORITHM);
            kg.init(KEY_SIZE, SecureRandom.getInstanceStrong());
            return kg.generateKey();
        } catch (Exception e) {
            throw new SecurityException("Secure key generation failed", e);
        }
    }

    private SecretKey deriveKey(SecretKey base, String info, int version) {
        try {
            return HmacKeyDerivationUtils.deriveKey(base, info, version);
        } catch (Exception e) {
            throw new RuntimeException("Key derivation error", e);
        }
    }

    // Usage tracking
    private void trackKeyUsage(SecretKey key) {
        KeyUsage usage = keyUsage.computeIfAbsent(key, k -> new KeyUsage());
        usage.activeOperations.incrementAndGet();
        usage.lastUsed.set(System.currentTimeMillis());
        operationCount.incrementAndGet();
    }

    public void cleanExpiredKeys() {
        long now = System.currentTimeMillis();
        keyUsage.entrySet().removeIf(e -> {
            KeyUsage u = e.getValue();
            if (u.activeOperations.get() == 0 && (now - u.lastUsed.get()) > KEY_EXPIRATION_MILLIS) {
                removeKeyFromStore(e.getKey());
                return true;
            }
            return false;
        });
    }

    private void removeKeyFromStore(SecretKey key) {
        int cur = timeVersion.get();
        keyStore.entrySet().removeIf(e -> {
            String[] parts = e.getKey().split("_v");
            if (parts.length == 2) {
                try {
                    int v = Integer.parseInt(parts[1]);
                    return e.getValue().equals(key) && v < cur - 1;
                } catch (NumberFormatException ex) {
                    return false;
                }
            }
            return false;
        });
    }

    // Utilities & exceptions
    private static class KeyUsage {
        final AtomicInteger activeOperations = new AtomicInteger(0);
        final AtomicLong lastUsed = new AtomicLong(System.currentTimeMillis());
    }

    public static class HmacKeyDerivationUtils {
        public static SecretKey deriveKey(SecretKey baseKey, String info, int version) throws Exception {
            Mac mac = Mac.getInstance(KEY_DERIVATION_ALGO);
            mac.init(baseKey);
            byte[] derived = mac.doFinal((info + "-" + version).getBytes(StandardCharsets.UTF_8));
            byte[] keyBytes = Arrays.copyOf(derived, 32);
            return new SecretKeySpec(keyBytes, KEY_ALGORITHM);
        }
    }

    public int getTimeVersion() {
        return timeVersion.get();
    }

    /**
     * Thrown when a key rotation operation fails and needs to be rolled back.
     */
    public class KeyRotationException extends RuntimeException {
        public KeyRotationException(String message) {
            super(message);
        }

        public KeyRotationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Returns an unmodifiable view of the current key store (version → SecretKey).
     */
    public Map<String, SecretKey> getKeyStore() {
        return Collections.unmodifiableMap(keyStore);
    }

    // Method to perform re-encryption of a DimensionContext
    public void reEncrypt(DimensionContext context) {
        if (context == null) {
            logger.error("DimensionContext is null, cannot re-encrypt");
            return;
        }

        try {
            // Assuming `DimensionContext` has a method to get the encrypted data
            byte[] encryptedData = context.getEncryptedData();
            if (encryptedData == null || encryptedData.length == 0) {
                logger.warn("No data to re-encrypt for context: {}", context.getId());
                return;
            }

            // Re-encrypt the data using the new key
            byte[] reEncryptedData = performEncryption(encryptedData);

            // Update the DimensionContext with the re-encrypted data
            context.setEncryptedData(reEncryptedData);

            logger.info("Successfully re-encrypted data for context: {}", context.getId());
        } catch (Exception e) {
            logger.error("Error during re-encryption of context: {}", context.getId(), e);
        }
    }

    // Example method to perform encryption with the new key
    private byte[] performEncryption(byte[] data) throws Exception {
        Key secretKey = getKey();  // You can modify this to use the key manager's current key
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);  // Initialize cipher in encryption mode
        return cipher.doFinal(data);  // Encrypt the data
    }

    // Fetch or generate a new encryption key (for demonstration, we'll generate it here)
    private Key getKey() throws Exception {
        if (currentKey == null || currentKey.isEmpty()) {
            // If no key exists, generate a new one
            logger.info("Generating a new AES key...");
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(256);  // 256-bit AES key
            SecretKey secretKey = keyGenerator.generateKey();
            currentKey = Base64.getEncoder().encodeToString(secretKey.getEncoded());  // Store the key as base64 string
        }

        // Convert the current key (base64 string) to a Key object
        byte[] decodedKey = Base64.getDecoder().decode(currentKey);
        return new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");  // Create a Key from the base64 string
    }
}
