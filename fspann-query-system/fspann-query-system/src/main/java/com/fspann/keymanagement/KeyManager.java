package com.fspann.keymanagement;

import com.fspann.encryption.EncryptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);
    private final SecretKey masterKey;
    private SecretKey currentKey;
    private SecretKey previousKey;
    private final AtomicInteger timeEpoch;
    private final int rotationInterval;
    private final Map<String, SecretKey> keyStore;  // <-- Added this line

    public KeyManager(int rotationInterval) {
        this.rotationInterval = rotationInterval;
        this.timeEpoch = new AtomicInteger(0);
        this.masterKey = generateMasterKey();
        this.currentKey = deriveKey(0);
        this.previousKey = null;
        this.keyStore = new ConcurrentHashMap<>(); // <-- Initialize keyStore
    }

    private SecretKey generateMasterKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256, new SecureRandom());
            return keyGen.generateKey();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize master key: " + e.getMessage(), e);
        }
    }

    private SecretKey deriveKey(int epoch) {
        try {
            return HmacKeyDerivationUtils.deriveKey(masterKey, "epoch", epoch);
        } catch (Exception e) {
            throw new RuntimeException("Failed to derive key for epoch " + epoch, e);
        }
    }

    public SecretKey getCurrentKey() {
        return currentKey;
    }

    public SecretKey getPreviousKey() {
        return previousKey;
    }

    public boolean needsRotation(int operationCount) {
        return operationCount >= rotationInterval;
    }

    public void rotateAllKeys(List<String> ids, Map<String, byte[]> encryptedDataMap) throws Exception {
        int oldEpoch = timeEpoch.getAndIncrement();
        previousKey = currentKey;
        currentKey = deriveKey(oldEpoch + 1);
    }

    public int getTimeEpoch() {
        return timeEpoch.get();
    }

    // For compatibility; no longer needed in KDF-based model
    public void registerKey(String id, SecretKey currentKey) {
        // No-op
    }

    public SecretKey getSessionKey(String context) {
        try {
            String[] parts = context.split("_");
            int epoch = Integer.parseInt(parts[1]);
            return deriveKey(epoch);
        } catch (Exception e) {
            return null;
        }
    }

    public void removeKey(String id) {
        keyStore.remove(id);
        logger.debug("Removed key for vector: {}", id);
    }

    /**
     * HMAC-based key derivation function.
     */
    public static class HmacKeyDerivationUtils {
        private static final String HMAC_ALGO = "HmacSHA256";

        public static SecretKey deriveKey(SecretKey baseKey, String info, int epoch) throws Exception {
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(baseKey);
            String context = info + "-" + epoch;
            byte[] derivedBytes = mac.doFinal(context.getBytes(StandardCharsets.UTF_8));

            // Truncate or pad to 256-bit AES key
            byte[] keyBytes = new byte[32];
            System.arraycopy(derivedBytes, 0, keyBytes, 0, Math.min(32, derivedBytes.length));

            return new SecretKeySpec(keyBytes, "AES");
        }
    }
}
