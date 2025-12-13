package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.RocksDBMetadataManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * AES-GCM Crypto Service with AAD binding.
 *
 * AAD: "id:{id}|v:{version}|d:{dim}"
 *
 * Supports forward-security + selective re-encryption.
 */
public class AesGcmCryptoService implements CryptoService {

    private static final Logger log = LoggerFactory.getLogger(AesGcmCryptoService.class);

    private static final String ALGO = "AES/GCM/NoPadding";
    private static final int IV_BITS = 96;       // 12 bytes
    private static final int TAG_BITS = 128;     // 16 bytes
    private static final int KEY_BITS = 256;

    private final MeterRegistry metrics;
    private KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadataManager;

    // Encryption listeners (optional)
    private final List<CryptoService.EncryptionListener> listeners =
            new CopyOnWriteArrayList<>();

    public AesGcmCryptoService(
            MeterRegistry metrics,
            KeyLifeCycleService keyService,
            RocksDBMetadataManager metadataManager
    ) {
        this.metrics = metrics;
        this.keyService = keyService;
        this.metadataManager = metadataManager;
        log.info("AesGcmCryptoService initialized with AAD binding");
    }

    @Override
    public EncryptedPoint encryptToPoint(String id, double[] plaintext, javax.crypto.SecretKey key) {
        if (id == null || plaintext == null || key == null) {
            throw new IllegalArgumentException("id, plaintext, and key cannot be null");
        }

        try {
            KeyVersion kv = (keyService != null)
                    ? keyService.getCurrentVersion()
                    : new KeyVersion(1, key);
            int version = kv.getVersion();
            int dim = plaintext.length;

            // Generate IV
            byte[] iv = new byte[IV_BITS / 8];
            new java.security.SecureRandom().nextBytes(iv);

            // AAD
            String aadStr = String.format("id:%s|v:%d|d:%d", id, version, dim);
            byte[] aad = aadStr.getBytes();

            // Serialize plaintext
            byte[] plainBytes = serializeVector(plaintext);

            // Encrypt
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(ALGO);
            javax.crypto.spec.GCMParameterSpec spec = new javax.crypto.spec.GCMParameterSpec(TAG_BITS, iv);
            cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, key, spec);
            cipher.updateAAD(aad);
            byte[] ciphertext = cipher.doFinal(plainBytes);

            // Record encryption event
            notifyListeners(id, 0);

            // Construct with ALL fields including shardId and buckets
            return new EncryptedPoint(
                    id,                    // id
                    version,               // version
                    iv,                    // iv
                    ciphertext,            // ciphertext
                    version,               // keyVersion
                    dim,                   // dimension
                    0,                     // shardId (default to 0)
                    java.util.List.of(),   // buckets (empty list)
                    java.util.List.of()    // metadata (empty list)
            );

        } catch (Exception e) {
            throw new RuntimeException("Encryption failed for id=" + id, e);
        }
    }


    /**
     * FIXED: Decrypt using the CORRECT key version
     *
     * Before: Used the key parameter passed in (wrong if key rotated)
     * After:  Uses point.getKeyVersion() to get the right key version
     *
     * This is CRITICAL for forward-security:
     * - Vector encrypted with key v1 must decrypt with key v1
     * - Even if current key is now v2
     * - Key v1 is retrieved from keyService based on version in point
     */
    @Override
    public double[] decryptFromPoint(EncryptedPoint encrypted, javax.crypto.SecretKey unusedKey) {
        if (encrypted == null) {
            throw new IllegalArgumentException("encrypted cannot be null");
        }

        try {
            String id = encrypted.getId();
            int version = encrypted.getVersion();
            int keyVersion = encrypted.getKeyVersion();  // ← CRITICAL: Use point's key version
            int dim = encrypted.getDimension();
            byte[] iv = encrypted.getIv();
            byte[] ciphertext = encrypted.getCiphertext();
            byte[] aad = encrypted.getAAD();  // ← Use AAD from point

            // ===== CRITICAL FIX #1: Get the CORRECT key version =====
            SecretKey key;
            try {
                // Try to get the specific key version used for encryption
                KeyVersion kv = keyService.getVersion(keyVersion);
                key = kv.getKey();
            } catch (Exception e) {
                log.warn("Key version {} not available for point {}, trying current key",
                        keyVersion, id);
                // Fallback to current key (may fail if key was rotated)
                KeyVersion currentKv = keyService.getCurrentVersion();
                key = currentKv.getKey();
            }

            if (key == null) {
                throw new IllegalArgumentException(
                        "No key available for decryption of point " + id +
                                " (encrypted with key v" + keyVersion + ")"
                );
            }

            // ===== CRITICAL FIX #2: Decrypt with CORRECT key =====
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(128, iv);  // 128-bit tag
            cipher.init(Cipher.DECRYPT_MODE, key, spec);

            // Update AAD if available
            if (aad != null && aad.length > 0) {
                cipher.updateAAD(aad);
            }

            // Decrypt
            byte[] plainBytes = cipher.doFinal(ciphertext);

            // Deserialize
            return deserializeVector(plainBytes);

        } catch (javax.crypto.AEADBadTagException e) {
            log.error("AEADBadTagException for point {}: Authentication tag mismatch. " +
                            "This likely means the wrong key was used for decryption. " +
                            "Expected key v{}, actual: {}",
                    encrypted.getId(), encrypted.getKeyVersion(), e.getMessage());
            throw new RuntimeException(
                    "Decryption failed: Tag mismatch for point " + encrypted.getId() +
                            " (key v" + encrypted.getKeyVersion() + ")",
                    e
            );
        } catch (Exception e) {
            log.error("Decryption failed for point {}", encrypted.getId(), e);
            throw new RuntimeException("Decryption failed for point " + encrypted.getId(), e);
        }
    }

    @Override
    public byte[] encryptQuery(double[] queryVector, SecretKey key, byte[] iv) {
        if (queryVector == null || key == null || iv == null) {
            throw new IllegalArgumentException("queryVector, key, and iv cannot be null");
        }

        try {
            byte[] plainBytes = serializeVector(queryVector);

            // No AAD for ephemeral queries
            Cipher cipher = Cipher.getInstance(ALGO);
            GCMParameterSpec spec = new GCMParameterSpec(TAG_BITS, iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);
            return cipher.doFinal(plainBytes);

        } catch (Exception e) {
            throw new RuntimeException("Query encryption failed", e);
        }
    }

    @Override
    public double[] decryptQuery(byte[] encryptedQuery, byte[] iv, SecretKey key) {
        if (encryptedQuery == null || iv == null || key == null) {
            throw new IllegalArgumentException("encryptedQuery, iv, and key cannot be null");
        }

        try {
            Cipher cipher = Cipher.getInstance(ALGO);
            GCMParameterSpec spec = new GCMParameterSpec(TAG_BITS, iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
            byte[] plainBytes = cipher.doFinal(encryptedQuery);
            return deserializeVector(plainBytes);

        } catch (Exception e) {
            throw new RuntimeException("Query decryption failed", e);
        }
    }

    @Override
    public KeyLifeCycleService getKeyService() {
        return keyService;
    }

    @Override
    public void setKeyService(KeyLifeCycleService keyService) {
        this.keyService = keyService;
    }

    @Override
    public KeyVersion getCurrentKeyVersion() {
        return (keyService != null) ? keyService.getCurrentVersion() : null;
    }

    @Override
    public void addEncryptionListener(CryptoService.EncryptionListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    // ========== HELPERS ==========

    private void notifyListeners(String vectorId, long encryptionTimeMs) {
        for (CryptoService.EncryptionListener listener : listeners) {
            try {
                listener.onEncryption(vectorId, encryptionTimeMs);
            } catch (Exception e) {
                log.warn("Listener notification failed", e);
            }
        }
    }

    private byte[] serializeVector(double[] v) {
        byte[] bytes = new byte[v.length * 8];
        for (int i = 0; i < v.length; i++) {
            long bits = Double.doubleToLongBits(v[i]);
            int offset = i * 8;
            bytes[offset] = (byte) (bits >> 56);
            bytes[offset + 1] = (byte) (bits >> 48);
            bytes[offset + 2] = (byte) (bits >> 40);
            bytes[offset + 3] = (byte) (bits >> 32);
            bytes[offset + 4] = (byte) (bits >> 24);
            bytes[offset + 5] = (byte) (bits >> 16);
            bytes[offset + 6] = (byte) (bits >> 8);
            bytes[offset + 7] = (byte) bits;
        }
        return bytes;
    }

    /**
     * Helper: Deserialize vector from bytes
     * (keep existing implementation)
     */
    private double[] deserializeVector(byte[] bytes) {
        double[] v = new double[bytes.length / 8];
        for (int i = 0; i < v.length; i++) {
            int offset = i * 8;
            long bits = 0L;
            bits |= ((long) (bytes[offset] & 0xFF)) << 56;
            bits |= ((long) (bytes[offset + 1] & 0xFF)) << 48;
            bits |= ((long) (bytes[offset + 2] & 0xFF)) << 40;
            bits |= ((long) (bytes[offset + 3] & 0xFF)) << 32;
            bits |= ((long) (bytes[offset + 4] & 0xFF)) << 24;
            bits |= ((long) (bytes[offset + 5] & 0xFF)) << 16;
            bits |= ((long) (bytes[offset + 6] & 0xFF)) << 8;
            bits |= ((long) (bytes[offset + 7] & 0xFF));
            v[i] = Double.longBitsToDouble(bits);
        }
        return v;
    }
}