package com.fspann.crypto;

import com.fspann.common.*;
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
    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;     // 16 bytes
    private static final int KEY_BITS = 256;
    private final MeterRegistry metrics;
    private KeyLifeCycleService keyService;
    private final MetadataManager metadataManager;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    // Encryption listeners (optional)
    private final List<CryptoService.EncryptionListener> listeners =
            new CopyOnWriteArrayList<>();

    public AesGcmCryptoService(
            MeterRegistry metrics,
            KeyLifeCycleService keyService,
            MetadataManager metadataManager
    ) {
        this.metrics = metrics;
        this.keyService = keyService;
        this.metadataManager = metadataManager;
        log.info("AesGcmCryptoService initialized with AAD binding");
    }

    @Override
    public EncryptedPoint encryptToPoint(String id, double[] plaintext, SecretKey key) {
        try {
            KeyVersion kv = (keyService != null) ? keyService.getCurrentVersion() : new KeyVersion(1, key);
            int version = kv.getVersion();
            int dim = plaintext.length;

            // PERFORMANCE FIX: Use shared static random
            byte[] iv = new byte[IV_BYTES];
            SECURE_RANDOM.nextBytes(iv);

            // AAD Binding: Ensures ciphertext integrity vs metadata context
            String aadStr = String.format("id:%s|v:%d|d:%d", id, version, dim);
            byte[] aad = aadStr.getBytes();

            byte[] plainBytes = serializeVector(plaintext);

            Cipher cipher = Cipher.getInstance(ALGO);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(aad);
            byte[] ciphertext = cipher.doFinal(plainBytes);

            if (keyService != null) {
                keyService.trackEncryption(id, version);
            }

            return new EncryptedPoint(id, version, iv, ciphertext, version, dim, 0, List.of(), List.of());
        } catch (Exception e) {
            throw new RuntimeException("Encryption failed for id=" + id, e);
        }
    }

    @Override
    public double[] decryptFromPoint(EncryptedPoint encrypted, SecretKey providedKey) {
        try {
            // Game-Based FS Rule: Use specific key if provided by adversary/test, else resolve from service
            final SecretKey keyToUse = (providedKey != null) ? providedKey : keyService.getVersion(encrypted.getKeyVersion()).getKey();

            Cipher cipher = Cipher.getInstance(ALGO);
            cipher.init(Cipher.DECRYPT_MODE, keyToUse, new GCMParameterSpec(TAG_BITS, encrypted.getIv()));

            byte[] aad = encrypted.getAAD();
            if (aad != null) cipher.updateAAD(aad);

            byte[] plain = cipher.doFinal(encrypted.getCiphertext());
            return deserializeVector(plain);
        } catch (Exception e) {
            throw new RuntimeException("Decryption integrity failure for point " + encrypted.getId(), e);
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

    // High-performance binary serialization
    private byte[] serializeVector(double[] v) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(v.length * 8);
        for (double d : v) buffer.putDouble(d);
        return buffer.array();
    }

    private double[] deserializeVector(byte[] bytes) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
        double[] v = new double[bytes.length / 8];
        for (int i = 0; i < v.length; i++) v[i] = buffer.getDouble();
        return v;
    }
}