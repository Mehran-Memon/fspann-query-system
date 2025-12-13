package com.fspann.crypto;

import com.fspann.common.KeyVersion;
import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;

/**
 * CryptoService: AES-GCM encryption with AAD binding.
 * Supports forward-security via multi-version key management.
 */
public interface CryptoService {

    /**
     * Encrypt a plaintext vector to an EncryptedPoint.
     */
    EncryptedPoint encryptToPoint(String id, double[] plaintext, javax.crypto.SecretKey key);

    /**
     * Convenience: Encrypt using current key version.
     * This is an alias for encryptToPoint() with getCurrentKeyVersion().getKey()
     */
    default EncryptedPoint encrypt(String id, double[] plaintext) {
        KeyVersion kv = getCurrentKeyVersion();
        if (kv == null) {
            throw new IllegalStateException("Current key version not available");
        }
        return encryptToPoint(id, plaintext, kv.getKey());
    }

    /**
     * Convenience: Encrypt using explicit key version.
     */
    default EncryptedPoint encrypt(String id, double[] plaintext, KeyVersion keyVersion) {
        if (keyVersion == null) {
            throw new IllegalArgumentException("keyVersion cannot be null");
        }
        return encryptToPoint(id, plaintext, keyVersion.getKey());
    }

    /**
     * Decrypt an EncryptedPoint back to plaintext double[].
     */
    double[] decryptFromPoint(EncryptedPoint encrypted, javax.crypto.SecretKey key);

    /**
     * Encrypt a query vector for encrypted search.
     */
    byte[] encryptQuery(double[] queryVector, javax.crypto.SecretKey key, byte[] iv);

    /**
     * Decrypt a query vector.
     */
    double[] decryptQuery(byte[] encryptedQuery, byte[] iv, javax.crypto.SecretKey key);

    /**
     * Get the key service for accessing current/historical keys.
     */
    KeyLifeCycleService getKeyService();

    /**
     * Set the key service (optional, for late initialization).
     */
    void setKeyService(KeyLifeCycleService keyService);

    /**
     * Get current key version.
     */
    KeyVersion getCurrentKeyVersion();

    /**
     * Listener for encryption events (optional).
     */
    @FunctionalInterface
    interface EncryptionListener {
        void onEncryption(String vectorId, long encryptionTimeMs);
    }

    /**
     * Register an encryption listener (optional).
     */
    void addEncryptionListener(EncryptionListener listener);
}