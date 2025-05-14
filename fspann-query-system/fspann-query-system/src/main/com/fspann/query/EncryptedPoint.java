package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;

import org.apache.commons.lang3.tuple.Pair;
import javax.crypto.AEADBadTagException;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedPoint implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPoint.class);

    private byte[] ciphertext;  // Store the encrypted data
    private String bucketId;
    private final String pointId;
    private final int index;
    private double[] vector;  // Store the original vector
    private int vectorIndex;
    private byte[] iv; // Initialization vector for GCM
    private final String id;

    // Constructor with ciphertext, bucketId, pointId, and index
    public EncryptedPoint(byte[] ciphertext, String bucketId, String pointId, int index, byte[] iv, String id) {
        if (iv.length != 12) {
            throw new IllegalArgumentException("IV must be exactly 12 bytes");
        }
        if (ciphertext == null || ciphertext.length == 0) {
            throw new IllegalArgumentException("Ciphertext cannot be null or empty");
        }
        this.ciphertext = ciphertext.clone();
        this.bucketId = Objects.requireNonNull(bucketId, "bucketId cannot be null");
        this.pointId = Objects.requireNonNull(pointId, "pointId cannot be null");
        this.index = index;
        this.iv = iv.clone();
        this.id = Objects.requireNonNull(id, "id cannot be null");
    }


    public String getId() {
        return id;
    }

    // Getter for index
    public int getIndex() {
        return index;
    }

    // Getter for bucketId
    public String getBucketId() {
        return bucketId;
    }

    // Getter for pointId
    public String getPointId() {
        return pointId;
    }

    // Getter for vector
    public double[] getVector() {
        return vector;
    }

    // Setter for the vector
    public void setVector(double[] vector) {
        this.vector = vector;
    }

    // Getter for vectorIndex
    public int getVectorIndex() {
        return vectorIndex;
    }

    // Re-encrypt the point's ciphertext with a new key
    public void reEncrypt(KeyManager keyManager, List<SecretKey> previousKeys) throws Exception {
        Objects.requireNonNull(keyManager, "KeyManager cannot be null");
        if (previousKeys == null) {
            throw new IllegalArgumentException("Previous keys list cannot be null");
        }

        SecretKey newKey = keyManager.getCurrentKey();
        if (newKey == null) {
            throw new IllegalStateException("Current key cannot be null for re-encryption");
        }

        // Try to decrypt with current key or previous keys
        Pair<double[], SecretKey> decryptionResult = tryDecryptWithKeys(keyManager.getCurrentKey(), previousKeys);
        if (decryptionResult == null) {
            throw new SecurityException("Failed to decrypt point " + id + " for re-encryption: no valid key found");
        }

        // Re-encrypt with new key
        Pair<byte[], byte[]> result = EncryptionUtils.reEncryptData(
                ciphertext,
                iv,
                decryptionResult.getRight(),
                newKey
        );
        updateEncryptedData(result.getLeft(), result.getRight());
    }

    private Pair<double[], SecretKey> tryDecryptWithKeys(
            SecretKey currentKey,
            List<SecretKey> previousKeys) {

        // First try with the current key
        try {
            double[] vector = EncryptionUtils.decryptVector(ciphertext, iv, currentKey);
            return Pair.of(vector, currentKey);
        } catch (AEADBadTagException e) {
            logger.debug("Current key failed for point {}: Tag mismatch", id);
        } catch (Exception e) {
            // catch any other possible errors (IO, padding, etc.)
            logger.error("Unexpected error decrypting point {} with current key: {}", id, e.getMessage(), e);
        }

        // Then try each previous key in order
        for (SecretKey key : previousKeys) {
            try {
                double[] vector = EncryptionUtils.decryptVector(ciphertext, iv, key);
                return Pair.of(vector, key);
            } catch (AEADBadTagException e) {
                logger.debug("Previous key {} failed for point {}: Tag mismatch", key.hashCode(), id);
            } catch (Exception e) {
                logger.error("Unexpected error decrypting point {} with previous key {}: {}", id, key.hashCode(), e.getMessage(), e);
            }
        }

        return null;
    }

    private void updateEncryptedData(byte[] newCiphertext, byte[] newIv) {
        this.ciphertext = newCiphertext.clone();
        this.iv = newIv.clone();
        logger.info("Successfully re-encrypted point {}", id);
    }

    // Decrypt the point's ciphertext using the provided key
    public double[] decrypt(SecretKey currentKey, List<SecretKey> previousKeys) throws Exception {
        Objects.requireNonNull(currentKey, "Current key cannot be null");
        if (previousKeys == null) {
            throw new IllegalArgumentException("Previous keys list cannot be null");
        }

        if (!isValidForDecryption()) {
            logger.warn("Invalid ciphertext or IV for point {}. Cannot decrypt.", id);
            return null;
        }

        // Try current key first
        try {
            return EncryptionUtils.decryptVector(ciphertext, iv, currentKey);
        } catch (AEADBadTagException e) {
            logger.debug("Decryption failed for point {} with current key", id);
        }

        // Try previous keys
        for (SecretKey key : previousKeys) {
            try {
                return EncryptionUtils.decryptVector(ciphertext, iv, key);
            } catch (AEADBadTagException e) {
                logger.debug("Decryption failed for point {} with previous key", id);
            }
        }

        logger.warn("No valid key available for point {}", id);
        return null;
    }

    private boolean isValidForDecryption() {
        return ciphertext != null && ciphertext.length > 0 &&
                iv != null && iv.length == EncryptionUtils.GCM_IV_LENGTH;
    }

    // Make sensitive operations thread-safe
    public synchronized void setCiphertext(byte[] newCiphertext) {
        this.ciphertext = newCiphertext != null ? newCiphertext.clone() : new byte[0];
    }

    public synchronized byte[] getCiphertext() {
        return ciphertext.clone();
    }

    public synchronized byte[] getIV() {
        return iv != null ? iv.clone() : null;
    }

    // Add methods for better security management
    public boolean isEncryptedWith(SecretKey key) {
        try {
            EncryptionUtils.decryptVector(ciphertext, iv, key);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean needsReencryption(SecretKey currentKey) {
        return !isEncryptedWith(currentKey);
    }


    // Override equals
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncryptedPoint that = (EncryptedPoint) o;
        return Arrays.equals(ciphertext, that.ciphertext) &&
                bucketId.equals(that.bucketId) &&
                pointId.equals(that.pointId) &&
                index == that.index;
    }

    // Override hashCode
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(ciphertext);
        result = 31 * result + bucketId.hashCode();
        result = 31 * result + pointId.hashCode();
        result = 31 * result + index;
        return result;
    }

    // Add writeObject/readObject for secure serialization
    @Serial
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    @Serial
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Defensive copying
        if (this.ciphertext != null) {
            this.ciphertext = this.ciphertext.clone();
        }
        if (this.iv != null) {
            this.iv = this.iv.clone();
        }
        // Validate after deserialization
        if (this.iv == null || this.iv.length != EncryptionUtils.GCM_IV_LENGTH) {
            throw new InvalidObjectException("Invalid IV length after deserialization");
        }
    }
}