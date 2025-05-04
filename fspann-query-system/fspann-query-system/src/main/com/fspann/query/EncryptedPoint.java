package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedPoint implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPoint.class);
    private byte[] ciphertext;
    private String bucketId;
    private final String pointId;
    private final int index;

    // Constructor with ciphertext, bucketId, pointId, and index
    public EncryptedPoint(byte[] ciphertext, String bucketId, String pointId, int index) {
        this.ciphertext = ciphertext != null ? ciphertext.clone() : new byte[0];
        this.bucketId = Objects.requireNonNull(bucketId, "bucketId cannot be null");
        this.pointId = Objects.requireNonNull(pointId, "pointId cannot be null");
        this.index = index;
    }

    // Constructor with just ciphertext, setting default values
    public EncryptedPoint(byte[] ciphertext) {
        this(ciphertext, "unknown", "unknown", -1); // Default index to -1
    }

    // Getter for index
    public int getIndex() {
        return index;
    }

    // Getter for ciphertext
    public byte[] getCiphertext() {
        return ciphertext.clone();
    }

    // Getter for bucketId
    public String getBucketId() {
        return bucketId;
    }

    // Getter for pointId
    public String getPointId() {
        return pointId;
    }

    // Setter for ciphertext
    public void setCiphertext(byte[] newCiphertext) {
        this.ciphertext = newCiphertext != null ? newCiphertext.clone() : new byte[0];
    }

    // Setter for bucketId
    public void setBucketId(String newBucketId) {
        this.bucketId = Objects.requireNonNull(newBucketId, "newBucketId cannot be null");
    }

    // Re-encrypt the point's ciphertext with a new key
    public void reEncrypt(KeyManager keyManager, String context) throws Exception {
        logger.info("Re-encrypting EncryptedPoint with new key for context: {}", context);

        SecretKey oldKey = keyManager.getSessionKey(context);
        SecretKey newKey = keyManager.getCurrentKey();

        if (oldKey == null || newKey == null) {
            logger.error("Failed to retrieve valid keys for re-encryption: oldKey={}, newKey={}", oldKey, newKey);
            throw new IllegalStateException("Failed to retrieve valid keys for re-encryption.");
        }

        double[] decryptedVector = EncryptionUtils.decryptVector(ciphertext, oldKey);
        this.ciphertext = EncryptionUtils.encryptVector(decryptedVector, newKey);
    }

    // Decrypt the point's ciphertext using the provided key
    public double[] decrypt(SecretKey key) throws Exception {
        return EncryptionUtils.decryptVector(ciphertext, key);
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
}