package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.Objects;

/**
 * Represents an encrypted data point with associated metadata for ANN queries.
 * Supports forward security and dynamic updates.
 */
public class EncryptedPoint {

    private byte[] ciphertext;  // The encrypted coordinate data
    private String bucketId;    // ID or label for the bucket
    private String pointId;     // Unique identifier for the point

    /**
     * Constructor for EncryptedPoint.
     * @param ciphertext Encrypted coordinate data.
     * @param bucketId ID of the bucket containing this point.
     * @param pointId Unique identifier for the point.
     */
    public EncryptedPoint(byte[] ciphertext, String bucketId, String pointId) {
        this.ciphertext = ciphertext != null ? ciphertext.clone() : new byte[0]; // Defensive copy
        this.bucketId = Objects.requireNonNull(bucketId, "bucketId cannot be null");
        this.pointId = Objects.requireNonNull(pointId, "pointId cannot be null");
    }

    /**
     * Gets the encrypted ciphertext.
     * @return The encrypted byte array.
     */
    public byte[] getCiphertext() {
        return ciphertext.clone(); // Return a copy to prevent modification
    }

    /**
     * Gets the bucket ID.
     * @return The bucket ID.
     */
    public String getBucketId() {
        return bucketId;
    }

    /**
     * Gets the point ID.
     * @return The unique point ID.
     */
    public String getPointId() {
        return pointId;
    }

    /**
     * Updates the bucket ID when the point is re-bucketed.
     * @param newBucketId The new bucket ID.
     */
    public void setBucketId(String newBucketId) {
        this.bucketId = Objects.requireNonNull(newBucketId, "newBucketId cannot be null");
    }

    /**
     * Re-encrypts the point with a new key for forward security.
     * @param keyManager KeyManager instance to get old and new keys.
     * @param context Context for the key (e.g., "epoch_<number>").
     * @throws Exception If encryption fails.
     */
    public void reEncrypt(KeyManager keyManager, String context) throws Exception {
        SecretKey oldKey = keyManager.getSessionKey(context);
        SecretKey newKey = keyManager.getCurrentKey();
        if (oldKey != null) {
            double[] decryptedVector = EncryptionUtils.decryptVector(ciphertext, oldKey);
            this.ciphertext = EncryptionUtils.encryptVector(decryptedVector, newKey);
        }
    }

    /**
     * Decrypts the point for distance computation (to be used by QueryProcessor).
     * @param key SecretKey for decryption.
     * @return The decrypted double[] vector.
     * @throws Exception If decryption fails.
     */
    public double[] decrypt(SecretKey key) throws Exception {
        return EncryptionUtils.decryptVector(ciphertext, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncryptedPoint that = (EncryptedPoint) o;
        return Arrays.equals(ciphertext, that.ciphertext) &&
                bucketId.equals(that.bucketId) &&
                pointId.equals(that.pointId);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(ciphertext);
        result = 31 * result + bucketId.hashCode();
        result = 31 * result + pointId.hashCode();
        return result;
    }
}