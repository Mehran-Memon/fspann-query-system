package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.Objects;
import java.util.Arrays;

public class EncryptedPoint {
    private byte[] ciphertext;
    private String bucketId;
    private String pointId;

    public EncryptedPoint(byte[] ciphertext, String bucketId, String pointId) {
        this.ciphertext = ciphertext != null ? ciphertext.clone() : new byte[0];
        this.bucketId = Objects.requireNonNull(bucketId, "bucketId cannot be null");
        this.pointId = Objects.requireNonNull(pointId, "pointId cannot be null");
    }

    public EncryptedPoint(byte[] ciphertext) {
        this(ciphertext, "unknown", "unknown");
    }

    public byte[] getCiphertext() {
        return ciphertext.clone();
    }

    public String getBucketId() {
        return bucketId;
    }

    public String getPointId() {
        return pointId;
    }

    public void setBucketId(String newBucketId) {
        this.bucketId = Objects.requireNonNull(newBucketId, "newBucketId cannot be null");
    }

    public void reEncrypt(KeyManager keyManager, String context) throws Exception {
        SecretKey oldKey = keyManager.getSessionKey(context);
        SecretKey newKey = keyManager.getCurrentKey();
        if (oldKey != null) {
            double[] decryptedVector = EncryptionUtils.decryptVector(ciphertext, oldKey);
            this.ciphertext = EncryptionUtils.encryptVector(decryptedVector, newKey);
        }
    }

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