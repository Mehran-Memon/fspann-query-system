package java.com.fspann.query;

import java.com.fspann.encryption.EncryptionUtils;
import java.com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.Objects;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedPoint {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPoint.class); // Initialize the logger
    private byte[] ciphertext;
    private String bucketId;
    private String pointId;

    // Constructor with ciphertext, bucketId, and pointId
    public EncryptedPoint(byte[] ciphertext, String bucketId, String pointId) {
        this.ciphertext = ciphertext != null ? ciphertext.clone() : new byte[0];
        this.bucketId = Objects.requireNonNull(bucketId, "bucketId cannot be null");
        this.pointId = Objects.requireNonNull(pointId, "pointId cannot be null");
    }

    // Constructor with just ciphertext, setting default values for bucketId and pointId
    public EncryptedPoint(byte[] ciphertext) {
        this(ciphertext, "unknown", "unknown");
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

    // Setter for ciphertext (to allow modification after rehash)
    public void setCiphertext(byte[] newCiphertext) {
        this.ciphertext = newCiphertext != null ? newCiphertext.clone() : new byte[0];
    }

    // Setter for bucketId
    public void setBucketId(String newBucketId) {
        this.bucketId = Objects.requireNonNull(newBucketId, "newBucketId cannot be null");
    }

    // Re-encrypt the point's ciphertext with a new key
    public void reEncrypt(KeyManager keyManager, String context) throws Exception {
        // Log key rotation process
        logger.info("Re-encrypting EncryptedPoint with new key for context: {}", context);

        // Get old and new keys for re-encryption
        SecretKey oldKey = keyManager.getSessionKey(context);
        SecretKey newKey = keyManager.getCurrentKey();

        if (oldKey == null || newKey == null) {
            throw new IllegalStateException("Failed to retrieve valid keys for re-encryption.");
        }

        // Decrypt with the old key and re-encrypt with the new key
        double[] decryptedVector = EncryptionUtils.decryptVector(ciphertext, oldKey);
        this.ciphertext = EncryptionUtils.encryptVector(decryptedVector, newKey);
    }

    // Decrypt the point's ciphertext using the provided key
    public double[] decrypt(SecretKey key) throws Exception {
        return EncryptionUtils.decryptVector(ciphertext, key);
    }

    // Override equals to compare EncryptedPoint objects based on ciphertext, bucketId, and pointId
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncryptedPoint that = (EncryptedPoint) o;
        return Arrays.equals(ciphertext, that.ciphertext) &&
                bucketId.equals(that.bucketId) &&
                pointId.equals(that.pointId);
    }

    // Override hashCode to generate hash based on ciphertext, bucketId, and pointId
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(ciphertext);
        result = 31 * result + bucketId.hashCode();
        result = 31 * result + pointId.hashCode();
        return result;
    }
}
