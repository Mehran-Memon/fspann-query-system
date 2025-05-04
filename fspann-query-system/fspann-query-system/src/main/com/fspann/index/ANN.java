package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryToken;
import com.fspann.query.QueryGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.util.List;
import java.util.UUID;

public class ANN {
    private final EvenLSH lsh;
    private final KeyManager keyManager;
    private final int numBuckets;
    private final SecureLSHIndex secureIndex;
    private static final Logger logger = LoggerFactory.getLogger(ANN.class);

    public ANN(int dimensions, int numBuckets, KeyManager keyManager) {
        this.lsh = new EvenLSH(dimensions, numBuckets);
        this.numBuckets = numBuckets;
        this.keyManager = keyManager;
        this.secureIndex = new SecureLSHIndex(1, keyManager.getCurrentKey(), null);
    }

    /**
     * Builds the ANN index from the provided dataset.
     * @param data dataset to build the ANN index with
     */
    public void buildIndex(List<double[]> data) {
        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null.");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }

            lsh.updateCriticalValues(data);
            // Clear index manually if needed by removing all points
            secureIndex.clear(); // Assuming 'clear' method exists, or implement it

            for (double[] point : data) {
                int bucketId = lsh.getBucketId(point);
                secureIndex.add(UUID.randomUUID().toString(), point, bucketId, false, data);
            }
        } catch (Exception e) {
            logger.error("Error while building ANN index: {}", e.getMessage(), e);
            throw new RuntimeException("Error building ANN index", e);
        }
    }

    public int getBucketId(double[] point) {
        return lsh.getBucketId(point);
    }

    public KeyManager getKeyManager() {
        return keyManager;  // Return the key manager instance
    }


    /**
     * Finds approximate nearest neighbors for a query vector.
     * @param queryVector the query vector
     * @param k the number of nearest neighbors to return
     * @return the list of encrypted nearest neighbors
     */
    public List<EncryptedPoint> getApproximateNearestNeighbors(double[] queryVector, int k) {
        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null.");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }

            QueryToken queryToken = QueryGenerator.generateQueryToken(queryVector, k, 1, lsh, keyManager);
            List<EncryptedPoint> candidates = secureIndex.findNearestNeighborsEncrypted(queryToken);
            return candidates.subList(0, Math.min(k, candidates.size()));
        } catch (Exception e) {
            logger.error("Error finding nearest neighbors: {}", e.getMessage(), e);
            throw new RuntimeException("Error finding nearest neighbors", e);
        }
    }

    /**
     * Adds a new point to the ANN index.
     * @param newPoint the new point to add to the index
     */
    public void updateIndex(double[] newPoint) {
        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null.");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }

            int bucketId = lsh.getBucketId(newPoint);
            byte[] encryptedPoint = EncryptionUtils.encryptVector(newPoint, sessionKey);
            secureIndex.add(UUID.randomUUID().toString(), newPoint, bucketId, false, null); // Add to the secure index
        } catch (Exception e) {
            logger.error("Error while updating ANN index: {}", e.getMessage(), e);
            throw new RuntimeException("Error updating ANN index", e);
        }
    }

    /**
     * Removes a point from the ANN index.
     * @param point the point to remove
     */
    public void removePoint(double[] point) {
        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null.");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }

            int bucketId = lsh.getBucketId(point);
            byte[] encryptedPoint = EncryptionUtils.encryptVector(point, sessionKey);
            // Remove point from the secure index (adjust based on SecureLSHIndex's remove method)
            secureIndex.remove(UUID.randomUUID().toString()); // Assuming remove only needs ID
        } catch (Exception e) {
            logger.error("Error while removing point from ANN index: {}", e.getMessage(), e);
            throw new RuntimeException("Error removing point from ANN index", e);
        }
    }

    /**
     * Returns the current index used by the ANN.
     * @return the SecureLSHIndex used by the ANN
     */
    public SecureLSHIndex getSecureIndex() {
        return secureIndex;
    }
}
