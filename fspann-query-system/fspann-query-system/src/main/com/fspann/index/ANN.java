package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.QueryToken;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ANN {
    private EvenLSH lsh;
    private List<List<byte[]>> buckets;
    private KeyManager keyManager;
    private final int numBuckets;
    private SecureLSHIndex secureIndex;
    private static final Logger logger = LoggerFactory.getLogger(ANN.class);

    public ANN(int dimensions, int numBuckets, KeyManager keyManager) {
        this.lsh = new EvenLSH(dimensions, numBuckets);
        this.numBuckets = numBuckets;
        this.buckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(new ArrayList<>());
        }
        this.keyManager = keyManager;
        this.secureIndex = new SecureLSHIndex(1, keyManager.getCurrentKey(), null);
    }

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

    public int getBucketId(double[] point) {
        return lsh.getBucketId(point);
    }

    /**
     * Builds the ANN index from the provided dataset.
     * @param data The dataset to build the ANN index.
     */
    public void buildIndex(List<double[]> data) {
        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null.");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }
            lsh.updateCriticalValues(data);
            buckets.clear();
            for (int i = 0; i < numBuckets; i++) {
                buckets.add(new ArrayList<>());
            }
            for (double[] point : data) {
                secureIndex.add(UUID.randomUUID().toString(), point, false, data);
            }
        } catch (Exception e) {
            logger.error("Error while building ANN index: {}", e.getMessage(), e);
            throw new RuntimeException("Error building ANN index", e);
        }
    }

    public KeyManager getKeyManager() {
        return keyManager;
    }

    /**
     * Updates the ANN index by adding a new point.
     * @param newPoint The new point to add to the index.
     */
    public void updateIndex(double[] newPoint) {
        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null.");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }

            int bucketId = lsh.getBucketId(newPoint);  // Fixed typo: changed 'point' to 'newPoint'
            if (bucketId < 0 || bucketId >= numBuckets) {
                logger.warn("Invalid bucket ID: {}. Cannot add point.", bucketId);
                return;
            }
            byte[] encryptedPoint = EncryptionUtils.encryptVector(newPoint, sessionKey);
            buckets.get(bucketId).add(encryptedPoint);
        } catch (Exception e) {
            logger.error("Error while updating ANN index: {}", e.getMessage(), e);
            throw new RuntimeException("Error updating ANN index", e);
        }
    }

    /**
     * Removes a point from the ANN index.
     * @param point The point to remove.
     */
    public void removePoint(double[] point) {
        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null.");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }

            int bucketId = lsh.getBucketId(point);
            if (bucketId < 0 || bucketId >= numBuckets) {
                logger.warn("Invalid bucket ID: {}. Cannot remove point.", bucketId);
                return;
            }
            byte[] encryptedPoint = EncryptionUtils.encryptVector(point, sessionKey);
            buckets.get(bucketId).removeIf(ep -> java.util.Arrays.equals(ep, encryptedPoint));
        } catch (Exception e) {
            logger.error("Error while removing point from ANN index: {}", e.getMessage(), e);
            throw new RuntimeException("Error removing point from ANN index", e);
        }
    }

    /**
     * Calculates the Euclidean distance between two vectors.
     * @param pointA The first point.
     * @param pointB The second point.
     * @return The Euclidean distance between the two points.
     */
    private double calculateDistance(double[] pointA, double[] pointB) {
        double sum = 0.0;
        for (int i = 0; i < pointA.length; i++) {
            sum += Math.pow(pointA[i] - pointB[i], 2);
        }
        return Math.sqrt(sum);
    }

    /**
     * Returns the current list of buckets in the ANN index.
     * @return A list of buckets.
     */
    public List<List<byte[]>> getBuckets() {
        return buckets;
    }
}