package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryToken;
import com.fspann.query.QueryGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ANN {
    private final EvenLSH lsh;
    private final KeyManager keyManager;
    private final int numBuckets;
    private final SecureLSHIndex secureIndex;
    private static final Logger logger = LoggerFactory.getLogger(ANN.class);
    private List<double[]> dataRef;
    private final List<double[]> baseVectors;

    /**
     * @param dimensions   dimensionality of vectors
     * @param numBuckets   number of hash buckets (also used as shard count here)
     * @param keyManager   key manager for encryption
     * @param baseVectors  backing list for vector storage
     */
    public ANN(int dimensions,
               int numBuckets,
               KeyManager keyManager,
               List<double[]> baseVectors) {
        this.lsh = new EvenLSH(dimensions, numBuckets);
        this.numBuckets = numBuckets;
        this.keyManager = keyManager;
        this.baseVectors = baseVectors;

        // Build a SecureLSHIndex with 1 hash table, numBuckets shards,
        // using the current session key, and seeded with our baseVectors
        this.secureIndex = new SecureLSHIndex(
                1,                      // one hash table
                numBuckets,             // shards = bucket count
                keyManager.getCurrentKey(),
                baseVectors             // initial data to index
        );
    }

    /**
     * Builds the ANN index from the provided dataset.
     * @param data dataset to build the ANN index with
     */
    public void buildIndex(List<double[]> data) {
        if (data == null || data.isEmpty()) {
            logger.warn("Cannot build ANN index: dataset is null or empty");
            return; // Or throw a specific exception if required
        }

        try {
            SecretKey sessionKey = keyManager.getCurrentKey();
            if (sessionKey == null) {
                logger.error("Encryption key cannot be null");
                throw new IllegalArgumentException("Encryption key cannot be null");
            }

            logger.debug("Building ANN index with {} vectors", data.size());
            lsh.updateCriticalValues(data);
            secureIndex.clear(); // Clear index after validation

            for (double[] point : data) {
                if (point == null || point.length != 128) { // Add dimension check
                    logger.warn("Skipping invalid vector: {}", Arrays.toString(point));
                    continue;
                }
                int bucketId = lsh.getBucketId(point);
                secureIndex.add(UUID.randomUUID().toString(), point, bucketId, false, data);
            }
            this.dataRef = data; // Store dataRef after successful processing
            logger.info("ANN index built successfully with {} vectors", data.size());
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
                throw new IllegalStateException("Encryption key cannot be null");
            }

            // Make sure the vector list is ready and contains the point
            if (baseVectors != null && !baseVectors.contains(newPoint)) {
                baseVectors.add(newPoint);
            }

            int bucketId = lsh.getBucketId(newPoint);

        /* Insert once ― pass the real baseVectors so SecureLSHIndex
           can resolve the index inside the vector list                */
            secureIndex.add(
                    UUID.randomUUID().toString(), // point ID
                    newPoint,                     // raw vector
                    bucketId,                     // bucket
                    /* useFakePoints = */ false,
                    baseVectors);                 // reference list

        } catch (Exception e) {
            logger.error("Error while updating ANN index", e);
            throw new RuntimeException("Error updating ANN index", e);
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
