package com.fspann.query;

import com.fspann.config.SystemConfig;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.utils.Profiler;
import javax.crypto.SecretKey;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fspann.utils.LRUCache;

public class QueryProcessor {
    private final Map<Integer, List<EncryptedPoint>> bucketIndex;
    private final KeyManager keyManager;
    private final LRUCache<String, List<EncryptedPoint>> cache;
    private final Profiler profiler;
    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    // Constructor to initialize the KeyManager and cache
    public QueryProcessor(Map<Integer, List<EncryptedPoint>> bucketIndex, KeyManager keyManager, int cacheCapacity) {
        this.bucketIndex = bucketIndex;
        this.keyManager = keyManager;
        this.cache = new LRUCache<>(cacheCapacity); // Set cache capacity
        this.profiler = new Profiler();

    }

    public String generateCacheKey(QueryToken queryToken) {
        // Using encryption context, candidate buckets, and encrypted query to create a unique cache key
        StringBuilder sb = new StringBuilder();
        sb.append(queryToken.getEncryptionContext());  // Adding encryption context
        sb.append(queryToken.getTopK());  // Adding topK
        sb.append(queryToken.getNumTables());  // Adding numTables
        sb.append(Arrays.toString(queryToken.getCandidateBuckets().toArray()));  // Adding candidateBuckets
        sb.append(Arrays.toString(queryToken.getEncryptedQuery()));  // Adding encryptedQuery

        // Return a string representation as cache key
        return sb.toString();
    }

    /**
     * Process the query by decrypting old data and re-encrypting with the current key.
     *
     * @param candidates The list of candidate encrypted points.
     * @return The list of encrypted points after processing.
     * @throws Exception if there is an error during decryption or encryption.
     */
    public List<EncryptedPoint> processQuery(QueryToken queryToken, List<EncryptedPoint> candidates) throws Exception {
        byte[] encryptedQuery = queryToken.getEncryptedQuery();

        if (SystemConfig.PROFILER_ENABLED) profiler.start("query");

        if (candidates == null || candidates.isEmpty()) {
            logger.warn("No candidates found for the query.");
            return new ArrayList<>();
        }

        List<EncryptedPoint> result = new ArrayList<>();
        SecretKey currentKey = keyManager.getCurrentKey();
        if (currentKey == null) {
            throw new IllegalStateException("No current session key available");
        }

        for (EncryptedPoint point : candidates) {
            SecretKey previousKey = keyManager.getPreviousKey();
            if (previousKey != null) {
                // Decryption under the old key
                if (SystemConfig.PROFILER_ENABLED) profiler.start("decryption");
                double[] decrypted = point.decrypt(previousKey, Collections.emptyList());
                if (SystemConfig.PROFILER_ENABLED) profiler.stop("decryption");

                // Re‑encrypt under the current key with a fresh IV
                if (SystemConfig.PROFILER_ENABLED) profiler.start("encryption");
                byte[] ivRe = EncryptionUtils.generateIV();
                byte[] reEncrypted = EncryptionUtils.encryptVector(decrypted, ivRe, currentKey);
                EncryptedPoint newEncryptedPoint = new EncryptedPoint(
                        reEncrypted,
                        point.getBucketId(),
                        point.getPointId(),
                        point.getIndex(),
                        ivRe,
                        point.getPointId()
                );
                result.add(newEncryptedPoint);
                if (SystemConfig.PROFILER_ENABLED) profiler.stop("encryption");
            } else {
                logger.warn("No previous key available for point {}. Skipping re‑encryption.", point.getPointId());
                result.add(point);
            }
        }

        if (SystemConfig.PROFILER_ENABLED) profiler.stop("query");

        String cacheKey = generateCacheKey(queryToken);
        cache.put(cacheKey, result);
        return result;
    }
    /**
     * Generates a cache key based on the query candidates.
     * @param candidates The query candidates.
     * @return A string key for caching.
     */
    private String generateCacheKey(List<EncryptedPoint> candidates) {
        StringBuilder key = new StringBuilder();
        for (EncryptedPoint point : candidates) {
            key.append(point.getPointId()).append(",");
        }
        return key.toString();
    }

    /**
     * Updates the bucket index with new points for a specific bucket ID.
     *
     * @param bucketId The bucket ID.
     * @param points The list of points to add to the bucket.
     */
    public void updateBucketIndex(int bucketId, List<EncryptedPoint> points) {
        // Validate input before updating
        if (points == null || points.isEmpty()) {
            throw new IllegalArgumentException("Points cannot be null or empty");
        }

        bucketIndex.put(bucketId, new ArrayList<>(points));
    }

    /**
     * Remove a point by its ID from the bucket index.
     *
     * @param pointId The point ID to remove.
     */
    public void removeFromBucketIndex(String pointId) {
        // Iterate through the buckets and remove the point by its ID
        bucketIndex.values().forEach(bucket -> bucket.removeIf(p -> p.getPointId().equals(pointId)));
    }
}
