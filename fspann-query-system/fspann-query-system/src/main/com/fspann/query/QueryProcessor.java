package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fspann.utils.LRUCache;

public class QueryProcessor {
    private final Map<Integer, List<EncryptedPoint>> bucketIndex;
    private final KeyManager keyManager;
    private final LRUCache<String, List<EncryptedPoint>> cache;

    // Constructor to initialize the KeyManager and cache
    public QueryProcessor(Map<Integer, List<EncryptedPoint>> bucketIndex, KeyManager keyManager, int cacheCapacity) {
        this.bucketIndex = bucketIndex;
        this.keyManager = keyManager;
        this.cache = new LRUCache<>(cacheCapacity); // Set cache capacity
    }

    /**
     * Process the query by decrypting old data and re-encrypting with the current key.
     *
     * @param candidates The list of candidate encrypted points.
     * @return The list of encrypted points after processing.
     * @throws Exception if there is an error during decryption or encryption.
     */
    public List<EncryptedPoint> processQuery(List<EncryptedPoint> candidates) throws Exception {
        if (candidates == null || candidates.isEmpty()) {
            return new ArrayList<>();
        }

        List<EncryptedPoint> result = new ArrayList<>();
        SecretKey currentKey = keyManager.getCurrentKey();
        if (currentKey == null) {
            throw new IllegalStateException("No current session key available");
        }

        // Process each candidate point
        for (EncryptedPoint point : candidates) {
            // If a previous key exists, decrypt the point with it and re-encrypt it with the current key
            SecretKey previousKey = keyManager.getPreviousKey();
            if (previousKey != null) {
                // Decrypt the point using the previous key
                double[] decrypted = point.decrypt(previousKey);

                // Re-encrypt with the current key
                byte[] reEncrypted = EncryptionUtils.encryptVector(decrypted, currentKey);

                // Create a new EncryptedPoint with the re-encrypted data
                point = new EncryptedPoint(reEncrypted, point.getBucketId(), point.getPointId());
            }
            // Add the re-encrypted (or original) point to the result list
            result.add(point);
        }

        // Cache the result before returning
        cache.put(generateCacheKey(candidates), result);

        return result;
    }

    /**
     * Generates a cache key based on the query candidates.
     * @param candidates The query candidates.
     * @return A string key for caching.
     */
    private String generateCacheKey(List<EncryptedPoint> candidates) {
        // Generate a unique key based on the candidate points' IDs or any other suitable method
        StringBuilder sb = new StringBuilder();
        for (EncryptedPoint point : candidates) {
            sb.append(point.getPointId());
        }
        return sb.toString();
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
