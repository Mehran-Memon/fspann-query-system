package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Processes server-side ANN queries by retrieving encrypted candidate points from buckets.
 * Supports forward security and dynamic updates.
 */
public class QueryProcessor {

    private final Map<Integer, List<EncryptedPoint>> bucketIndex;
    private final KeyManager keyManager;

    public QueryProcessor(Map<Integer, List<EncryptedPoint>> bucketIndex, KeyManager keyManager) {
        this.bucketIndex = new ConcurrentHashMap<>(Objects.requireNonNull(bucketIndex, "bucketIndex cannot be null"));
        this.keyManager = Objects.requireNonNull(keyManager, "keyManager cannot be null");
    }

    /**
     * Processes the incoming QueryToken and returns the encrypted candidate points.
     * @param token The QueryToken containing candidate buckets and encrypted query.
     * @return List of EncryptedPoint candidates.
     * @throws IllegalArgumentException If the token or buckets are invalid.
     * @throws Exception If re-encryption fails.
     */
    public List<EncryptedPoint> processQuery(QueryToken token) throws Exception {
        if (token == null) {
            throw new IllegalArgumentException("QueryToken cannot be null");
        }
        List<Integer> candidateBuckets = token.getCandidateBuckets();
        if (candidateBuckets == null || candidateBuckets.isEmpty()) {
            return new ArrayList<>(); // Return empty list for no candidates
        }

        List<EncryptedPoint> result = new ArrayList<>();
        SecretKey currentKey = keyManager.getCurrentKey();
        if (currentKey == null) {
            throw new IllegalStateException("No current session key available");
        }

        // 2. Gather and optionally re-encrypt points from each candidate bucket
        for (Integer bucketId : candidateBuckets) {
            List<EncryptedPoint> bucketPoints = bucketIndex.get(bucketId);
            if (bucketPoints != null) {
                for (EncryptedPoint point : bucketPoints) {
                    // Re-encrypt for forward security if the key has rotated
                    SecretKey previousKey = keyManager.getPreviousKey();
                    if (previousKey != null && !Arrays.equals(point.getCiphertext(), EncryptionUtils.encryptVector(point.decrypt(previousKey), currentKey))) {
                        point.reEncrypt(keyManager, "epoch_" + (keyManager.getTimeEpoch() - 1));
                    }
                    result.add(point);
                }
            }
        }

        // 3. Return all candidate points (server-side filtering not implemented here)
        return result;
    }

    /**
     * Updates the bucket index with new or modified points.
     * @param bucketId The bucket ID to update.
     * @param points The list of EncryptedPoint objects.
     */
    public void updateBucketIndex(int bucketId, List<EncryptedPoint> points) {
        bucketIndex.put(bucketId, new ArrayList<>(Objects.requireNonNull(points, "points cannot be null")));
    }

    /**
     * Removes a point from the bucket index.
     * @param pointId The ID of the point to remove.
     */
    public void removeFromBucketIndex(String pointId) {
        bucketIndex.values().forEach(bucket -> bucket.removeIf(p -> p.getPointId().equals(pointId)));
    }
}