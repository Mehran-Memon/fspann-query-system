package java.com.fspann.query;

import java.com.fspann.encryption.EncryptionUtils;
import java.com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class QueryProcessor {
    private final Map<Integer, List<EncryptedPoint>> bucketIndex;
    private final KeyManager keyManager;

    public QueryProcessor(Map<Integer, List<EncryptedPoint>> bucketIndex, KeyManager keyManager) {
        this.bucketIndex = new ConcurrentHashMap<>(Objects.requireNonNull(bucketIndex, "bucketIndex cannot be null"));
        this.keyManager = Objects.requireNonNull(keyManager, "keyManager cannot be null");
    }

    public List<EncryptedPoint> processQuery(List<EncryptedPoint> candidates) throws Exception {
        if (candidates == null || candidates.isEmpty()) {
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
                double[] decrypted = point.decrypt(previousKey);

                // Check if re-encryption is necessary before adding to the result
                byte[] reEncrypted = EncryptionUtils.encryptVector(decrypted, currentKey);
                point = new EncryptedPoint(reEncrypted, point.getBucketId(), point.getPointId());
            }
            result.add(point);
        }

        return result;
    }

    public void updateBucketIndex(int bucketId, List<EncryptedPoint> points) {
        // Validate input before updating
        if (points == null || points.isEmpty()) {
            throw new IllegalArgumentException("Points cannot be null or empty");
        }

        bucketIndex.put(bucketId, new ArrayList<>(points));
    }

    public void removeFromBucketIndex(String pointId) {
        // Iterate through the buckets and remove the point by its ID
        bucketIndex.values().forEach(bucket -> bucket.removeIf(p -> p.getPointId().equals(pointId)));
    }
}
