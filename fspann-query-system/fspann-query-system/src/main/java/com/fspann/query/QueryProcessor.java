package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
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
        if (candidates == null) {
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
                byte[] reEncrypted = EncryptionUtils.encryptVector(decrypted, currentKey);
                point = new EncryptedPoint(reEncrypted, point.getBucketId(), point.getPointId());
            }
            result.add(point);
        }

        return result;
    }

    public void updateBucketIndex(int bucketId, List<EncryptedPoint> points) {
        bucketIndex.put(bucketId, new ArrayList<>(Objects.requireNonNull(points, "points cannot be null")));
    }

    public void removeFromBucketIndex(String pointId) {
        bucketIndex.values().forEach(bucket -> bucket.removeIf(p -> p.getPointId().equals(pointId)));
    }
}