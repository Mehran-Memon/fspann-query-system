package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryToken;
import com.fspann.utils.PersistenceUtils;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SecureLSHIndex implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int dimensions;
    private final int numHashTables;
    private final int numIntervals;
    private final List<EvenLSH> lshFunctions;
    private final List<Map<Integer, List<EncryptedPoint>>> hashTables;
    private final Map<String, double[]> vectors;
    private final Map<String, EncryptedPoint> encryptedPoints;
    private transient SecretKey currentKey; // Transient: don't serialize secrets
    private final int maxBucketSize;
    private final int targetBucketSize;

    public SecureLSHIndex(int dimensions, int numHashTables, int numIntervals, SecretKey key,
                          int maxBucketSize, int targetBucketSize, List<double[]> initialData) {
        this.dimensions = dimensions;
        this.numHashTables = numHashTables;
        this.numIntervals = numIntervals;
        this.lshFunctions = new ArrayList<>();
        this.hashTables = new ArrayList<>();
        this.vectors = new ConcurrentHashMap<>();
        this.encryptedPoints = new ConcurrentHashMap<>();
        this.currentKey = key;
        this.maxBucketSize = maxBucketSize;
        this.targetBucketSize = targetBucketSize;
        initializeHashTables(initialData);
    }

    private void initializeHashTables(List<double[]> initialData) {
        for (int i = 0; i < numHashTables; i++) {
            lshFunctions.add(new EvenLSH(dimensions, numIntervals, initialData));
            hashTables.add(new HashMap<>());
        }
    }

    public void add(String id, double[] vector, boolean useFakePoints) throws Exception {
        if (vector == null || vector.length != dimensions) {
            throw new IllegalArgumentException("Vector is null or dimension mismatch: expected " + dimensions);
        }
        vectors.put(id, vector.clone());
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        EvenLSH lsh = lshFunctions.getFirst();
        int bucketId = lsh.getBucketId(vector);
        EncryptedPoint point = new EncryptedPoint(encryptedVector, "bucket_" + bucketId, id);
        encryptedPoints.put(id, point);

        for (int i = 0; i < numHashTables; i++) {
            lsh = lshFunctions.get(i);
            bucketId = lsh.getBucketId(vector);
            List<double[]> points = Collections.singletonList(vector);
            List<List<byte[]>> buckets = BucketConstructor.greedyMerge(points, maxBucketSize, currentKey);
            if (useFakePoints) {
                buckets = BucketConstructor.applyFakeAddition(buckets, targetBucketSize, currentKey, dimensions);
            }
            EncryptedPoint encryptedPoint = new EncryptedPoint(buckets.getFirst().getFirst(), "bucket_" + bucketId, id);
            hashTables.get(i).computeIfAbsent(bucketId, k -> new ArrayList<>()).add(encryptedPoint);
        }
    }


    public void update(String id, double[] newVector, boolean useFakePoints) throws Exception {
        remove(id);
        add(id, newVector, useFakePoints);
        for (EvenLSH lsh : lshFunctions) {
            lsh.updateCriticalValues(new ArrayList<>(vectors.values()), numIntervals);
        }
    }


    public void remove(String id) {
        EncryptedPoint point = encryptedPoints.get(id);
        if (point == null) return;
        vectors.remove(id);
        encryptedPoints.remove(id);
        for (int i = 0; i < numHashTables; i++) {
            int bucketId = Integer.parseInt(point.getBucketId().replace("bucket_", ""));
            List<EncryptedPoint> bucket = hashTables.get(i).get(bucketId);
            if (bucket != null) {
                bucket.removeIf(p -> p.getPointId().equals(id));
            }
        }
    }

    public List<EncryptedPoint> findNearestNeighborsEncrypted(QueryToken queryToken) {
        if (queryToken == null) {
            throw new IllegalArgumentException("QueryToken cannot be null");
        }
        Set<EncryptedPoint> candidates = new HashSet<>();
        List<Integer> candidateBuckets = queryToken.getCandidateBuckets();
        if (candidateBuckets == null || candidateBuckets.isEmpty()) {
            return new ArrayList<>();
        }
        for (int i = 0; i < numHashTables; i++) {
            for (Integer bucketId : candidateBuckets) {
                List<EncryptedPoint> bucket = hashTables.get(i).getOrDefault(bucketId, new ArrayList<>());
                candidates.addAll(bucket);
            }
        }
        return new ArrayList<>(candidates);
    }

    public void rehash(KeyManager keyManager, String context) throws Exception {
        SecretKey oldKey = keyManager.getPreviousKey();
        SecretKey newKey = keyManager.getCurrentKey();
        if (newKey == null) {
            throw new IllegalStateException("No current key available for rehashing");
        }
        for (Map<Integer, List<EncryptedPoint>> table : hashTables) {
            for (List<EncryptedPoint> bucket : table.values()) {
                for (EncryptedPoint point : bucket) {
                    point.reEncrypt(keyManager, context);
                }
            }
        }
        for (EvenLSH lsh : lshFunctions) {
            lsh.rehash();
        }
        this.currentKey = newKey;
    }

    public List<EvenLSH> getLshFunctions() {
        return Collections.unmodifiableList(lshFunctions);
    }

    public void setCurrentKey(SecretKey key) {
        this.currentKey = key;
    }

    // 💾 Save the index
    public void saveIndex(String directoryPath) throws IOException {
        File dir = new File(directoryPath);
        if (!dir.exists()) dir.mkdirs();

        PersistenceUtils.saveObject(encryptedPoints, directoryPath + "/encrypted_points.ser");
        PersistenceUtils.saveObject(hashTables, directoryPath + "/hash_tables.ser");
        PersistenceUtils.saveObject(vectors, directoryPath + "/vectors.ser");
    }

    // 💾 Load the index
    @SuppressWarnings("unchecked")
    public void loadIndex(String directoryPath) throws IOException, ClassNotFoundException {
        this.encryptedPoints.clear();
        this.hashTables.clear();
        this.vectors.clear();

        Map<String, EncryptedPoint> epMap =
                PersistenceUtils.loadObject(directoryPath + "/encrypted_points.ser", Map.class);
        List<Map<Integer, List<EncryptedPoint>>> htList =
                PersistenceUtils.loadObject(directoryPath + "/hash_tables.ser", List.class);
        Map<String, double[]> vecMap =
                PersistenceUtils.loadObject(directoryPath + "/vectors.ser", Map.class);

        this.encryptedPoints.putAll(epMap);
        this.hashTables.addAll(htList);
        this.vectors.putAll(vecMap);
    }
}
