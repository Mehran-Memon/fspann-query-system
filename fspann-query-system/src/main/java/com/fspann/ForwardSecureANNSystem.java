package com.fspann;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.index.EvenLSH;
import com.fspann.index.SecureLSHIndex;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryClientHandler;
import com.fspann.query.QueryGenerator;
import com.fspann.query.QueryProcessor;
import com.fspann.query.QueryToken;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The main system class for forward-secure, privacy-preserving approximate nearest neighbor (ANN) queries.
 */
public class ForwardSecureANNSystem {
    private final SecureLSHIndex index;
    private final KeyManager keyManager;
    private final QueryClientHandler queryClientHandler;
    private final QueryGenerator queryGenerator;
    private final QueryProcessor queryProcessor;
    private final ConcurrentHashMap<String, EncryptedPoint> encryptedDataStore;
    private final ConcurrentHashMap<String, String> metadata;
    private final ExecutorService queryExecutor;
    private int operationCount;

    public ForwardSecureANNSystem(int dimensions, int numHashTables, int numIntervals, int maxBucketSize, int targetBucketSize, List<double[]> initialData) {
        this.keyManager = new KeyManager(1000); // Rotate every 1000 operations
        EvenLSH initialLsh = new EvenLSH(dimensions, numIntervals, initialData != null ? initialData : Collections.emptyList());
        this.index = new SecureLSHIndex(dimensions, numHashTables, numIntervals, null, maxBucketSize, targetBucketSize, initialData);
        this.queryGenerator = new QueryGenerator(initialLsh, keyManager);
        this.queryClientHandler = new QueryClientHandler(keyManager);
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager); // Initialize with empty map
        this.encryptedDataStore = new ConcurrentHashMap<>();
        this.metadata = new ConcurrentHashMap<>();
        this.queryExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.operationCount = 0;
        initializeKeys();
    }

    private void initializeKeys() {
        try {
            keyManager.generateMasterKey();
            index.setCurrentKey(keyManager.getCurrentKey());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize keys: " + e.getMessage());
        }
    }

    public void insert(String id, double[] vector) throws Exception {
        if (id == null || vector == null) {
            throw new IllegalArgumentException("ID and vector cannot be null");
        }
        operationCount++;
        if (keyManager.needsRotation(operationCount)) {
            rotateKeys();
        }
        SecretKey key = keyManager.getCurrentKey();
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, key);
        EvenLSH lsh = index.getLshFunctions().get(0);
        int bucketId = lsh.getBucketId(vector);
        EncryptedPoint point = new EncryptedPoint(encryptedVector, "bucket_" + bucketId, id);
        encryptedDataStore.put(id, point);
        index.add(id, vector);
        queryProcessor.updateBucketIndex(bucketId, Collections.singletonList(point));
        metadata.put(id, "INSERT");
    }

    public void update(String id, double[] newVector) throws Exception {
        if (id == null || newVector == null) {
            throw new IllegalArgumentException("ID and newVector cannot be null");
        }
        operationCount++;
        if (keyManager.needsRotation(operationCount)) {
            rotateKeys();
        }
        EncryptedPoint oldPoint = encryptedDataStore.get(id);
        if (oldPoint == null) {
            throw new IllegalArgumentException("Data with ID " + id + " not found.");
        }
        SecretKey oldKey = keyManager.getCurrentKey();
        keyManager.rotateKey("epoch_" + keyManager.getTimeEpoch(), oldPoint.getCiphertext());
        SecretKey newKey = keyManager.getCurrentKey();
        EvenLSH lsh = index.getLshFunctions().get(0);
        int newBucketId = lsh.getBucketId(newVector);
        byte[] encryptedVector = EncryptionUtils.reEncrypt(oldPoint.getCiphertext(), oldKey, newKey);
        EncryptedPoint newPoint = new EncryptedPoint(encryptedVector, "bucket_" + newBucketId, id);
        encryptedDataStore.put(id, newPoint);
        index.update(id, newVector);
        queryProcessor.removeFromBucketIndex(id);
        queryProcessor.updateBucketIndex(newBucketId, Collections.singletonList(newPoint));
        metadata.put(id, "UPDATE");
    }

    public void delete(String id) {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null");
        }
        operationCount++;
        if (keyManager.needsRotation(operationCount)) {
            rotateKeys();
        }
        EncryptedPoint point = encryptedDataStore.get(id);
        if (point != null) {
            int bucketId = Integer.parseInt(point.getBucketId().replace("bucket_", ""));
            index.remove(id);
            encryptedDataStore.remove(id);
            queryProcessor.removeFromBucketIndex(id);
            metadata.put(id, "DELETE");
        }
    }

    public List<double[]> query(double[] queryVector, int k) throws Exception {
        if (queryVector == null || k < 0) {
            throw new IllegalArgumentException("queryVector cannot be null and k cannot be negative");
        }
        operationCount++;
        if (keyManager.needsRotation(operationCount)) {
            rotateKeys();
        }
        QueryToken queryToken = queryGenerator.generateQueryToken(queryVector, k, 1); // Expansion range of 1
        List<EncryptedPoint> candidates = queryProcessor.processQuery(queryToken);
        return queryClientHandler.decryptAndRefine(candidates, queryVector, k);
    }

    private void rotateKeys() throws Exception {
        Map<String, byte[]> dataMap = new HashMap<>();
        for (Map.Entry<String, EncryptedPoint> entry : encryptedDataStore.entrySet()) {
            dataMap.put(entry.getKey(), entry.getValue().getCiphertext());
        }
        keyManager.rotateAllKeys(new ArrayList<>(keyManager.getSessionKeys().keySet()), dataMap);
        index.rehash(keyManager, "epoch_" + (keyManager.getTimeEpoch() - 1));
    }

    public void shutdown() {
        queryExecutor.shutdown();
        queryClientHandler.shutdown();
    }
}