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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

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
        this.keyManager = new KeyManager(1000);
        EvenLSH initialLsh = new EvenLSH(dimensions, numIntervals, initialData != null ? initialData : Collections.emptyList());
        this.index = new SecureLSHIndex(dimensions, numHashTables, numIntervals, null, maxBucketSize, targetBucketSize, initialData);
        this.queryGenerator = new QueryGenerator(initialLsh, keyManager);
        this.queryClientHandler = new QueryClientHandler(keyManager);
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager);
        this.encryptedDataStore = new ConcurrentHashMap<>();
        this.metadata = new ConcurrentHashMap<>();
        this.queryExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.operationCount = 0;
        initializeKeys();
    }

    private void initializeKeys() {
        try {
            index.setCurrentKey(keyManager.getCurrentKey());
        } catch (Exception e) {
            logger.error("Failed to initialize keys", e);
            throw new RuntimeException("Failed to initialize keys: " + e.getMessage());
        }
    }

    public void insert(String id, double[] vector) throws Exception {
        operationCount++;
        SecretKey currentKey = keyManager.getCurrentKey();
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        EvenLSH lsh = index.getLshFunctions().get(0); // Use first LSH function to determine bucket
        int bucketId = lsh.getBucketId(vector);
        EncryptedPoint encryptedPoint = new EncryptedPoint(encryptedVector, "bucket_" + bucketId, id);
        encryptedDataStore.put(id, encryptedPoint);
        metadata.put(id, "epoch_" + keyManager.getCurrentKey().hashCode());
        index.add(id, vector); // Add to LSH index

        if (keyManager.needsRotation(operationCount)) {
            Map<String, byte[]> encryptedDataMap = new HashMap<>();
            for (Map.Entry<String, EncryptedPoint> entry : encryptedDataStore.entrySet()) {
                encryptedDataMap.put(entry.getKey(), entry.getValue().getCiphertext());
            }
            keyManager.rotateAllKeys(new ArrayList<>(encryptedDataStore.keySet()), encryptedDataMap);
            index.rehash(keyManager, "epoch_" + (keyManager.getTimeEpoch() - 1));
            operationCount = 0;
        }
    }

    public List<double[]> query(double[] queryVector, int k) throws Exception {
        // Use k as topK and set expansionRange to a default value (e.g., 1)
        QueryToken token = queryGenerator.generateQueryToken(queryVector, k, 1);
        List<EncryptedPoint> candidates = index.findNearestNeighborsEncrypted(token);
        List<String> candidateIds = new ArrayList<>();
        for (EncryptedPoint point : candidates) {
            candidateIds.add(point.getPointId());
        }
        List<double[]> nearestNeighbors = new ArrayList<>();
        SecretKey currentKey = keyManager.getCurrentKey();

        for (String id : candidateIds) {
            EncryptedPoint point = encryptedDataStore.get(id);
            if (point != null) {
                nearestNeighbors.add(decryptPoint(point, currentKey));
            }
        }

        nearestNeighbors.sort((v1, v2) -> Double.compare(distance(queryVector, v1), distance(queryVector, v2)));
        return nearestNeighbors.subList(0, Math.min(k, nearestNeighbors.size()));
    }

    private double[] decryptPoint(EncryptedPoint point, SecretKey key) throws Exception {
        return point.decrypt(key); // Use EncryptedPoint's decrypt method
    }

    private double distance(double[] v1, double[] v2) {
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public void shutdown() {
        queryExecutor.shutdown();
        logger.info("Query executor shut down");
    }

    public static void main(String[] args) {
        try {
            logger.info("Starting ForwardSecureANNSystem...");
            int dimensions = 128;
            int numHashTables = 5;
            int numIntervals = 15;
            int maxBucketSize = 1000;
            int targetBucketSize = 1500;

            List<double[]> initialData = Collections.emptyList();
            ForwardSecureANNSystem system = new ForwardSecureANNSystem(dimensions, numHashTables, numIntervals, maxBucketSize, targetBucketSize, initialData);

            double[] vector1 = new double[dimensions];
            Arrays.fill(vector1, 1.0);
            double[] vector2 = new double[dimensions];
            Arrays.fill(vector2, 2.0);
            system.insert("point1", vector1);
            system.insert("point2", vector2);

            double[] queryVector = new double[dimensions];
            Arrays.fill(queryVector, 1.5);
            List<double[]> nearestNeighbors = system.query(queryVector, 1);
            logger.info("Nearest neighbor: {}", Arrays.toString(nearestNeighbors.get(0)));

            system.shutdown();
            logger.info("ForwardSecureANNSystem shutdown successfully");
        } catch (Exception e) {
            logger.error("Error executing ForwardSecureANNSystem", e);
        }
    }
}