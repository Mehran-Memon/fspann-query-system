package com.fspann;

import com.fspann.data.DataLoader;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.evaluation.EvaluationEngine;
import com.fspann.index.EvenLSH;
import com.fspann.index.SecureLSHIndex;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryClientHandler;
import com.fspann.query.QueryGenerator;
import com.fspann.query.QueryProcessor;
import com.fspann.query.QueryToken;
import com.fspann.utils.Profiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
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
    private final DataLoader dataLoader;
    private final List<double[]> baseVectors;
    private final List<double[]> queryVectors;
    private final List<int[]> groundTruth;
    private final int dimensions;
    private final boolean useFakePoints;
    private final boolean useForwardSecurity;
    private int operationCount;
    public final Profiler profiler = new Profiler();
    private int totalFakePoints = 0;
    private int totalRehashes = 0;
    private long totalInsertTimeMs = 0;

    public ForwardSecureANNSystem(String basePath, String queryPath, String groundTruthPath,
                                  int numHashTables, int numIntervals,
                                  int maxBucketSize, int targetBucketSize,
                                  boolean useFakePoints, boolean useForwardSecurity) throws IOException {
        this.dataLoader = new DataLoader();
        this.useFakePoints = useFakePoints;
        this.useForwardSecurity = useForwardSecurity;

        logger.info("[STEP] 📥 Loading Datasets...");
        this.baseVectors = dataLoader.readFvecs(basePath);
        this.queryVectors = dataLoader.readFvecs(queryPath);
        this.groundTruth = dataLoader.readIvecs(groundTruthPath);
        logger.info("[STEP] ✅ Dataset loading complete. Base Vectors: {}, Query Vectors: {}", baseVectors.size(), queryVectors.size());

        int estimatedRehashes = baseVectors.size() / 1000; // or keyManager.getRotationThreshold()
        logger.info("[INFO] 🔁 Estimated total rehash runs for this dataset: {}", estimatedRehashes);

        this.dimensions = baseVectors.isEmpty() ? 0 : baseVectors.getFirst().length;

        logger.info("[STEP] 🔐 Initializing KeyManager and LSH...");
        this.keyManager = new KeyManager(1000);
        EvenLSH initialLsh = new EvenLSH(dimensions, numIntervals, baseVectors);
        this.index = new SecureLSHIndex(dimensions, numHashTables, numIntervals, null, maxBucketSize, targetBucketSize, baseVectors);
        this.queryGenerator = new QueryGenerator(initialLsh, keyManager);
        this.queryClientHandler = new QueryClientHandler(keyManager);
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager);
        this.encryptedDataStore = new ConcurrentHashMap<>();
        this.metadata = new ConcurrentHashMap<>();
        this.queryExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.operationCount = 0;

        initializeKeys();
        initializeData();
    }

    private void initializeKeys() {
        try {
            logger.info("[STEP] 🔐 Starting Key Initialization...");
            SecretKey currentKey = keyManager.getCurrentKey();
            index.setCurrentKey(currentKey);
            logger.info("[STEP] ✅ Key Initialization Complete.");
        } catch (Exception e) {
            logger.error("Failed to initialize keys", e);
            throw new RuntimeException("Failed to initialize keys: " + e.getMessage());
        }
    }

    private void initializeData() {
        try {
            logger.info("[STEP] 📦 Starting Index Insertion...");
            profiler.start("IndexBuild");
            long start = System.currentTimeMillis();
            for (int i = 0; i < baseVectors.size(); i++) {
                String id = "vector_" + i;
                insert(id, baseVectors.get(i));
            }
            totalInsertTimeMs += (System.currentTimeMillis() - start);
            profiler.stop("IndexBuild");
            profiler.log("IndexBuild");
            logger.info("[STEP] ✅ Index Insertion Complete. Inserted {} vectors.", baseVectors.size());
        } catch (Exception e) {
            logger.error("Failed to initialize data", e);
            throw new RuntimeException("Failed to initialize data: " + e.getMessage());
        }
    }

    public void insert(String id, double[] vector) throws Exception {
        operationCount++;
        SecretKey currentKey = keyManager.getCurrentKey();
        keyManager.registerKey(id, currentKey);

        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        EvenLSH lsh = index.getLshFunctions().get(0);
        int bucketId = lsh.getBucketId(vector);
        EncryptedPoint encryptedPoint = new EncryptedPoint(encryptedVector, "bucket_" + bucketId, id);
        encryptedDataStore.put(id, encryptedPoint);
        metadata.put(id, "epoch_" + currentKey.hashCode());

        int addedFakes = index.add(id, vector, useFakePoints);
        totalFakePoints += addedFakes;

        if (useForwardSecurity && keyManager.needsRotation(operationCount)) {
            profiler.start("Rehash");
            totalRehashes++;

            Map<String, byte[]> encryptedDataMap = new HashMap<>();
            for (Map.Entry<String, EncryptedPoint> entry : encryptedDataStore.entrySet()) {
                encryptedDataMap.put(entry.getKey(), entry.getValue().getCiphertext());
            }

            keyManager.rotateAllKeys(new ArrayList<>(encryptedDataStore.keySet()), encryptedDataMap);
            index.rehash(keyManager, "epoch_" + (keyManager.getTimeEpoch() - 1));

            profiler.stop("Rehash");
            profiler.log("Rehash");

            operationCount = 0;
        }
    }

    // Modified delete method
    public void delete(String id) throws Exception {
        // Remove from the encrypted data store
        encryptedDataStore.remove(id);
        metadata.remove(id);
        logger.debug("Removed vector and metadata for id: {}", id);

        // Remove from the index
        index.remove(id);

        // Remove the key associated with the vector
        keyManager.removeKey(id);

        // If needed, trigger rehash (if deletion affects forward security)
        if (keyManager.needsRotation(operationCount)) {
            profiler.start("Rehash");
            index.rehash(keyManager, "epoch_" + (keyManager.getTimeEpoch() - 1));
            profiler.stop("Rehash");
            profiler.log("Rehash");
            operationCount = 0;
        }

        logger.info("Deleted vector and key for id: {}", id);
    }

    // Range query method
    public List<double[]> query(double[] queryVector, int k, double range) throws Exception {
        profiler.start("Query");

        List<EncryptedPoint> candidates;
        if (range > 0) {
            // Perform range query
            candidates = index.findRangeQueryEncrypted(queryVector, range);
        } else {
            // Perform k-NN query
            QueryToken token = queryGenerator.generateQueryToken(queryVector, k, 1);
            candidates = index.findNearestNeighborsEncrypted(token);
        }

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

        nearestNeighbors.sort(Comparator.comparingDouble(v -> distance(queryVector, v)));
        profiler.stop("Query");
        profiler.log("Query");

        return nearestNeighbors.subList(0, Math.min(k, nearestNeighbors.size()));
    }

    private double[] decryptPoint(EncryptedPoint point, SecretKey key) throws Exception {
        return point.decrypt(key);
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

    public List<double[]> getQueryVectors() {
        return queryVectors;
    }

    public List<int[]> getGroundTruth() {
        return groundTruth;
    }

    public List<double[]> getBaseVectors() {
        return baseVectors;
    }

    public void saveIndex(String path) {
        try {
            index.saveIndex(path);
            logger.info("Encrypted index saved to: {}", path);
        } catch (IOException e) {
            logger.error("Failed to save index", e);
        }
    }

    public void loadIndex(String path) {
        try {
            index.loadIndex(path);
            logger.info("Encrypted index loaded from: {}", path);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to load index", e);
        }
    }

    public static void main(String[] args) {
        try {
            logger.info("🚀 Starting ForwardSecureANNSystem...");

            ForwardSecureANNSystem system = getForwardSecureANNSystem();
            String backupPath = "data/index_backup";

            if (Files.exists(new File(backupPath + "/encrypted_points.ser").toPath())) {
                logger.info("[STEP] Loading Index Backup...");
                system.loadIndex(backupPath);
            } else {
                logger.info("[STEP] No index backup found. Rebuilding from scratch...");
            }

            logger.info("[STEP] Running Sample Query...");
            double[] queryVector = system.getQueryVectors().getFirst(); // Use the first query vector as an example
            List<double[]> nearestNeighbors = system.query(queryVector, 10, 0); // k-NN query with k=10
            logger.info("Nearest neighbors: {}", Arrays.toString(nearestNeighbors.getFirst())); // Log first neighbor

            // For Range Query:
            double range = 5.0; // Example range
            List<double[]> rangeNeighbors = system.query(queryVector, 10, range); // Range query
            logger.info("Range query results: {}", rangeNeighbors);

            system.profiler.exportToCSV("logs/profiler_stats.csv");

            logger.info("[SUMMARY] Total insert time (ms): {}", system.totalInsertTimeMs);
            logger.info("[SUMMARY] Total rehashes: {}", system.totalRehashes);
            logger.info("[SUMMARY] Total fake points inserted: {}", system.totalFakePoints);

            system.saveIndex(backupPath);
            system.shutdown();

            logger.info("✅ ForwardSecureANNSystem shutdown successfully");
        } catch (Exception e) {
            logger.error("❌ Error executing ForwardSecureANNSystem", e);
        }
    }

    private static ForwardSecureANNSystem getForwardSecureANNSystem() throws IOException {
        String basePath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs";
        String queryPath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_query.fvecs";
        String groundTruthPath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_groundtruth.ivecs";

        return new ForwardSecureANNSystem(
                basePath, queryPath, groundTruthPath,
                5, 15, 1000, 1500,
                true,  // useFakePoints
                true   // useForwardSecurity
        );
    }
}
