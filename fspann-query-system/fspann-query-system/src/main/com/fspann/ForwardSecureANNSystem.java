package com.fspann;

import com.fspann.data.DataLoader;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.evaluation.EvaluationEngine;
import com.fspann.index.EvenLSH;
import com.fspann.index.SecureLSHIndex;
import com.fspann.keymanagement.KeyManager;
import com.fspann.keymanagement.KeyVersionManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryGenerator;
import com.fspann.query.QueryProcessor;
import com.fspann.query.QueryToken;
import com.fspann.utils.LRUCache;
import com.fspann.utils.Profiler;
import com.fspann.index.ANN;
import com.fspann.keymanagement.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.math3.util.MathArrays.distance;

public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

    private final SecureLSHIndex index;
    private final KeyManager keyManager;
    private final KeyVersionManager keyVersionManager;
    private final QueryProcessor queryProcessor;
    private final LRUCache<QueryToken, List<EncryptedPoint>> queryCache;
    public final Map<String, EncryptedPoint> encryptedDataStore;
    private final Profiler profiler;
    private final AtomicInteger operationCount;
    private List<double[]> baseVectors;
    private List<int[]> groundTruth;
    private List<double[]> queryVectors;
    private final MetadataManager metadataManager;
    private final ANN ann;

    private long totalInsertTimeMs = 0;
    private int totalRehashes = 0;
    private int totalFakePoints = 0;

    public ForwardSecureANNSystem(String basePath, String queryPath, String groundTruthPath,
                                  int numHashTables, int numIntervals,
                                  int maxBucketSize, int targetBucketSize, boolean useFakePoints, boolean useForwardSecurity) throws IOException {

        keyManager = new KeyManager(1000);
        this.keyVersionManager = new KeyVersionManager(keyManager, 1000);
        this.queryCache = new LRUCache<>(1000);
        this.encryptedDataStore = new HashMap<>();
        this.operationCount = new AtomicInteger(0);
        this.profiler = new Profiler();
        this.metadataManager = new MetadataManager(keyManager);

        logger.info("[STEP] 📥 Loading Datasets...");
        DataLoader dataLoader = new DataLoader();
        baseVectors = dataLoader.loadData(basePath, 1000);
        queryVectors = dataLoader.loadData(queryPath, 1000);
        groundTruth = dataLoader.loadGroundTruth(groundTruthPath, 1000);

        logger.info("Base Vectors Size: {}", baseVectors.size());
        logger.info("Query Vectors Size: {}", queryVectors.size());
        logger.info("Ground Truth Size: {}", groundTruth.size());

        logger.info("[STEP] ✅ Dataset loading complete.");

        EvenLSH initialLsh = new EvenLSH(baseVectors.get(0).length, numIntervals);
        this.index = new SecureLSHIndex(numHashTables, keyManager.getCurrentKey(), baseVectors);        QueryGenerator queryGenerator = new QueryGenerator(initialLsh, keyManager);
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager, 1000);

        logger.info("Query vectors size before sample query: {}", queryVectors.size());
        ann = new ANN(baseVectors.get(0).length, numIntervals, keyManager);  // Initialize the ANN index
        ann.buildIndex(baseVectors);  // Build the index with the base data
    }

    // Added getter to resolve 'getQueryVectors' error
    public List<double[]> getQueryVectors() {
        return queryVectors;
    }

    public ANN getANN() {
        return ann;
    }

    public List<double[]> query(double[] queryVector, int topK) throws Exception {

        SecretKey currentKey = keyManager.getCurrentKey();
        if (currentKey == null) {
            logger.error("Failed to get the current session key. KeyManager might not be initialized correctly.");
            throw new IllegalStateException("Current session key is null");
        }

        EvenLSH lsh = new EvenLSH(baseVectors.get(0).length, 1000);  // Initialize LSH
        QueryToken queryToken = QueryGenerator.generateQueryToken(queryVector, topK, 1, lsh, keyManager);

        List<EncryptedPoint> result = queryCache.get(queryToken);
        if (result != null) {
            logger.info("Cache hit for query: {}", queryToken);
            return decryptEncryptedPoints(result);
        }

        result = queryProcessor.processQuery(index.findNearestNeighborsEncrypted(queryToken));
        queryCache.put(queryToken, result);
        return decryptEncryptedPoints(result);
    }

    public void insert(String id, double[] vector) throws Exception {
        long startTime = System.currentTimeMillis();
        operationCount.incrementAndGet();
        SecretKey currentKey = keyManager.getSessionKey(keyVersionManager.getTimeVersion());
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        int index = baseVectors.indexOf(vector);
        int bucketId = ann.getBucketId(vector);
        EncryptedPoint encryptedPoint = new EncryptedPoint(encryptedVector, "bucket_v" + bucketId, id, index);
        encryptedDataStore.put(id, encryptedPoint);
        int addedFakes = this.index.add(id, vector, true, baseVectors);
        totalFakePoints += addedFakes;
        if (keyVersionManager.needsRotation()) {
            logger.info("[STEP] 🔄 Rotating keys...");
            keyVersionManager.rotateKeys();
            this.index.rehash(keyManager, "epoch_v" + keyVersionManager.getTimeVersion());
            totalRehashes++;
            operationCount.set(0);
        }
        totalInsertTimeMs += (System.currentTimeMillis() - startTime);
    }

    private List<double[]> decryptEncryptedPoints(List<EncryptedPoint> encryptedPoints) throws Exception {
        List<double[]> decryptedVectors = new ArrayList<>();
        SecretKey currentKey = keyManager.getSessionKey(keyVersionManager.getTimeVersion()); // Use the keyVersionManager to get the current key

        for (EncryptedPoint point : encryptedPoints) {
            double[] decryptedVector = point.decrypt(currentKey);
            decryptedVectors.add(decryptedVector);
        }
        return decryptedVectors;
    }

    public void delete(String id) throws Exception {
        encryptedDataStore.remove(id);
        index.remove(id);
    }

    public void saveIndex(String path) {
        try {
            index.saveIndex(path);
            logger.info("Encrypted index saved to: {}", path);
        } catch (Exception e) {
            logger.error("Failed to save index to: {}", path, e);
            throw new RuntimeException("Failed to save index", e);
        }
    }

    public void loadIndex(String path) {
        try {
            index.loadIndex(path);
            logger.info("Encrypted index loaded from: {}", path);
        } catch (Exception e) {
            logger.error("Failed to load index", e);
        }
    }

    // Added method to resolve 'shutdown' error
    public void shutdown() {
        logger.info("Shutting down ForwardSecureANNSystem...");
    }

    public List<int[]> getGroundTruth() {
        return groundTruth;
    }

    public List<double[]> getBaseVectors() {
        return baseVectors;
    }

    public static void main(String[] args) {
        try {
            // Initialize DataLoader
            DataLoader dataLoader = new DataLoader();
            int batchSize = 1000;

            // Define file paths
            String basePath = "data/sift_dataset/sift/sift_base.fvecs";
            String queryPath = "data/sift_dataset/sift/sift_query.fvecs";
            String groundTruthPath = "data/sift_dataset/sift/sift_groundtruth.ivecs";

            // Verify file existence
            for (String path : new String[]{basePath, queryPath, groundTruthPath}) {
                File file = new File(path);
                if (!file.exists() || !file.isFile()) {
                    logger.error("File not found or invalid: {}", file.getAbsolutePath());
                    throw new IOException("File not found: " + path);
                }
                logger.info("File verified: {}", file.getAbsolutePath());
            }

            // Initialize system with file paths
            logger.info("Initializing ForwardSecureANNSystem...");
            ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                    basePath, queryPath, groundTruthPath,
                    3, 10, 1000, 1500, true, true
            );
            logger.info("System initialized successfully");

            // Check for index backup
            String backupPath = "data/index_backup";
            File backupDir = new File(backupPath);
            if (backupDir.exists() && backupDir.isDirectory()) {
                logger.info("[STEP] Index backup found at: {}", backupPath);
            } else {
                logger.info("[STEP] No index backup found. Rebuilding from scratch...");
            }

            // Run sample query
            logger.info("[STEP] Running Sample Query...");
            List<double[]> queryVectors = system.getQueryVectors();
            if (queryVectors.isEmpty()) {
                logger.error("No query vectors available to process.");
                throw new IllegalStateException("Query vectors list is empty");
            }
            List<double[]> nearestNeighbors = system.query(queryVectors.get(0), 10);
            logger.info("Nearest neighbor: {}", Arrays.toString(nearestNeighbors.get(0)));

            // Evaluate
            logger.info("[STEP] Evaluating Recall@10 on 100 queries...");
            EvaluationEngine.evaluate(system, 10, 100, 0);
            logger.info("[STEP] ✅ Evaluation Complete.");

            // Save index
            logger.info("[STEP] Saving Index...");
            if (!backupDir.exists()) {
                backupDir.mkdirs();
            }
            system.saveIndex(backupPath);

            // Shutdown
            logger.info("[STEP] Shutting down...");
            system.shutdown();

        } catch (Exception e) {
            logger.error("❌ Error executing ForwardSecureANNSystem", e);
            throw new RuntimeException("Execution failed", e);
        }
    }

//    public static void main(String[] args) {
//        DataLoader dataLoader = new DataLoader();
//        try {
//            List<double[]> queryVectors = dataLoader.loadData("C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_query.fvecs", 1000);
//            logger.info("Query vectors loaded: {}", queryVectors.size());
//            List<int[]> groundTruth = dataLoader.loadGroundTruth("C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_groundtruth.ivecs", 1000);
//            logger.info("Ground truth entries loaded: {}", groundTruth.size());
//        } catch (IOException e) {
//            logger.error("Failed to load data", e);
//        }
//    }

    private static ForwardSecureANNSystem getForwardSecureANNSystem() throws IOException {
        String basePath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs";
        String queryPath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_query.fvecs";
        String groundTruthPath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_groundtruth.ivecs";

        return new ForwardSecureANNSystem(
                basePath, queryPath, groundTruthPath,
                5, 15, 1000, 1500,
                true, true
        );
    }
}
