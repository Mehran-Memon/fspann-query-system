package com.fspann;

import com.fspann.config.SystemConfig;
import com.fspann.data.DataLoader;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.evaluation.EvaluationEngine;
import com.fspann.index.EvenLSH;
import com.fspann.index.SecureLSHIndex;
import com.fspann.keymanagement.KeyManager;
import com.fspann.keymanagement.KeyVersionManager;
import com.fspann.keymanagement.ReEncryptor;
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
    private final ReEncryptor reEncryptor;
    private final Thread reEncryptThread;

    private long totalInsertTimeMs = 0;
    private int totalRehashes = 0;
    private int totalFakePoints = 0;

    public  ForwardSecureANNSystem(String basePath, String queryPath, String groundTruthPath,
                                  int numHashTables, int numIntervals,
                                  int maxBucketSize, int targetBucketSize, boolean useFakePoints, boolean useForwardSecurity) throws IOException {

        keyManager = new KeyManager(1000);
        this.keyVersionManager = new KeyVersionManager(keyManager, 1000);
        this.queryCache = new LRUCache<>(1000);
        this.encryptedDataStore = new HashMap<>();
        this.operationCount = new AtomicInteger(0);
        this.profiler = new Profiler();
        this.metadataManager = new MetadataManager(keyManager);

        // Load datasets
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
        this.index = new SecureLSHIndex(numHashTables, keyManager.getCurrentKey(), baseVectors);
        QueryGenerator queryGenerator = new QueryGenerator(initialLsh, keyManager);
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager, 1000);

        // Initialize ANN
        logger.info("Initializing ANN index...");
        ann = new ANN(baseVectors.get(0).length, numIntervals, keyManager);
        ann.buildIndex(baseVectors);  // Build the index with the base data
        this.reEncryptor = new ReEncryptor(index, keyManager);
        this.reEncryptThread = new Thread(reEncryptor, "ReEncryptor");
        reEncryptThread.setDaemon(true);
        reEncryptThread.start();

    }

    public List<double[]> query(double[] queryVector, int topK) throws Exception {
        logger.info("[STEP] 📤 Querying with vector: {}", Arrays.toString(queryVector));
        SecretKey currentKey = keyManager.getCurrentKey();
        if (currentKey == null) {
            logger.error("Failed to get the current session key. KeyManager might not be initialized correctly.");
            throw new IllegalStateException("Current session key is null");
        }

        EvenLSH lsh = new EvenLSH(baseVectors.get(0).length, 1000);
        QueryToken queryToken = QueryGenerator.generateQueryToken(queryVector, topK, 1, lsh, keyManager);
        logger.info("[STEP] 📦 Generated query token: {}", queryToken);

        List<EncryptedPoint> result = queryCache.get(queryToken);
        if (result != null) {
            logger.info("Cache hit for query: {}", queryToken);
            return decryptEncryptedPoints(result);
        }

        logger.info("Processing query...");
        result = queryProcessor.processQuery(queryToken, index.findNearestNeighborsEncrypted(queryToken));
        queryCache.put(queryToken, result);
        return decryptEncryptedPoints(result);
    }

    public void insert(String id, double[] vector) throws Exception {
        if (SystemConfig.PROFILER_ENABLED) profiler.start("insert");

        long startTime = System.currentTimeMillis();
        operationCount.incrementAndGet();

        // Ensure the vector exists in baseVectors by adding it if not present
        if (!baseVectors.contains(vector)) {
            baseVectors.add(vector);
        }

        SecretKey currentKey = keyManager.getSessionKey(keyVersionManager.getTimeVersion());
        byte[] encryptedVec = EncryptionUtils.encryptVector(vector, currentKey);

        int vecIndex = baseVectors.indexOf(vector); // Now works because vector is added to baseVectors
        int bucketId = ann.getBucketId(vector);        // bucketCode == bucketId for now

        EncryptedPoint ep = new EncryptedPoint(encryptedVec, "bucket_v" + bucketId, id, vecIndex);
        encryptedDataStore.put(id, ep);

        /* use the SecureLSHIndex field explicitly */
        int addedFakes = this.index.add(id, vector, bucketId, true, baseVectors);
        totalFakePoints += addedFakes;

        if (keyVersionManager.needsRotation()) {
            logger.info("[STEP] 🔄 Rotating keys...");
            keyVersionManager.rotateKeys();
            this.index.rehash(keyManager, "epoch_v" + keyVersionManager.getTimeVersion());
            totalRehashes++;
            operationCount.set(0);
        }
        totalInsertTimeMs += (System.currentTimeMillis() - startTime);

        if (SystemConfig.PROFILER_ENABLED) profiler.stop("insert");

        System.gc();

    }

    private List<double[]> decryptEncryptedPoints(List<EncryptedPoint> encryptedPoints) throws Exception {
        List<double[]> decryptedVectors = new ArrayList<>();
        SecretKey currentKey = keyManager.getSessionKey(keyVersionManager.getTimeVersion());

        for (EncryptedPoint point : encryptedPoints) {
            double[] decryptedVector = point.decrypt(currentKey);
            decryptedVectors.add(decryptedVector);
        }
        return decryptedVectors;
    }

    public void delete(String id) throws Exception {
        encryptedDataStore.remove(id);
        index.remove(id);
        logger.info("Deleted point with ID: {}", id);
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

    public List<double[]> getQueryVectors() {
        return queryVectors;  // Return the query vectors
    }

    public List<int[]> getGroundTruth() {
        return groundTruth;  // Return the ground truth data
    }

    public List<double[]> getBaseVectors() {
        return baseVectors;  // Return the base vectors
    }

    public ANN getANN() {
        return ann;  // Return the ANN instance
    }

    public void loadIndex(String path) {
        try {
            index.loadIndex(path);
            logger.info("Encrypted index loaded from: {}", path);
        } catch (Exception e) {
            logger.error("Failed to load index", e);
        }
    }

    public void shutdown() {
        logger.info("Shutting down ForwardSecureANNSystem...");
        reEncryptor.shutdown();
        try {
            reEncryptThread.join();
        } catch (InterruptedException ignored) {
        }
    }
    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger("FSPANN-Main");
        try {
            // System bootstrap
            String basePath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs";
            String queryPath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_query.fvecs";
            String groundTruthPath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_groundtruth.ivecs";

            // Initialize the system
            ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                    basePath, queryPath, groundTruthPath,
                    3, 10, 1000, 1500, true, true);

            // Insert 1 million data points
            log.info("Inserting 1 million data points...");
            Random rnd = new Random();
            List<double[]> base = sys.getBaseVectors();

            // Insert 1M data points
            for (int i = 0; i < 1000000; i++) {
                double[] vec = base.get(rnd.nextInt(base.size()));
                sys.insert("dyn_" + i, vec);
                if (i % 10000 == 0) {
                    log.info("  ↳ {} records inserted", i + 1);
                }
            }

            // Perform a sample query with one of the inserted vectors
            log.info("Performing a sample query...");
            double[] q0 = sys.getQueryVectors().getFirst(); // Sample query vector
            sys.query(q0, 10);  // Query top 10 nearest neighbors

            // Delete a random data point (optional)
            String deleteId = "dyn_" + rnd.nextInt(1000000);
            log.info("Deleting the data point with ID: {}", deleteId);
            sys.delete(deleteId);

            // Save and load the index
            log.info("Saving the index...");
            sys.saveIndex("C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup");

            log.info("Loading the index...");
            sys.loadIndex("C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup");

            // Shutdown the system gracefully
            sys.shutdown();

        } catch (Exception ex) {
            LoggerFactory.getLogger("FSPANN-Main").error("🔥 Fatal error", ex);
            System.exit(1);
        }
    }

    }