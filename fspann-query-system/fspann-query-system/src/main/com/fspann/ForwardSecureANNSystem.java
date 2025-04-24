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
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static com.fspann.query.QueryGenerator.*;

public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

    private final SecureLSHIndex index;
    private final KeyManager keyManager;
    private final KeyVersionManager keyVersionManager;  // KeyVersionManager initialized here
    private final QueryProcessor queryProcessor;
    private final LRUCache<QueryToken, List<EncryptedPoint>> queryCache;
    public final Map<String, EncryptedPoint> encryptedDataStore;
    private final int totalInsertTimeMs = 0;
    private final int totalFakePoints = 0;
    private final int totalRehashes = 0;
    private final Profiler profiler;
    private final AtomicInteger operationCount;
    private List<double[]> baseVectors;
    private List<int[]> groundTruth;
    private List<double[]> queryVectors;

    public ForwardSecureANNSystem(String basePath, String queryPath, String groundTruthPath,
                                  int numHashTables, int numIntervals,
                                  int maxBucketSize, int targetBucketSize, boolean useFakePoints, boolean useForwardSecurity) throws IOException {

        // Initialize KeyManager and KeyVersionManager
        keyManager = new KeyManager(1000);  // Set your rotation interval here
        this.keyVersionManager = new KeyVersionManager(keyManager, 1000);  // KeyVersionManager with KeyManager

        this.queryCache = new LRUCache<>(1000);  // LRUCache for query results
        this.encryptedDataStore = new HashMap<>();
        Map<String, String> metadata = new HashMap<>();
        this.operationCount = new AtomicInteger(0);
        this.profiler = new Profiler();  // Initialize profiler

        // Load the datasets
        logger.info("[STEP] 📥 Loading Datasets...");
        DataLoader dataLoader = new DataLoader();
        List<double[]> baseVectors = dataLoader.loadData(basePath, 1000);
        List<double[]> queryVectors = dataLoader.loadData(queryPath, 1000);
        dataLoader.loadData(groundTruthPath,1000);

        logger.info("[STEP] ✅ Dataset loading complete.");

        // Initialize LSH and the index
        EvenLSH initialLsh = new EvenLSH(baseVectors.get(0).length, numIntervals, baseVectors); // Initialize LSH
        this.index = new SecureLSHIndex(numHashTables, keyManager.getCurrentKey(), baseVectors);  // SecureLSHIndex
        QueryGenerator queryGenerator = new QueryGenerator(initialLsh, keyManager);  // Provide initialLsh and keyManager to QueryGenerator
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager);  // Provide keyManager to QueryProcessor
    }

    // Query the encrypted data store with caching mechanism
    public List<double[]> query(double[] queryVector, int k) throws Exception {
        // Step 1: Generate QueryToken from queryVector
        EvenLSH initialLsh = null;
        QueryToken queryToken = generateQueryToken(initialLsh, keyManager, queryVector, k, 1);

        // Step 2: Check the cache for previous results
        List<EncryptedPoint> result = queryCache.get(queryToken);

        // If the result is in the cache, return it
        if (result != null) {
            logger.info("Cache hit for query: {}", queryToken);
            return decryptEncryptedPoints(result);  // Decrypt cached points
        }

        // Otherwise, process the query
        result = queryProcessor.processQuery(index.findNearestNeighborsEncrypted(queryToken));

        // Cache the result for future queries
        queryCache.put(queryToken, result);

        return decryptEncryptedPoints(result);  // Decrypt the result before returning

    }

    // Insert a new vector and handle key rotation and encryption
    public void insert(String id, double[] vector) throws Exception {
        operationCount.incrementAndGet();

        // Get the current versioned key
        SecretKey currentKey = keyVersionManager.getCurrentKey();

        // Encrypt the vector
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        EncryptedPoint encryptedPoint = new EncryptedPoint(encryptedVector, "bucket_v" + currentKey.hashCode(), id);
        encryptedDataStore.put(id, encryptedPoint);

        // Add fake points if necessary (not used currently but could be in future)
        int addedFakes = index.add(id, vector, true);

        // Check if rehashing is required based on the number of operations
        if (keyVersionManager.needsRotation(operationCount.get())) {
            logger.info("[STEP] 🔄 Rotating keys...");
            keyVersionManager.rotateKeys();  // Rotate keys
            index.rehash(keyVersionManager.getKeyManager(), "epoch_v" + (keyVersionManager.getTimeVersion() - 1));  // Pass KeyManager
            operationCount.set(0); // Reset operation count
        }
    }

    // Method to decrypt a list of EncryptedPoints into double[] vectors
    private List<double[]> decryptEncryptedPoints(List<EncryptedPoint> encryptedPoints) throws Exception {
        List<double[]> decryptedVectors = new ArrayList<>();
        SecretKey currentKey = keyVersionManager.getCurrentKey();  // Get the current key for decryption

        for (EncryptedPoint point : encryptedPoints) {
            double[] decryptedVector = point.decrypt(currentKey);  // Decrypt the point
            decryptedVectors.add(decryptedVector);  // Add the decrypted vector to the result list
        }

        return decryptedVectors;
    }

    // Delete a vector from the data store and rehash if necessary
    public void delete(String id) throws Exception {
        encryptedDataStore.remove(id);
        index.remove(id);  // Remove the point from index as well
    }

    // Get the current operation count
    public int getOperationCount() {
        return operationCount.get();
    }

    public Profiler getProfiler() {
        return profiler;
    }

    public List<int[]> getGroundTruth() {
        return groundTruth;
    }

    // Method to return the base vectors
    public List<double[]> getBaseVectors() {
        return baseVectors;
    }

    // Getters for query vectors, ground truth, etc.
    public List<double[]> getQueryVectors() {
        return new ArrayList<>(); // Placeholder: Implement as per requirement
    }

    // Shutdown the system and clean up resources
    public void shutdown() {
        logger.info("Shutting down ForwardSecureANNSystem...");
    }

    // Method to save index
    public void saveIndex(String path) {
        try {
            index.saveIndex(path); // Assuming saveIndex in SecureLSHIndex does not throw IOException
            logger.info("Encrypted index saved to: {}", path);
        } catch (Exception e) {  // Catching general exception as a fallback
            logger.error("Failed to save index", e);
        }
    }

    // Method to load index
    public void loadIndex(String path) {
        try {
            index.loadIndex(path); // Assuming loadIndex in SecureLSHIndex does not throw IOException/ ClassNotFoundException
            logger.info("Encrypted index loaded from: {}", path);
        } catch (Exception e) {  // Catching general exception as a fallback
            logger.error("Failed to load index", e);
        }
    }

    // Main method to run the system
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
            List<double[]> queryVectors = system.getQueryVectors();
            if (queryVectors.isEmpty()) {
                logger.error("No query vectors available to process.");
                return;  // Exit or handle the error as needed
            }
            double[] queryVector = queryVectors.get(0);  // Use .get(0) to safely get the first query vector
                List<double[]> nearestNeighbors = system.query(queryVector, 0);  // Pass range as 0 for k-NN query
            logger.info("Nearest neighbor: {}", Arrays.toString(nearestNeighbors.getFirst()));  // Corrected: Use .get(0) for first result

            logger.info("[STEP] Evaluating Recall@10 on 100 queries...");
            EvaluationEngine.evaluate(system, 10, 100, 0);  // Pass range as 0 for k-NN query
            logger.info("[STEP] ✅ Evaluation Complete.");

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