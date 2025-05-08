package com.fspann;

import com.fspann.data.DataLoader;
import com.fspann.encryption.EncryptionUtils;
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

import java.io.File;
import java.util.Set;

import java.io.IOException;
import javax.crypto.SecretKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private final int numIntervals;
    private final Set<Integer> uniqueHashes = ConcurrentHashMap.newKeySet();
    String metadataFilePath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup\\metadata.ser";
    String keysFilePath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup\\keys.ser";

    private long totalInsertTimeMs = 0;
    private int totalRehashes = 0;
    private int totalFakePoints = 0;

    public ForwardSecureANNSystem(String basePath, String queryPath, String groundTruthPath,
                                  int numHashTables, int numIntervals,
                                  int maxBucketSize, int targetBucketSize, boolean useFakePoints, boolean useForwardSecurity,
                                  String keysFilePath) throws IOException {
        // Initialize keyManager first
        this.keyManager = new KeyManager(keysFilePath, 1000);  // Pass the keysFilePath here
        MetadataManager metadataManager = new MetadataManager(keyManager);
        try {
            metadataManager.loadMetadata(metadataFilePath); // Ensure metadata is loaded
            metadataManager.loadKeys(keysFilePath);         // Ensure keys are loaded
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Error loading metadata or keys, initializing new manager.");
            // Handle the issue if loading fails, perhaps creating a fresh MetadataManager
            createNewMetadata(); // This can be a method to handle fresh creation
        }
        this.encryptedDataStore = new HashMap<>();
        // Now initialize other components
        this.keyVersionManager = new KeyVersionManager(keyManager, 1000);
        this.queryCache = new LRUCache<>(1000);
        this.operationCount = new AtomicInteger(0);
        this.profiler = new Profiler();
        this.metadataManager = new MetadataManager(keyManager);
        this.numIntervals = numIntervals;
        metadataManager.addMetadata("version", "1.0", metadataFilePath);
        String version = metadataManager.getMetadata("version");

        // Check if metadata file exists, if not, create it
        File metadataFile = new File("C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup\\metadata.ser\"");
        if (!metadataFile.exists()) {
            // Create new metadata if not found
            createNewMetadata();
        }

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
        ann = new ANN(baseVectors.get(0).length, numIntervals, keyManager, baseVectors );
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

        EvenLSH lsh = new EvenLSH(queryVector.length, numIntervals);  // Ensure lsh can handle multi-dimensional vectors
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

    public void insert(String id, double[] vec) throws Exception {
        long t0 = System.nanoTime();

        // Check if key rotation is needed after an insert
        keyManager.rotateKeysIfNeeded(); // Rotate the keys if the operation count exceeds the threshold

        // Continue with the insert process
        int hash = Arrays.hashCode(vec);
        if (!uniqueHashes.add(hash)) return; // Skip if already processed

        int vecIdx = baseVectors.size();
        baseVectors.add(vec);

        // Encrypt the new vector
        SecretKey key  = keyManager.getCurrentKey();
        byte[] cipher  = EncryptionUtils.encryptVector(vec, key);
        int bucketId   = ann.getBucketId(vec);

        // Book-keeping
        EncryptedPoint ep = new EncryptedPoint(cipher, "bucket_v"+bucketId, id, vecIdx);
        encryptedDataStore.put(id, ep);

        // Update index with the encrypted point
        totalFakePoints += index.add(id, vec, bucketId, true, baseVectors);
        ann.updateIndex(vec);

        // Rehash the index with new keys if necessary
        keyManager.rehashIndexForNewKeys();

        totalInsertTimeMs += (System.nanoTime() - t0) / 1_000_000.0;
    }

    private void createNewMetadata() {
        // Implement logic to generate new metadata
        // This could be any default or initial configuration that your system needs
        System.out.println("Creating new metadata...");
        metadataManager.addMetadata("version", "1.0", "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup");
        metadataManager.saveMetadata("C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup");
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

    public static void main(String[] args) throws Exception {
        // Initialize the ForwardSecureANNSystem with proper file paths and parameters
        var sys = new ForwardSecureANNSystem(
                "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs",
                "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_query.fvecs",
                "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_groundtruth.ivecs",
                3,               // hash tables
                10,              // numIntervals
                1_000, 1_500,    // bucket sizes
                true, true, "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup\\keys.ser");     // forward security enabled

        // Check if base vectors are loaded properly
        if (sys.getBaseVectors() == null || sys.getBaseVectors().isEmpty()) {
            System.err.println("Base vectors are empty or not loaded correctly.");
            return;
        }

        // List to hold the results of the insertion
        List<String> insertedUUIDs = new ArrayList<>();
        System.out.println("Starting batch insertion...");
        for (double[] vec : sys.getBaseVectors()) {
            try {
                String uuid = UUID.randomUUID().toString();
                sys.insert(uuid, vec);
                insertedUUIDs.add(uuid); // Collect the inserted UUIDs
            } catch (Exception e) {
                System.err.println("Error during insertion of vector: " + Arrays.toString(vec));
                throw new RuntimeException("Error during insertion", e);
            }
        }
        System.out.println("Batch insertion completed.");

        // *** Query Sample ***
        System.out.println("Performing a sample query...");
        List<double[]> result = sys.query(sys.getQueryVectors().get(0), 10);
        if (result == null || result.isEmpty()) {
            System.err.println("Query returned no results.");
            return;
        }
        System.out.println("Top-1 distance = " + java.util.Arrays.toString(result.get(0)));

        // *** Save Index ***
        String backupDirectory = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup";
        File dir = new File(backupDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        System.out.println("Saving the index...");
        sys.saveIndex(backupDirectory);
        System.out.println("Index saved successfully.");

        // *** Shutdown System ***
        System.out.println("Shutting down system...");
        sys.shutdown();
        System.out.println("System shutdown complete.");
    }

 }