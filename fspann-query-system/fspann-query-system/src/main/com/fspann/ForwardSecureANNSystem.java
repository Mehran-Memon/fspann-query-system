package com.fspann;

import com.fspann.data.DataLoader;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.evaluation.EvaluationEngine;
import com.fspann.index.ANN;
import com.fspann.index.DimensionContext;
import com.fspann.index.EvenLSH;
import com.fspann.index.SecureLSHIndex;
import com.fspann.keymanagement.*;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryGenerator;
import com.fspann.query.QueryProcessor;
import com.fspann.query.QueryToken;
import com.fspann.utils.LRUCache;
import com.fspann.utils.Profiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Main entry for the Forward-Secure ANN System.
 * Supports dynamic insertion, querying, and forward-secure rehashing
 * across multiple dimensions with LSH-backed encrypted indexing.
 */
public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

    /* ---------- Core Components ---------- */
    private final KeyManager keyManager;
    private final KeyVersionManager keyVersionManager;
    private final MetadataManager metadataManager;
    private final ReEncryptor reEncryptor;
    private final Thread reEncryptThread;
    private final QueryProcessor queryProcessor;
    private final LRUCache<QueryToken, List<EncryptedPoint>> queryCache;
    private final Profiler profiler;

    /* ---------- Data & Index Stores ---------- */
    private final ConcurrentHashMap<String, EncryptedPoint> encryptedDataStore = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, DimensionContext> dimensionContexts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Set<List<Double>>> uniqueHashes = new ConcurrentHashMap<>();

    /* ---------- Synchronization ---------- */
    private final ReadWriteLock indexLock = new ReentrantReadWriteLock();

    /* ---------- Configuration ---------- */
    private final int numHashTables;
    private final int numIntervals;
    private final AtomicInteger operationCount = new AtomicInteger(0);

    /* ---------- Statistics ---------- */
    private long totalInsertTimeNanos = 0;
    private int totalFakePoints = 0;

    /* ---------- I/O Paths ---------- */
    private final String metadataFilePath;
    private final String keysFilePath;

    /* ---------- Constructor ---------- */
    public ForwardSecureANNSystem(String basePath,
                                  String queryPath,
                                  String groundTruthPath,
                                  int numHashTables,
                                  int numIntervals,
                                  int maxOperations,
                                  boolean useForwardSecurity,
                                  String metadataFilePath,
                                  String keysFilePath) throws Exception {

        // Core setup
        this.numHashTables = numHashTables;
        this.numIntervals  = numIntervals;
        this.metadataFilePath = metadataFilePath;
        this.keysFilePath = keysFilePath;

        // Initialize KeyManager and metadata
        int maxKeyUsage = 5000;
        KeyRotationPolicy rotationPolicy = new KeyRotationPolicy(
                maxOperations,
                TimeUnit.HOURS.toMillis(1),     // rotate at least every hour
                TimeUnit.SECONDS.toMillis(30),  // shard rotation timeout
                maxKeyUsage                   // max key usage same as operations threshold
        );
        this.keyManager = new KeyManager(keysFilePath, rotationPolicy);
        this.metadataManager = new MetadataManager(keyManager);
        try {
            metadataManager.loadMetadata(metadataFilePath);
            metadataManager.loadKeys(keysFilePath);
        } catch (Exception e) {
            logger.info("Metadata or keys not found; initializing new metadata.");
            createNewMetadata();
        }

        // Auxiliary services
        this.keyVersionManager = new KeyVersionManager(keyManager, maxOperations);
        this.queryCache = new LRUCache<>(maxOperations);
        this.profiler = new Profiler();
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager, maxOperations);

        // Start background re-encryption if enabled
        this.reEncryptor = new ReEncryptor(dimensionContexts, keyManager);
        this.reEncryptThread = new Thread(reEncryptor, "ReEncryptor");
        this.reEncryptThread.setDaemon(true);
        if (useForwardSecurity) reEncryptThread.start();

        // Load data
        logger.info("Loading datasets...");
        DataLoader loader = new DataLoader();
        List<double[]> baseVectors   = loader.loadData(basePath, 1000);
        List<double[]> queryVectors  = loader.loadData(queryPath, 1000);
        List<int[]>   groundTruth    = loader.loadGroundTruth(groundTruthPath, 1000);
        logger.info("Datasets loaded: base={}, queries={}, groundTruth={}",
                baseVectors.size(), queryVectors.size(), groundTruth.size());

        // (Optionally) warm-up each dimension context
        for (double[] vec : baseVectors) {
            insert(UUID.randomUUID().toString(), vec);
        }
    }

    /**
     * Insert a new vector into the appropriate dimension index.
     */
//    public void insert(String id, double[] vec) throws Exception {
//        indexLock.writeLock().lock();
//        try {
//            int dims = vec.length;
//            DimensionContext ctx = dimensionContexts.computeIfAbsent(dims, this::createContext);
//            ANN ann   = ctx.getAnn();
//            EvenLSH lsh = ctx.getLsh();
//            SecureLSHIndex idx = ctx.getIndex();
//
//            // Duplicate suppression (skip if seen)
//            Set<Integer> seen = uniqueHashes.computeIfAbsent(dims, d -> ConcurrentHashMap.newKeySet());
//            int h = Arrays.hashCode(vec);
//            if (!seen.add(h)) {
//                logger.debug("Skipping duplicate vector {} for dim {}", id, dims);
//                return;
//            }
//
//            // Update ANN and baseVectors
//            List<double[]> base = ctx.getBaseVectors();
//            base.add(vec);
//            ann.updateIndex(vec);
//
//            // Encrypt
//            SecretKey key = keyManager.getCurrentKey();
//            byte[] iv     = EncryptionUtils.generateIV();
//            byte[] ct = EncryptionUtils.encryptVector(vec, iv, key);  // Pass IV explicitly
//            int bucket    = ann.getBucketId(vec);
//            EncryptedPoint ep = new EncryptedPoint(ct, "bucket_"+bucket, id, base.size()-1, iv, id);
//
//            // Store
//            encryptedDataStore.put(id, ep);
//            totalFakePoints += idx.add(id, vec, bucket, true, base);
//
//            // Rotation & rehash trigger
//            int before = keyManager.getTimeVersion();
//            keyManager.rotateKeysIfNeeded();
//            int after = keyManager.getTimeVersion();
//            if (after > before) rehashIndex();
//
//            // Stats
//            profiler.stop("insert");
//        } finally {
//            indexLock.writeLock().unlock();
//        }
//    }

    public void insert(String id, double[] vec) throws Exception {
        indexLock.writeLock().lock();
        try {
            int dims = vec.length;
            DimensionContext ctx = dimensionContexts.computeIfAbsent(dims, this::createContext);
            ANN ann   = ctx.getAnn();
            EvenLSH lsh = ctx.getLsh();
            SecureLSHIndex idx = ctx.getIndex();

            // Duplicate suppression (skip if seen)
            Set<List<Double>> seen = uniqueHashes.computeIfAbsent(dims, d -> ConcurrentHashMap.newKeySet());
            // Compare the vectors directly by checking if they are equal
            List<Double> vectorList = Arrays.stream(vec).boxed().collect(Collectors.toList());
            if (!seen.add(vectorList)) {
                logger.debug("Skipping duplicate vector {} for dim {}", id, dims);
                return;
            }



            // Update ANN and baseVectors
            List<double[]> base = ctx.getBaseVectors();
            base.add(vec);
            ann.updateIndex(vec);

            // Encrypt
            SecretKey key = keyManager.getCurrentKey();
            byte[] iv = EncryptionUtils.generateIV();  // Generate IV
            byte[] ct = EncryptionUtils.encryptVector(vec, iv, key);  // Pass IV explicitly

            int bucket = ann.getBucketId(vec);
            EncryptedPoint ep = new EncryptedPoint(ct, "bucket_" + bucket, id, base.size() - 1, iv, id);

            // Store
            encryptedDataStore.put(id, ep);
            totalFakePoints += idx.add(id, vec, bucket, true, base);

            // Rotation & rehash trigger
            int before = keyManager.getTimeVersion();
            keyManager.rotateKeysIfNeeded();
            int after = keyManager.getTimeVersion();
            if (after > before) rehashIndex();

            // Stats
            profiler.stop("insert");
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * Perform full index rehash under write lock.
     */
    public void rehashIndex() {
        indexLock.writeLock().lock();
        try {
            logger.info("Rehashing entire index due to key rotation...");
            // Get all points from encryptedDataStore
            List<EncryptedPoint> points = new ArrayList<>(encryptedDataStore.values());
            // Build a list containing at most the single previous key
            SecretKey prevKey = keyManager.getPreviousKey();
            List<SecretKey> previousKeys = prevKey == null
                    ? Collections.emptyList()
                    : Collections.singletonList(prevKey);

            // Re-encrypt points
            for (EncryptedPoint p : points) {
                try {
                    p.reEncrypt(keyManager, previousKeys);
                } catch (Exception ex) {
                    logger.warn("Failed to re-encrypt {}: {}", p.getId(), ex.getMessage());
                }
            }

            // Rebuild each dimension context
            for (Map.Entry<Integer, DimensionContext> entry : dimensionContexts.entrySet()) {
                int dims = entry.getKey();
                DimensionContext context = entry.getValue();
                ANN ann = context.getAnn();
                SecureLSHIndex idx = context.getIndex();
                List<double[]> base = context.getBaseVectors();
                base.clear();
                idx.clear();
                for (EncryptedPoint p : points) {
                    double[] v = p.decrypt(keyManager.getCurrentKey(), previousKeys);
                    if (v != null && v.length == dims) {
                        base.add(v);
                        int bucketId = ann.getBucketId(v);
                        idx.add(p.getId(), v, bucketId, true, base);
                        ann.updateIndex(v);
                    }
                }
            }

            logger.info("Rehash complete: reinserted {} points", encryptedDataStore.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * Create new metadata entries and persist them.
     */
    private void createNewMetadata() {
        // Initialize version metadata
        metadataManager.addMetadata("version", "1.0", metadataFilePath);
        metadataManager.saveMetadata(metadataFilePath);
    }

    /**
     * Generate a reader-friendly context for the given dimension.
     */
    private DimensionContext createContext(int dims) {
        List<double[]> base = new CopyOnWriteArrayList<>();
        DimensionContext ctx = new DimensionContext(dims, numIntervals, numHashTables, keyManager, base);
        try { ctx.getAnn().buildIndex(base); }
        catch (Exception ex) { throw new RuntimeException("ANN init failed", ex); }
        return ctx;
    }

    /**
     * Query for the top-K nearest neighbors of the given vector.
     */
    public List<double[]> query(double[] vec, int topK) throws Exception {
        indexLock.readLock().lock();
        try {
            int dims = vec.length;
            DimensionContext ctx = dimensionContexts.get(dims);
            if (ctx == null) {
                throw new IllegalArgumentException("No index for dims=" + dims);
            }

            // Prepare query token
            QueryToken token = QueryGenerator.generateQueryToken(
                    vec, topK, numHashTables, ctx.getLsh(), keyManager
            );

            // Check cache first
            List<EncryptedPoint> encResults = queryCache.get(token);
            if (encResults != null) {
                logger.debug("Cache hit for {}", token);
            } else {
                // Execute encrypted LSH search directly
                SecureLSHIndex idx = ctx.getIndex();
                encResults = idx.findNearestNeighborsEncrypted(token);
                queryCache.put(token, encResults);
            }

            // Decrypt and return
            return decryptPoints(encResults);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    // Decrypt a list of encrypted points
    private List<double[]> decryptPoints(List<EncryptedPoint> eps) {
        List<double[]> res = new ArrayList<>();
        SecretKey key = keyManager.getCurrentKey();
        SecretKey prevKey = keyManager.getPreviousKey();
        List<SecretKey> prevKeys = prevKey == null
                ? Collections.emptyList()
                : Collections.singletonList(prevKey);
        for (EncryptedPoint p : eps) {
            try {
                double[] v = p.decrypt(key, prevKeys);
                if (v != null) {
                    res.add(v);
                }
            } catch (Exception e) {
                logger.error("Decrypt failed for {}", p.getId(), e);
            }
        }
        return res;
    }

    /**
     * Persist all dimension‐specific SecureLSHIndex instances.
     *
     * @param baseDir  directory under which each index will be saved
     */
    public void saveIndex(String baseDir) {
        for (Map.Entry<Integer, DimensionContext> entry : dimensionContexts.entrySet()) {
            int dims = entry.getKey();
            SecureLSHIndex idx = entry.getValue().getIndex();
            String dirForDim = baseDir + "/index_dim_" + dims;
            // ensure the directory exists
            new File(dirForDim).mkdirs();
            idx.saveIndex(dirForDim);
            logger.info("Saved index for dim {} to {}", dims, dirForDim);
        }
    }

    /**
     * Shutdown background services and persist state.
     */
    public void shutdown() {
        logger.info("Shutting down...");
        reEncryptor.shutdown();
        try { reEncryptThread.join(); } catch (InterruptedException ignored) {}
        logger.info("Total inserts: {}, avg insert time(ms): {}", operationCount.get(),
                totalInsertTimeNanos/1_000_000.0/operationCount.get());
    }

    /**
     * Entry point for demo and evaluation.
     */
    public static void main(String[] args) throws Exception {
        // === Data file paths ===
        String basePath   = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs";
        String queryPath  = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_query.fvecs";
        String truthPath  = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_groundtruth.ivecs";
        String learnPath  = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_learn.fvecs";

        logger.info("=== Initializing Forward-Secure ANN System ===");
        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                basePath, queryPath, truthPath,
                3,                  // numHashTables
                10,                 // numIntervals
                10000,              // maxOperations
                true,               // useForwardSecurity
                "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup\\metadata.ser",
                "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup\\keys.ser"
        );
        logger.info("System initialized successfully.");

        // === Step 1: Batch Insert ===
        int totalVectors = sys.dimensionContexts.values().stream()
                .mapToInt(ctx -> ctx.getBaseVectors().size())
                .sum();
        logger.info("Starting batch insertion of {} vectors...", totalVectors);
        for (Map.Entry<Integer, DimensionContext> entry : sys.dimensionContexts.entrySet()) {
            int dim = entry.getKey();
            DimensionContext ctx = entry.getValue();
            logger.info(" Inserting {} vectors for dimension {}...", ctx.getBaseVectors().size(), dim);
            for (double[] vec : ctx.getBaseVectors()) {
                sys.insert(UUID.randomUUID().toString(), vec);
            }
            logger.info(" Completed insertion for dimension {}", dim);
        }
        logger.info("Batch insertion complete.");

        // === Step 2: Sample Query ===
        int demoDim = sys.dimensionContexts.keySet().iterator().next();
        double[] demoVec = sys.dimensionContexts.get(demoDim).getBaseVectors().get(0);
        logger.info("Performing sample query (dim={}, topK=10)...", demoDim);
        List<double[]> sampleRes = sys.query(demoVec, 10);
        logger.info("Sample query returned {} results.", sampleRes.size());

        // === Step 3: Save Index ===
        String backupDir = "data/index_backup";
        logger.info("Saving encrypted indexes to {}", backupDir);
        sys.saveIndex(backupDir);
        logger.info("Indexes saved successfully.");

        // === Shutdown ===
        logger.info("Shutting down system...");
        sys.shutdown();
        logger.info("System shutdown complete.");
    }

}
