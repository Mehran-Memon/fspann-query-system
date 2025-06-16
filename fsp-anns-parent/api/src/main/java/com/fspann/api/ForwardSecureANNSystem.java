package com.fspann.api;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.MetadataManager;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryService;
import com.fspann.query.service.QueryServiceImpl;
import com.fspann.loader.DefaultDataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Facade for the Forward-Secure ANN system with local performance metrics and visualization.
 */
public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);
    private static final double DEFAULT_NOISE_SCALE = 0.01;
    private static final int DEFAULT_TOP_K = 5;
    private static final int DEFAULT_FAKE_POINT_COUNT = 100;

    private final SecureLSHIndexService indexService;
    private final QueryTokenFactory tokenFactory;
    private final QueryService queryService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final SystemConfig config;
    private final LRUCache<QueryToken, List<QueryResult>> cache;
    private final Profiler profiler;

    private long totalIndexingTime = 0;
    private long totalQueryTime = 0;
    private int indexingCount = 0;

    /**
     * @param configPath    Path to JSON/YAML config file
     * @param dataPath      Path to feature data file
     * @param keysFilePath  Path to serialized keys file for KeyManager
     * @param dimensions    Dimensionality of vectors
     */
    public ForwardSecureANNSystem(
            String configPath,
            String dataPath,
            String keysFilePath,
            int dimensions
    ) throws Exception {
        logger.info("Initializing ForwardSecureANNSystem with config={}, data={}, keys={}, dims={}",
                configPath, dataPath, keysFilePath, dimensions);

        // Load configuration
        ApiSystemConfig apiConfig = new ApiSystemConfig(configPath);
        this.config = apiConfig.getConfig();
        logger.info("Loaded system configuration");

        // Setup key management
        KeyRotationPolicy policy = new KeyRotationPolicy(
                (int) config.getOpsThreshold(),
                config.getAgeThresholdMs()
        );
        KeyManager keyManager = new KeyManager(keysFilePath);
        this.keyService = new KeyRotationServiceImpl(keyManager, policy);
        logger.info("KeyManager initialized, using keys file at {}", keysFilePath);

        // Crypto service
        this.cryptoService = new AesGcmCryptoService();
        logger.info("CryptoService (AES-GCM) ready");

        // Build core index
        int numShards = config.getNumShards();
        SecureLSHIndex coreIndex = new SecureLSHIndex(1, numShards, new EvenLSH(2, 10));
        EvenLSH lshHelper = new EvenLSH(dimensions, numShards);

        // Initialize indexService
        this.indexService = new SecureLSHIndexService(coreIndex, cryptoService, keyService, lshHelper, new MetadataManager(), 2, 10);
        logger.info("SecureLSHIndexService initialized with {} shards", numShards);

        // Cache and profiler
        this.cache = new LRUCache<>(1000);
        this.profiler = config.isProfilerEnabled() ? new Profiler() : null;
        logger.info("Cache and Profiler initialized");

        // Load and insert base data
        logger.info("Loading base data from {}", dataPath);
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dimensions);
        logger.info("Loaded {} vectors for initial indexing", vectors.size());
        for (double[] vec : vectors) {
            insert(UUID.randomUUID().toString(), vec);
        }
        logger.info("Initial data insertion complete");

        // Setup query pipeline
        EvenLSH queryLsh = new EvenLSH(dimensions, numShards);
        this.tokenFactory = new QueryTokenFactory(
                cryptoService,
                keyService,
                queryLsh,
                /* expansionRange= */ 1,
                /* numTables= */ 1
        );
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);
        logger.info("QueryService initialized and ready");
    }

    /** Insert a new vector into the index */
    public void insert(String id, double[] vector) {
        logger.debug("Inserting vector id={} into index", id);
        if (profiler != null) profiler.start("insert");
        indexService.insert(id, vector);
        if (profiler != null) {
            profiler.stop("insert");
            long duration = profiler.getTimings("insert").get(profiler.getTimings("insert").size() - 1);
            totalIndexingTime += duration;
            indexingCount++;
            logger.info("Insert complete for id={} in {} ms", id, duration / 1_000_000.0);
        }
    }

    /** Insert fake points for forward security */
    public void insertFakePoints(int numFakePoints, int dims) {
        logger.info("Inserting {} fake points", numFakePoints);
        if (profiler != null) profiler.start("insertFakePoints");
        for (int i = 0; i < numFakePoints; i++) {
            double[] fakeVector = new double[dims];
            for (int j = 0; j < dims; j++) {
                fakeVector[j] = Math.random();
            }
            String fakeId = UUID.randomUUID().toString();
            insert(fakeId, fakeVector);
        }
        if (profiler != null) profiler.stop("insertFakePoints");
    }

    /** Cloak a query vector by adding noise */
    public QueryToken cloakQuery(double[] queryVector) {
        logger.debug("Cloaking query vector={}", Arrays.toString(queryVector));
        if (profiler != null) profiler.start("cloakQuery");
        double[] cloakedQuery = new double[queryVector.length];
        Random random = new Random();
        for (int i = 0; i < queryVector.length; i++) {
            cloakedQuery[i] = queryVector[i] + (random.nextGaussian() * DEFAULT_NOISE_SCALE);
        }
        QueryToken token = tokenFactory.create(cloakedQuery, DEFAULT_TOP_K);
        if (profiler != null) profiler.stop("cloakQuery");
        return token;
    }

    /** Query the top-K nearest neighbors */
    public List<QueryResult> query(double[] queryVector, int topK) {
        logger.info("Executing query for topK={} with query vector={}", topK, Arrays.toString(queryVector));
        if (profiler != null) profiler.start("query");
        QueryToken token = tokenFactory.create(queryVector, topK); // Bypass cloaking for direct query
        List<QueryResult> cached = cache.get(token);
        if (cached != null) {
            logger.debug("Cache hit for token={}", token);
            if (profiler != null) profiler.stop("query");
            if (profiler != null) {
                long duration = profiler.getTimings("query").get(profiler.getTimings("query").size() - 1);
                totalQueryTime += duration;
                logger.info("Query completed in {} ms (cached)", duration / 1_000_000.0);
            }
            return cached;
        }
        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);
        if (profiler != null) profiler.stop("query");
        if (profiler != null) {
            long duration = profiler.getTimings("query").get(profiler.getTimings("query").size() - 1);
            totalQueryTime += duration;
            logger.info("Query completed in {} ms", duration / 1_000_000.0);
            logger.info("Query returned {} results", results.size());
            for (QueryResult r : results) {
                logger.debug("Query result: ID={} Distance={}", r.getId(), r.getDistance());
            }
            profiler.log("query");
        }
        return results;
    }

    /** Query with cloaking */
    public List<QueryResult> queryWithCloak(double[] queryVector, int topK) {
        logger.debug("Cloaked query for topK={} with query vector={}", topK, Arrays.toString(queryVector));
        QueryToken token = cloakQuery(queryVector);
        return queryService.search(token);
    }

    public int getIndexedVectorCount() {
        int count = indexService.getIndexedVectorCount();
        logger.info("Number of indexed vectors: {}", count);
        return count;
    }

    /** Clean up resources, persist state as needed */
    public void shutdown() {
        logger.info("Shutting down ForwardSecureANNSystem");
        logger.info("Total indexing time: {} ms for {} inserts", TimeUnit.NANOSECONDS.toMillis(totalIndexingTime), indexingCount);
        logger.info("Total query time: {} ms", TimeUnit.NANOSECONDS.toMillis(totalQueryTime));
        if (profiler != null) profiler.exportToCSV("profiler_metrics.csv");
    }

    /** End-to-end workflow with visualization and accuracy */
    public void runEndToEnd(String dataPath, double[] queryVector, int topK) throws Exception {
        logger.info("Running end-to-end workflow with dataPath={}, topK={}", dataPath, topK);
        if (profiler != null) profiler.start("endToEnd");

        // Load data
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, queryVector.length);
        Map<String, double[]> vectorMap = new HashMap<>();
        for (int i = 0; i < vectors.size(); i++) {
            String id = "vec" + i;
            vectorMap.put(id, vectors.get(i));
            insert(id, vectors.get(i));
        }
        logger.info("Loaded and indexed {} vectors", vectors.size());

        // Insert fake points
        insertFakePoints(DEFAULT_FAKE_POINT_COUNT, queryVector.length);

        // Perform cloaked query
        List<QueryResult> results = queryWithCloak(queryVector, topK);

        // Visualize performance
        if (profiler != null) {
            PerformanceVisualizer.visualizeTimings(profiler.getTimings("endToEnd"));
            PerformanceVisualizer.visualizeQueryResults(results);
        }

        // Compute and visualize accuracy matrix
        int[][] confusionMatrix = evaluateAccuracy(vectorMap, queryVector, results);
        PerformanceVisualizer.visualizeConfusionMatrix(confusionMatrix, topK);

        if (profiler != null) {
            profiler.log("endToEnd");
            profiler.stop("endToEnd");
        }
    }

    /** Evaluate accuracy with a confusion matrix */
    private int[][] evaluateAccuracy(Map<String, double[]> vectorMap, double[] queryVector, List<QueryResult> results) {
        int topK = Math.min(results.size(), DEFAULT_TOP_K);
        int[][] matrix = new int[topK][topK]; // Rows: predicted, Columns: actual

        // Compute true nearest neighbors
        List<String> trueNeighbors = findTrueNearestNeighbors(vectorMap, queryVector, topK);
        for (int i = 0; i < topK && i < results.size(); i++) {
            String predictedId = results.get(i).getId();
            int predictedRank = i;
            int trueRank = trueNeighbors.indexOf(predictedId);
            if (trueRank >= 0 && trueRank < topK) {
                matrix[predictedRank][trueRank]++;
            } else {
                matrix[predictedRank][topK - 1]++; // Treat as false positive
            }
        }
        return matrix;
    }

    /** Find true nearest neighbors based on Euclidean distance */
    private List<String> findTrueNearestNeighbors(Map<String, double[]> vectorMap, double[] queryVector, int topK) {
        PriorityQueue<Map.Entry<String, Double>> pq = new PriorityQueue<>(
                (a, b) -> Double.compare(a.getValue(), b.getValue())
        );
        for (Map.Entry<String, double[]> entry : vectorMap.entrySet()) {
            double dist = computeEuclideanDistance(queryVector, entry.getValue());
            pq.offer(new AbstractMap.SimpleEntry<>(entry.getKey(), dist));
            if (pq.size() > topK) pq.poll();
        }
        List<String> neighbors = new ArrayList<>();
        while (!pq.isEmpty()) {
            neighbors.add(0, pq.poll().getKey());
        }
        return neighbors;
    }

    /** Compute Euclidean distance between two vectors */
    private double computeEuclideanDistance(double[] v1, double[] v2) {
        double sum = 0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: <configPath> <dataPath> <keysFilePath> <dimensions>");
            System.exit(1);
        }

        String configFile = args[0];
        String dataPath = args[1];
        String keysFile = args[2];
        int dimensions = Integer.parseInt(args[3]);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(configFile, dataPath, keysFile, dimensions);
        double[] queryVector = new double[dimensions];
        Arrays.fill(queryVector, Math.random());
        logger.info("Generated random query vector: {}", Arrays.toString(queryVector));

        // Run end-to-end workflow with visualization
        sys.runEndToEnd(dataPath, queryVector, DEFAULT_TOP_K);

        // Additional query example
        double[] additionalQuery = new double[dimensions];
        Arrays.fill(additionalQuery, Math.random() * 2);
        logger.info("Running additional query with vector: {}", Arrays.toString(additionalQuery));
        List<QueryResult> additionalResults = sys.query(additionalQuery, DEFAULT_TOP_K);
        PerformanceVisualizer.visualizeQueryResults(additionalResults);

        sys.shutdown();
    }
}