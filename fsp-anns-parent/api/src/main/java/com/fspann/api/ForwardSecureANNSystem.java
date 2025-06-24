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
import com.fspann.loader.DefaultDataLoader;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryService;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Facade for the Forward-Secure ANN system with enhanced security, efficiency, and visualization.
 */
public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);
    private static final double DEFAULT_NOISE_SCALE = 0.01;
    private static final int DEFAULT_TOP_K = 5;
    private static final int DEFAULT_FAKE_POINT_COUNT = 100;
    private static final int BATCH_SIZE = 100;

    private final SecureLSHIndexService indexService;
    private final QueryTokenFactory tokenFactory;
    private final QueryService queryService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final SystemConfig config;
    private final LRUCache<QueryToken, List<QueryResult>> cache;
    private final Profiler profiler;
    private final Map<Integer, List<double[]>> dimensionDataMap;
    private final Map<Integer, List<String>> dimensionIdMap;
    private final Map<String, byte[]> encryptionKeys;
    private final Map<String, List<double[]>> datasetMap;

    private long totalIndexingTime = 0;
    private long totalQueryTime = 0;
    private int indexingCount = 0;

    /**
     * @param configPath    Path to JSON/YAML config file
     * @param dataPath      Path to feature data file
     * @param keysFilePath  Path to serialized keys file for KeyManager
     * @param dimensions    List of supported dimensionalities
     * @param metadataPath  Path to metadata storage for key rotation
     */
    public ForwardSecureANNSystem(
            String configPath,
            String dataPath,
            String keysFilePath,
            List<Integer> dimensions,
            Path metadataPath
    ) throws Exception {
        logger.info("Initializing ForwardSecureANNSystem with config={}, data={}, keys={}, dims={}, metadata={}",
                configPath, dataPath, keysFilePath, dimensions, metadataPath);

        // Initialize collections
        this.dimensionDataMap = new ConcurrentHashMap<>();
        this.dimensionIdMap = new ConcurrentHashMap<>();
        this.encryptionKeys = new ConcurrentHashMap<>();
        this.datasetMap = new ConcurrentHashMap<>();

        // Load configuration
        logger.debug("Loading configuration");
        ApiSystemConfig apiConfig;
        try {
            apiConfig = new ApiSystemConfig(configPath);
            this.config = apiConfig.getConfig();
            logger.info("Loaded system configuration");
        } catch (Exception e) {
            logger.error("Failed to load configuration", e);
            throw new Exception("Configuration loading failed", e);
        }

        // Setup key management with enhanced forward security
        logger.debug("Initializing KeyManager");
        KeyManager keyManager;
        try {
            keyManager = new KeyManager(keysFilePath);
            this.encryptionKeys.put("initial_key", keyManager.getCurrentVersion().getKey().getEncoded());
            logger.info("KeyManager initialized");
        } catch (Exception e) {
            logger.error("Failed to initialize KeyManager", e);
            throw new Exception("KeyManager initialization failed", e);
        }

        logger.debug("Creating KeyRotationPolicy");
        KeyRotationPolicy policy = new KeyRotationPolicy(
                (int) config.getOpsThreshold(),
                config.getAgeThresholdMs()
        );

        // Initialize keyService
        logger.debug("Initializing KeyRotationServiceImpl with metadata path {}", metadataPath);
        try {
            Files.createDirectories(metadataPath);
            this.keyService = new KeyRotationServiceImpl(keyManager, policy, metadataPath.toString());
            logger.info("KeyRotationServiceImpl initialized successfully");
            if (this.keyService == null) {
                logger.error("keyService is null after initialization");
                throw new Exception("KeyRotationServiceImpl initialization resulted in null");
            }
        } catch (Exception e) {
            logger.error("Failed to initialize KeyRotationServiceImpl with metadata path {}", metadataPath, e);
            throw new Exception("KeyRotationService initialization failed", e);
        }
        logger.info("KeyManager initialized with forward-secure key derivation, using keys file at {}", keysFilePath);

        // Crypto service with forward-secure AES-GCM
        logger.debug("Initializing CryptoService");
        try {
            this.cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, new MetadataManager());
            logger.info("CryptoService (AES-GCM with forward security) ready");
        } catch (Exception e) {
            logger.error("Failed to initialize CryptoService", e);
            throw new Exception("CryptoService initialization failed", e);
        }

        // Build core index with dynamic dimensionality support
        logger.debug("Initializing SecureLSHIndexService");
        int numShards = config.getNumShards();
        SecureLSHIndex coreIndex = new SecureLSHIndex(1, numShards, new EvenLSH(2, numShards));
        try {
            this.indexService = new SecureLSHIndexService(cryptoService, keyService, new MetadataManager());
            logger.info("SecureLSHIndexService initialized with {} shards", numShards);
        } catch (Exception e) {
            logger.error("Failed to initialize SecureLSHIndexService", e);
            throw new Exception("SecureLSHIndexService initialization failed", e);
        }

        // Cache and profiler
        logger.debug("Initializing Cache and Profiler");
        this.cache = new LRUCache<>(1000);
        this.profiler = config.isProfilerEnabled() ? new Profiler() : null;
        logger.info("Cache and Profiler initialized");

        // Setup query pipeline with dynamic dimensionality
        logger.debug("Initializing QueryTokenFactory and QueryService");
        try {
            this.tokenFactory = new QueryTokenFactory(
                    cryptoService,
                    keyService,
                    new EvenLSH(Collections.max(dimensions), numShards),
                    /* expansionRange= */ 1,
                    /* numTables= */ 1
            );
            this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);
            logger.info("QueryService initialized and ready");
        } catch (Exception e) {
            logger.error("Failed to initialize QueryTokenFactory or QueryService", e);
            throw new Exception("Query pipeline initialization failed", e);
        }

        // Load and insert base data
        logger.info("Loading base data from {}", dataPath);
        DefaultDataLoader loader = new DefaultDataLoader();
        for (int dim : dimensions) {
            try {
                List<double[]> vectors = loader.loadData(dataPath, dim);
                dimensionDataMap.put(dim, vectors);
                dimensionIdMap.put(dim, new ArrayList<>());
                logger.info("Loaded {} vectors for dimension {}", vectors.size(), dim);
                batchInsert(vectors, dim);
            } catch (Exception e) {
                logger.error("Failed to load data for dimension {}", dim, e);
                throw new Exception("Data loading failed for dimension " + dim, e);
            }
        }
        logger.info("Initial data insertion complete");
    }


    /** Batch insert vectors for efficiency */
    public void batchInsert(List<double[]> vectors, int dim) {
        logger.debug("Batch inserting {} vectors for dimension {}", vectors.size(), dim);
        if (profiler != null) profiler.start("batchInsert");
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < vectors.size(); i += BATCH_SIZE) {
            List<double[]> batch = vectors.subList(i, Math.min(i + BATCH_SIZE, vectors.size()));
            for (double[] vec : batch) {
                String id = UUID.randomUUID().toString();
                ids.add(id);
                indexService.insert(id, vec);
            }
        }
        dimensionIdMap.computeIfAbsent(dim, k -> new ArrayList<>()).addAll(ids);
        if (profiler != null) {
            profiler.stop("batchInsert");
            long duration = profiler.getTimings("batchInsert").get(profiler.getTimings("batchInsert").size() - 1);
            totalIndexingTime += duration;
            indexingCount += vectors.size();
            logger.info("Batch insert complete for {} vectors in {} ms", vectors.size(), duration / 1_000_000.0);
        }
    }

    /** Insert a single vector */
    public void insert(String id, double[] vector, int dim) {
        logger.debug("Inserting vector id={} for dimension {}", id, dim);
        if (profiler != null) profiler.start("insert");
        indexService.insert(id, vector);
        dimensionIdMap.computeIfAbsent(dim, k -> new ArrayList<>()).add(id);
        dimensionDataMap.computeIfAbsent(dim, k -> new ArrayList<>()).add(vector);
        if (profiler != null) {
            profiler.stop("insert");
            long duration = profiler.getTimings("insert").get(profiler.getTimings("insert").size() - 1);
            totalIndexingTime += duration;
            indexingCount++;
            logger.info("Insert complete for id={} in {} ms", id, duration / 1_000_000.0);
        }
    }

    /** Insert fake points for forward security */
    public void insertFakePoints(int numFakePoints, int dim) {
        logger.info("Inserting {} fake points for dimension {}", numFakePoints, dim);
        if (profiler != null) profiler.start("insertFakePoints");
        List<double[]> fakePoints = new ArrayList<>();
        for (int i = 0; i < numFakePoints; i++) {
            double[] fakeVector = new double[dim];
            for (int j = 0; j < dim; j++) {
                fakeVector[j] = Math.random();
            }
            fakePoints.add(fakeVector);
            String fakeId = UUID.randomUUID().toString();
            insert(fakeId, fakeVector, dim);
        }
        if (profiler != null) profiler.stop("insertFakePoints");
        PerformanceVisualizer.visualizeFakePoints(fakePoints, dimensionDataMap.getOrDefault(dim, new ArrayList<>()), dim);
    }

    /** Cloak a query vector by adding noise */
    public QueryToken cloakQuery(double[] queryVector, int dim) {
        logger.debug("Cloaking query vector for dimension {}", dim);
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
    public List<QueryResult> query(double[] queryVector, int topK, int dim) {
        logger.info("Executing query for topK={} and dimension {}", topK, dim);
        if (profiler != null) profiler.start("query");
        QueryToken token = tokenFactory.create(queryVector, topK);
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
            PerformanceVisualizer.visualizeKNeighbors(results, topK, dim);
        }
        return results;
    }

    /** Query with cloaking */
    public List<QueryResult> queryWithCloak(double[] queryVector, int topK, int dim) {
        logger.debug("Cloaked query for topK={} and dimension {}", topK, dim);
        QueryToken token = cloakQuery(queryVector, dim);
        List<QueryResult> results = queryService.search(token);
        logger.info("Cloaked query returned {} results", results.size());
        return results;
    }

    public int getIndexedVectorCount(int dim) {
        int count = dimensionIdMap.getOrDefault(dim, new ArrayList<>()).size();
        logger.info("Number of indexed vectors for dimension {}: {}", dim, count);
        return count;
    }

    public void addDataset(String datasetName, String dataPath, int dim) throws Exception {
        logger.info("Adding dataset '{}' from {} for dimension {}", datasetName, dataPath, dim);
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dim);
        datasetMap.put(datasetName, vectors);

        batchInsert(vectors, dim);

        PerformanceVisualizer.visualizeRawData(vectors, dim, datasetName);
        PerformanceVisualizer.visualizeIndexedData(dimensionDataMap.getOrDefault(dim, new ArrayList<>()), dim, datasetName);
    }

    /** Clean up resources */
    public void shutdown() {
        logger.info("Shutting down ForwardSecureANNSystem");
        logger.info("Total indexing time: {} ms for {} inserts", TimeUnit.NANOSECONDS.toMillis(totalIndexingTime), indexingCount);
        logger.info("Total query time: {} ms", TimeUnit.NANOSECONDS.toMillis(totalQueryTime));
        if (profiler != null) profiler.exportToCSV("profiler_metrics.csv");
    }

    /** End-to-end workflow with visualization and accuracy */
    public void runEndToEnd(String dataPath, double[] queryVector, int topK, int dim) throws Exception {
        logger.info("Running end-to-end workflow with dataPath={}, topK={}, dim={}", dataPath, topK, dim);
        if (profiler != null) profiler.start("endToEnd");

        // Load and index data
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dim);
        Map<String, double[]> vectorMap = new HashMap<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < vectors.size(); i++) {
            String id = "vec" + i;
            vectorMap.put(id, vectors.get(i));
            ids.add(id);
        }
        batchInsert(vectors, dim);
        logger.info("Loaded and indexed {} vectors", vectors.size());

        // Insert fake points
        insertFakePoints(DEFAULT_FAKE_POINT_COUNT, dim);

        // Perform cloaked query
        List<QueryResult> results = queryWithCloak(queryVector, topK, dim);

        // Visualize performance and data
        if (profiler != null) {
            PerformanceVisualizer.visualizeTimings(profiler.getTimings("endToEnd"));
            PerformanceVisualizer.visualizeQueryResults(results);
            PerformanceVisualizer.visualizeRawData(vectors, dim, "Base Dataset");
            PerformanceVisualizer.visualizeIndexedData(dimensionDataMap.getOrDefault(dim, new ArrayList<>()), dim, "Indexed Dataset");
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
        int[][] matrix = new int[topK][topK];

        List<String> trueNeighbors = findTrueNearestNeighbors(vectorMap, queryVector, topK);
        for (int i = 0; i < topK && i < results.size(); i++) {
            String predictedId = results.get(i).getId();
            int predictedRank = i;
            int trueRank = trueNeighbors.indexOf(predictedId);
            if (trueRank >= 0 && trueRank < topK) {
                matrix[predictedRank][trueRank]++;
            } else {
                matrix[predictedRank][topK - 1]++;
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
        if (args.length < 5) {
            System.err.println("Usage: <configPath> <dataPath> <keysFilePath> <dimensions> <metadataPath>");
            System.exit(1);
        }

        String configFile = args[0];
        String dataPath = args[1];
        String keysFile = args[2];
        List<Integer> dimensions = Arrays.stream(args[3].split(","))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        String metadataPath = args[4];  // The 5th argument: metadataPath

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(configFile, dataPath, keysFile, dimensions, Path.of(metadataPath));
        int dim = dimensions.get(0);
        double[] queryVector = new double[dim];
        Arrays.fill(queryVector, Math.random());
        logger.info("Generated random query vector: {}", Arrays.toString(queryVector));

        sys.runEndToEnd(dataPath, queryVector, DEFAULT_TOP_K, dim);

        double[] additionalQuery = new double[dim];
        Arrays.fill(additionalQuery, Math.random() * 2);
        logger.info("Running additional query with vector: {}", Arrays.toString(additionalQuery));
        List<QueryResult> additionalResults = sys.query(additionalQuery, DEFAULT_TOP_K, dim);
        PerformanceVisualizer.visualizeQueryResults(additionalResults);

        sys.shutdown();
    }
}
