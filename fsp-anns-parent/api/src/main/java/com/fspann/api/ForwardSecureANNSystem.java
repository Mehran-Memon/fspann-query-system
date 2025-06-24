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
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

    private final boolean verbose;

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
            Path metadataPath,
            boolean verbose
    ) throws Exception {
        this.verbose = verbose;

        logger.info("Initializing ForwardSecureANNSystem with config={}, data={}, keys={}, dims={}, metadata={}",
                configPath, dataPath, keysFilePath, dimensions, metadataPath);


        if (verbose) logger.info("Initializing ForwardSecureANNSystem with config={}, data={}, keys={}, dims={}, metadata={}",
                configPath, dataPath, keysFilePath, dimensions, metadataPath);

        // Initialize collections
        this.dimensionDataMap = new ConcurrentHashMap<>();
        this.dimensionIdMap = new ConcurrentHashMap<>();
        this.encryptionKeys = new ConcurrentHashMap<>();
        this.datasetMap = new ConcurrentHashMap<>();
        ApiSystemConfig apiConfig = new ApiSystemConfig(configPath);
        this.config = apiConfig.getConfig();
        if (verbose) logger.info("Loaded system configuration");

        KeyManager keyManager = new KeyManager(keysFilePath);
        KeyRotationPolicy policy = new KeyRotationPolicy((int) config.getOpsThreshold(), config.getAgeThresholdMs());
        Files.createDirectories(metadataPath);
        this.keyService = new KeyRotationServiceImpl(keyManager, policy, metadataPath.toString());

        this.cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, new MetadataManager());
        SecureLSHIndex coreIndex = new SecureLSHIndex(1, config.getNumShards(), new EvenLSH(2, config.getNumShards()));
        this.indexService = new SecureLSHIndexService(cryptoService, keyService, new MetadataManager());

        this.cache = new LRUCache<>(1000);
        this.profiler = config.isProfilerEnabled() ? new Profiler() : null;

        this.tokenFactory = new QueryTokenFactory(cryptoService, keyService, new EvenLSH(Collections.max(dimensions), config.getNumShards()), 1, 1);
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);

        // Load and insert base data
        DefaultDataLoader loader = new DefaultDataLoader();
        for (int dim : dimensions) {
            List<double[]> vectors = loader.loadData(dataPath, dim);
            dimensionDataMap.put(dim, vectors);
            dimensionIdMap.put(dim, new ArrayList<>());
            batchInsert(vectors, dim);
        }
    }

    /** Batch insert vectors for efficiency */
    public void batchInsert(List<double[]> vectors, int dim) {
        if (profiler != null) profiler.start("batchInsert");
        List<String> ids = new ArrayList<>();
        for (double[] vec : vectors) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            indexService.insert(id, vec);
        }
        dimensionIdMap.computeIfAbsent(dim, k -> new ArrayList<>()).addAll(ids);
        if (profiler != null) {
            profiler.stop("batchInsert");
            long duration = profiler.getTimings("batchInsert").getLast();
            totalIndexingTime += duration;
            indexingCount += vectors.size();
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
        if (profiler != null) profiler.start("insertFakePoints");
        List<double[]> fakePoints = new ArrayList<>();
        for (int i = 0; i < numFakePoints; i++) {
            double[] fakeVec = new double[dim];
            for (int j = 0; j < dim; j++) fakeVec[j] = Math.random();
            indexService.insert(UUID.randomUUID().toString(), fakeVec);
            fakePoints.add(fakeVec);
        }
        if (profiler != null) profiler.stop("insertFakePoints");
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
        if (profiler != null) profiler.start("query");
        QueryToken token = tokenFactory.create(queryVector, topK);
        List<QueryResult> cached = cache.get(token);
        if (cached != null) {
            if (profiler != null) profiler.stop("query");
            return cached;
        }
        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);
        if (profiler != null) {
            profiler.stop("query");
            long duration = profiler.getTimings("query").getLast();
            totalQueryTime += duration;
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

    public class ResultWriter {
        private final Path outputPath;

        public ResultWriter(Path outputPath) {
            this.outputPath = outputPath;
        }

        public void writeTable(String title, String[] columns, List<String[]> rows) throws IOException {
            try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                writer.write(title + "\n");
                writer.write(String.join("\t", columns) + "\n");
                for (String[] row : rows) {
                    writer.write(String.join("\t", row) + "\n");
                }
                writer.write("\n\n");
            }
        }
    }

    public void saveResults(String filename, List<QueryResult> results, int dim) throws IOException {
        ResultWriter rw = new ResultWriter(Path.of(filename));
        rw.writeTable(
                "Query Results (dim=" + dim + ")",
                new String[]{"Neighbor ID", "Distance"},
                results.stream()
                        .map(r -> new String[]{r.getId(), String.format("%.6f", r.getDistance())})
                        .collect(Collectors.toList())
        );
    }

    /** Clean up resources */
    public void shutdown() {
        System.out.printf("\n=== System Shutdown ===\nTotal indexing time: %d ms\nTotal query time: %d ms\n\n",
                TimeUnit.NANOSECONDS.toMillis(totalIndexingTime),
                TimeUnit.NANOSECONDS.toMillis(totalQueryTime));
        if (profiler != null) profiler.exportToCSV("profiler_metrics.csv");
    }

    /** End-to-end workflow with visualization and accuracy */
    public void runEndToEnd(String dataPath, double[] queryVector, int topK, int dim) throws Exception {
        if (verbose) {
            System.out.printf("\n=== Forward-Secure ANN Run ===\nDataset: %s\nDims: %d\nTopK: %d\n\n", dataPath, dim, topK);
        }

        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dim);

        batchInsert(vectors, dim);

        insertFakePoints(DEFAULT_FAKE_POINT_COUNT, dim);

        List<QueryResult> results = query(queryVector, topK, dim);

        if (verbose) {
            System.out.println("\nTop-" + topK + " results:");
            results.forEach(r -> System.out.printf("ID: %s\tDistance: %.6f\n", r.getId(), r.getDistance()));
        }

        ResultWriter rw = new ResultWriter(Path.of("results_table.txt"));
        rw.writeTable("Query Results (dim=" + dim + ")", new String[]{"Neighbor ID", "Distance"},
                results.stream()
                        .map(r -> new String[]{r.getId(), String.format("%.6f", r.getDistance())})
                        .collect(Collectors.toList()));

        PerformanceVisualizer.visualizeQueryResults(results);
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
        String metadataPath = args[4];

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(configFile, dataPath, keysFile, dimensions, Path.of(metadataPath), false);
        int dim = dimensions.get(0);

        // Run End-to-End with random query
        double[] queryVector = new double[dim];
        Arrays.fill(queryVector, Math.random());
        sys.runEndToEnd(dataPath, queryVector, DEFAULT_TOP_K, dim);

        // Additional Query
        double[] additionalQuery = new double[dim];
        Arrays.fill(additionalQuery, Math.random() * 2);
        List<QueryResult> additionalResults = sys.query(additionalQuery, DEFAULT_TOP_K, dim);

        // Save additional results
        sys.saveResults("results_table.txt", additionalResults, dim);

        sys.shutdown();
    }
}
