package com.fspann.api;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
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
    private final boolean verbose;

    private long totalIndexingTime = 0;
    private long totalQueryTime = 0;
    private int indexingCount = 0;

    public ForwardSecureANNSystem(
            String configPath,
            String dataPath,
            String keysFilePath,
            List<Integer> dimensions,
            Path metadataPath,
            boolean verbose,
            MetadataManager metadataManager,
            CryptoService cryptoService
    ) throws Exception {
        this.verbose = verbose;

        logger.info("Initializing ForwardSecureANNSystem with config={}, data={}, keys={}, dims={}, metadata={}",
                configPath, dataPath, keysFilePath, dimensions, metadataPath);

        this.dimensionDataMap = new HashMap<>();
        this.dimensionIdMap = new HashMap<>();
        ApiSystemConfig apiConfig = new ApiSystemConfig(configPath);
        this.config = apiConfig.getConfig();

        Files.createDirectories(metadataPath);

        KeyManager keyManager = new KeyManager(keysFilePath);
        KeyRotationPolicy policy = new KeyRotationPolicy((int) config.getOpsThreshold(), config.getAgeThresholdMs());
        this.keyService = new KeyRotationServiceImpl(keyManager, policy, metadataPath.toString(), metadataManager, cryptoService);
        this.cryptoService = cryptoService;
        this.indexService = new SecureLSHIndexService(cryptoService, keyService, metadataManager);
        this.cache = new LRUCache<>(1000);
        this.profiler = config.isProfilerEnabled() ? new Profiler() : null;
        this.tokenFactory = new QueryTokenFactory(cryptoService, keyService, new EvenLSH(Collections.max(dimensions), config.getNumShards()), 1, 1);
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);

        DefaultDataLoader loader = new DefaultDataLoader();
        for (int dim : dimensions) {
            List<double[]> vectors = loader.loadData(dataPath, dim);
            dimensionDataMap.put(dim, vectors);
            dimensionIdMap.put(dim, new ArrayList<>());
            batchInsert(vectors, dim);
        }
    }


    public void batchInsert(List<double[]> vectors, int dim) {
        if (profiler != null) profiler.start("batchInsert");
        long start = System.nanoTime();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < vectors.size(); i++) {
            String id = UUID.randomUUID().toString();
            indexService.insert(id, vectors.get(i));
            ids.add(id);

            if ((i + 1) % 10000 == 0) {
                long elapsed = (System.nanoTime() - start) / 1_000_000;
                logger.info("[BatchInsert] Inserted {} of {} vectors... elapsed: {} ms", i + 1, vectors.size(), elapsed);
            }
        }

        dimensionIdMap.computeIfAbsent(dim, k -> new ArrayList<>()).addAll(ids);

        if (profiler != null) {
            profiler.stop("batchInsert");
            long duration = profiler.getTimings("batchInsert").getLast();
            totalIndexingTime += duration;
            indexingCount += vectors.size();
            logger.info("✅ Batch insert complete: {} vectors in {} ms", vectors.size(), TimeUnit.NANOSECONDS.toMillis(duration));
        } else {
            long totalElapsed = (System.nanoTime() - start) / 1_000_000;
            logger.info("✅ Batch insert complete: {} vectors in {} ms", vectors.size(), totalElapsed);
        }
    }

    public void insert(String id, double[] vector, int dim) {
        if (profiler != null) profiler.start("insert");

        keyService.incrementOperation();        // <-- ADD
        keyService.rotateIfNeeded();            // <-- ADD
        indexService.insert(id, vector);

        dimensionIdMap.computeIfAbsent(dim, k -> new ArrayList<>()).add(id);
        dimensionDataMap.computeIfAbsent(dim, k -> new ArrayList<>()).add(vector);

        if (profiler != null) {
            profiler.stop("insert");
            long duration = profiler.getTimings("insert").getLast();
            totalIndexingTime += duration;
            indexingCount++;
            if (verbose) logger.info("Insert complete for id={} in {} ms", id, duration / 1_000_000.0);
        }
    }

    public void insertFakePoints(int numFakePoints, int dim) {
        for (int i = 0; i < numFakePoints; i++) {
            double[] fakeVec = new double[dim];
            for (int j = 0; j < dim; j++) fakeVec[j] = Math.random();
            indexService.insert(UUID.randomUUID().toString(), fakeVec);
        }
    }

    public QueryToken cloakQuery(double[] queryVector, int dim) {
        double[] cloakedQuery = new double[queryVector.length];
        Random random = new Random();
        for (int i = 0; i < queryVector.length; i++) {
            cloakedQuery[i] = queryVector[i] + (random.nextGaussian() * DEFAULT_NOISE_SCALE);
        }
        return tokenFactory.create(cloakedQuery, DEFAULT_TOP_K);
    }

    public List<QueryResult> query(double[] queryVector, int topK, int dim) {
        QueryToken token = tokenFactory.create(queryVector, topK);
        List<QueryResult> cached = cache.get(token);
        if (cached != null) return cached;
        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);
        return results;
    }

    public List<QueryResult> queryWithCloak(double[] queryVector, int topK, int dim) {
        QueryToken token = cloakQuery(queryVector, dim);
        List<QueryResult> results = queryService.search(token);
        if (verbose) logger.info("Cloaked query returned {} results", results.size());
        return results;
    }

    public void runEndToEnd(String dataPath, String queryPath, int topK, int dim) throws Exception {
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dim);
        batchInsert(vectors, dim);
        insertFakePoints(DEFAULT_FAKE_POINT_COUNT, dim);
        List<double[]> queries = loader.loadData(queryPath, dim);
        ResultWriter rw = new ResultWriter(Path.of("results_table.txt"));

        for (int q = 0; q < queries.size(); q++) {
            double[] queryVector = queries.get(q);
            List<QueryResult> results = query(queryVector, topK, dim);

            rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ")",
                    new String[]{"Neighbor ID", "Distance"},
                    results.stream().map(r -> new String[]{r.getId(), String.format("%.6f", r.getDistance())}).collect(Collectors.toList()));

            if (q < 3) {
                PerformanceVisualizer.visualizeQueryResults(results);
            }
        }
    }

    public int getIndexedVectorCount(int dim) {
        return dimensionIdMap.getOrDefault(dim, new ArrayList<>()).size();
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

    public void shutdown() {
        System.out.printf("\n=== System Shutdown ===\nTotal indexing time: %d ms\nTotal query time: %d ms\n\n",
                TimeUnit.NANOSECONDS.toMillis(totalIndexingTime),
                TimeUnit.NANOSECONDS.toMillis(totalQueryTime));
        if (profiler != null) {
            profiler.exportToCSV("profiler_metrics.csv");
        }
        System.gc();
    }

    public CryptoService getCryptoService() {
        return this.cryptoService;
    }

    public EncryptedPoint getEncryptedPointById(String id) {
        return indexService.getEncryptedPoint(id);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: <configPath> <dataPath> <queryPath> <keysFilePath> <dimensions> <metadataPath>");
            System.exit(1);
        }

        String configFile = args[0];
        String dataPath = args[1];
        String queryPath = args[2];
        String keysFile = args[3];
        List<Integer> dimensions = Arrays.stream(args[4].split(",")).map(Integer::parseInt).collect(Collectors.toList());
        Path metadataPath = Path.of(args[5]);

        // Create shared metadata manager and crypto service
        MetadataManager metadataManager = new MetadataManager();
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), null, metadataManager);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile, dataPath, keysFile, dimensions, metadataPath, false,
                metadataManager, cryptoService
        );

        int dim = dimensions.get(0);
        sys.runEndToEnd(dataPath, queryPath, DEFAULT_TOP_K, dim);
        sys.shutdown();
    }
}

