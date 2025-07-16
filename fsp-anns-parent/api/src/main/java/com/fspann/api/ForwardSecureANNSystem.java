package com.fspann.api;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.common.IndexService;
import com.fspann.common.EncryptedPointBuffer;
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
import com.fspann.loader.IvecsLoader;
import java.util.concurrent.TimeUnit;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
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
    private final boolean verbose;
    private final List<Long> batchDurations = new ArrayList<>();
    private final EncryptedPointBuffer pointBuffer;
    private final RocksDBMetadataManager metadataManager;
    private int currentVersion;
    private int totalInserted = 0; // Optional
    private final int BATCH_SIZE;
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
            RocksDBMetadataManager metadataManager,
            CryptoService cryptoService,
            int batchSize

    ) throws Exception {
        this.verbose = verbose;
        this.BATCH_SIZE = batchSize;

        ApiSystemConfig apiConfig = new ApiSystemConfig(configPath);
        this.config = apiConfig.getConfig();

        Files.createDirectories(metadataPath);

        KeyManager keyManager = new KeyManager(keysFilePath);
        KeyRotationPolicy policy = new KeyRotationPolicy((int) config.getOpsThreshold(), config.getAgeThresholdMs());
        this.keyService = new KeyRotationServiceImpl(keyManager, policy, metadataPath.toString(), metadataManager, cryptoService);
        this.cryptoService = cryptoService;
        this.metadataManager = metadataManager;
        this.pointBuffer = new EncryptedPointBuffer(metadataPath.toString(), metadataManager);
        this.currentVersion = keyService.getCurrentVersion().getVersion();
        this.indexService = new SecureLSHIndexService(cryptoService, keyService, metadataManager);
        ((KeyRotationServiceImpl) keyService).setIndexService(indexService);
        this.cache = new LRUCache<>(10000);
        this.profiler = config.isProfilerEnabled() ? new Profiler() : null;
        this.tokenFactory = new QueryTokenFactory(cryptoService, keyService, new EvenLSH(Collections.max(dimensions), config.getNumShards()), 1, 1);
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);

        DefaultDataLoader loader = new DefaultDataLoader();
        for (int dim : dimensions) {
            List<double[]> vectors;
            int batch = 0;
            do {
                vectors = loader.loadData(dataPath, dim, BATCH_SIZE);
                if (!vectors.isEmpty()) {
                    batchInsert(vectors, dim);
                    batch++;
                }
            } while (!vectors.isEmpty());
        }
    }


    public void batchInsert(List<double[]> vectors, int dim) {
        if (profiler != null) profiler.start("batchInsert");
        long start = System.nanoTime();

        List<String> allIds = new ArrayList<>();
        List<String> ids = new ArrayList<>(BATCH_SIZE);
        List<double[]> batch = new ArrayList<>(BATCH_SIZE);

        for (int i = 0; i < vectors.size(); i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            batch.add(vectors.get(i));

            if (batch.size() == BATCH_SIZE || i == vectors.size() - 1) {
                long batchStart = System.nanoTime();
                indexService.batchInsert(ids, batch);
                totalInserted += ids.size();
                long batchEnd = System.nanoTime();
                batchDurations.add(TimeUnit.NANOSECONDS.toMillis(batchEnd - batchStart));

                logger.info("📦 Batch[{}]: {} pts - {} ms - {} MB free",
                        (totalInserted / BATCH_SIZE),
                        ids.size(),
                        (batchEnd - batchStart) / 1_000_000,
                        Runtime.getRuntime().freeMemory() / (1024 * 1024));

                allIds.addAll(ids);
                ids.clear();
                batch.clear();
            }
        }

        if (profiler != null) {
            profiler.stop("batchInsert");
            long duration = profiler.getTimings("batchInsert").getLast();
            totalIndexingTime += duration;
            indexingCount += vectors.size();
        }
    }


    public int getIndexedVectorCount() {
        return totalInserted + pointBuffer.getTotalFlushedPoints();
    }


    public void insert(String id, double[] vector, int dim) {
        if (profiler != null) profiler.start("insert");

        keyService.incrementOperation();
        List<EncryptedPoint> updatedPoints = ((KeyRotationServiceImpl) keyService).rotateIfNeededAndReturnUpdated();
        for (EncryptedPoint pt : updatedPoints) {
            indexService.updateCachedPoint(pt);
        }
        indexService.insert(id, vector);

        if (profiler != null) {
            profiler.stop("insert");
            long duration = profiler.getTimings("insert").getLast();
            totalIndexingTime += duration;
            indexingCount++;
            if (verbose) logger.info("Insert complete for id={} in {} ms", id, duration / 1_000_000.0);
        }
    }

    public void insertFakePointsInBatches(int total, int dim) {
        int remaining = total;
        List<String> ids = new ArrayList<>(BATCH_SIZE);
        List<double[]> batch = new ArrayList<>(BATCH_SIZE);

        Random random = new Random();

        while (remaining > 0) {
            ids.clear();
            batch.clear();
            int size = Math.min(BATCH_SIZE, remaining);

            for (int i = 0; i < size; i++) {
                double[] fakeVec = new double[dim];
                for (int j = 0; j < dim; j++) fakeVec[j] = random.nextDouble();
                batch.add(fakeVec);
                ids.add(UUID.randomUUID().toString());
            }

            indexService.batchInsert(ids, batch);
            remaining -= size;

            if (profiler != null) profiler.logMemory("After fake batch, remaining=" + remaining);
        }

        logger.info("✅ Inserted {} fake points for dim={}", total, dim);
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
        long start = System.nanoTime();
        QueryToken token = tokenFactory.create(queryVector, topK);
        List<QueryResult> cached = cache.get(token);
        if (cached != null) return cached;
        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);
        long elapsed = System.nanoTime() - start;
        totalQueryTime += elapsed;
        return results;
    }

    public List<QueryResult> queryWithCloak(double[] queryVector, int topK, int dim) {
        long start = System.nanoTime();
        QueryToken token = cloakQuery(queryVector, dim);
        List<QueryResult> results = queryService.search(token);
        if (verbose) logger.info("Cloaked query returned {} results", results.size());
        long elapsed = System.nanoTime() - start;
        totalQueryTime += elapsed;
        return results;
    }

    public void runEndToEnd(String dataPath, String queryPath, int topK, int dim) throws Exception {
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dim);
        batchInsert(vectors, dim);
//        insertFakePointsInBatches(DEFAULT_FAKE_POINT_COUNT, dim);

        List<double[]> queries = loader.loadData(queryPath, dim);
        List<int[]> groundTruth = new IvecsLoader().loadIndices("groundtruth.ivecs", queries.size());

        ResultWriter rw = new ResultWriter(Path.of("results_table.txt"));

        for (int q = 0; q < queries.size(); q++) {
            double[] queryVector = queries.get(q);
            long clientStart = System.nanoTime();

            List<QueryResult> results = query(queryVector, topK, dim);

            long clientEnd = System.nanoTime();
            double clientMs = (clientEnd - clientStart) / 1_000_000.0;
            long serverNs = ((QueryServiceImpl) queryService).getLastQueryDurationNs();
            double serverMs = serverNs / 1_000_000.0;

            double ratio = computeRatio(queryVector, results, vectors, groundTruth.get(q));
            if (profiler != null) {
                profiler.recordQueryMetric("q" + q, serverMs, clientMs, ratio);
            }

            rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ")",
                    new String[]{"Neighbor ID", "Distance"},
                    results.stream()
                            .map(r -> new String[]{r.getId(), String.format("%.6f", r.getDistance())})
                            .collect(Collectors.toList()));

            if (q < 3) {
                PerformanceVisualizer.visualizeQueryResults(results);
            }
        }
    }

    public IndexService getIndexService() {
        return this.indexService;
    }

    private double computeRatio(double[] query, List<QueryResult> predicted, List<double[]> vectors, int[] gtIndices) {
        if (predicted.isEmpty() || gtIndices.length == 0) return Double.MAX_VALUE;

        double ratioSum = 0.0;
        int count = Math.min(predicted.size(), gtIndices.length);

        for (int i = 0; i < count; i++) {
            double predDist = predicted.get(i).getDistance();
            double trueDist = computeDistance(query, vectors.get(gtIndices[i]));
            ratioSum += predDist / trueDist;
        }

        return ratioSum / count;
    }

    private double computeDistance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
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

    public void flushAll() throws IOException {
        logger.info("📤 ForwardSecureANNSystem flushAll started");
        pointBuffer.flushAll();             // Flush all buffered encrypted points
        indexService.flushBuffers();        // Flush any index buffers
        metadataManager.saveIndexVersion(currentVersion);  // Persist current index version
        logger.info("✅ ForwardSecureANNSystem flushAll completed");
    }

    public void shutdown() {
        try {
            System.out.printf("\n=== System Shutdown ===\nTotal indexing time: %d ms\nTotal query time: %d ms\n\n",
                    TimeUnit.NANOSECONDS.toMillis(totalIndexingTime),
                    TimeUnit.NANOSECONDS.toMillis(totalQueryTime));

            // Start the emergency timeout flush (safety guard)
            Thread shutdownGuard = new Thread(() -> {
                try {
                    Thread.sleep(10_000);  // 10-second timeout
                    System.err.println("⚠️ Shutdown taking too long, forcing flush and exit...");
                    flushAll(); // safe to call multiple times
                    System.exit(0);
                } catch (Exception ex) {
                    logger.error("Emergency flush failed", ex);
                }
            });
            shutdownGuard.setDaemon(true);
            shutdownGuard.start();

            // Normal shutdown logic
            if (indexService != null) {
                indexService.flushBuffers();
                indexService.shutdown();
            }

            if (profiler != null) {
                profiler.exportToCSV("profiler_metrics.csv");
                profiler.exportQueryMetrics("query_metrics.csv");

                if (!profiler.getAllClientQueryTimes().isEmpty()) {
                    PerformanceVisualizer.visualizeQueryLatencies(
                            profiler.getAllClientQueryTimes(),
                            profiler.getAllServerQueryTimes()
                    );
                }

                if (!profiler.getAllQueryRatios().isEmpty()) {
                    PerformanceVisualizer.visualizeRatioDistribution(
                            profiler.getAllQueryRatios()
                    );
                }
            }

            if (!batchDurations.isEmpty()) {
                System.out.println("==== Batch Duration Summary ====");
                for (int i = 0; i < batchDurations.size(); i++) {
                    System.out.printf("Batch %02d: %d ms%n", i + 1, batchDurations.get(i));
                }
            }

            System.out.printf("Total vectors flushed: %d\n", pointBuffer.getTotalFlushedPoints());
            System.out.printf("Final indexed count: %d\n", getIndexedVectorCount());

            metadataManager.printSummary();
            metadataManager.logStats();

        } catch (Exception e) {
            logger.error("Unexpected error during shutdown", e);
        } finally {
            System.gc(); // encourage cleanup
        }
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
        int batchSize = args.length >= 7 ? Integer.parseInt(args[6]) : 10000;

        Files.createDirectories(metadataPath);

        RocksDBMetadataManager metadataManager;
        try {
            metadataManager = new RocksDBMetadataManager(metadataPath.toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize RocksDBMetadataManager", e);
        }

        KeyManager keyManager = new KeyManager(keysFile);
        KeyRotationPolicy policy = new KeyRotationPolicy(100000, 999_999);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, metadataPath.toString(), metadataManager, null);

        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile, dataPath, keysFile, dimensions, metadataPath, true,
                metadataManager, cryptoService, batchSize
        );


        int dim = dimensions.get(0);

        // Time query fetches
        List<double[]> queries = new DefaultDataLoader().loadData(queryPath, dim);
        long totalQueryTimeMs = 0;
        int matchCount = 0;

        for (double[] query : queries) {
            long startTime = System.nanoTime();
            List<QueryResult> results = sys.query(query, DEFAULT_TOP_K, dim);
            long endTime = System.nanoTime();
            totalQueryTimeMs += (endTime - startTime) / 1_000_000;

            if (!results.isEmpty()) matchCount++; // Very simple recall check
        }

        double avgQueryTime = (double) totalQueryTimeMs / queries.size();
        double recallRatio = (double) matchCount / queries.size();

        System.out.print("\n==== QUERY PERFORMANCE METRICS ====\n");
        System.out.printf("Average Query Fetch Time: %.2f ms\n", avgQueryTime);
        System.out.printf("Recall Ratio (matched / total): %.4f\n", recallRatio);
        System.out.print("Expected < 1000ms/query and Recall ≈ 1.0\n");

        long start = System.currentTimeMillis();
        logger.info("Calling system.shutdown()...");
        sys.shutdown();
        logger.info("Shutdown completed in {} ms", System.currentTimeMillis() - start);
    }
}