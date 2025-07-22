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
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.core.TopKProfiler;
import com.fspann.query.service.QueryService;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final TopKProfiler topKProfiler = new TopKProfiler();

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
        // 1) basic params
        this.verbose    = verbose;
        this.BATCH_SIZE = batchSize;

        // 2) load the JSON config
        ApiSystemConfig apiConfig = new ApiSystemConfig(configPath);
        this.config = apiConfig.getConfig();

        // 3) make sure metadata folder exists
        Files.createDirectories(metadataPath);

        // 4) wire in the exact same KeyLifeCycleService your CryptoService uses
        this.cryptoService   = cryptoService;
        this.metadataManager = metadataManager;
        KeyLifeCycleService injectedKeySvc = cryptoService.getKeyService();
        if (injectedKeySvc == null) {
            throw new IllegalStateException(
                    "CryptoService.getKeyService() must return a non-null KeyLifeCycleService"
            );
        }
        this.keyService = injectedKeySvc;

        // 5) build your encrypted‐point buffer and secure LSH index
        this.pointBuffer  = new EncryptedPointBuffer(metadataPath.toString(), metadataManager);
        this.indexService = new SecureLSHIndexService(cryptoService, keyService, metadataManager);
        if (keyService instanceof KeyRotationServiceImpl) {
            ((KeyRotationServiceImpl) keyService).setIndexService(indexService);
        }

        // 6) cache, profiler, query token factory, query service
        this.cache        = new LRUCache<>(10_000);
        this.profiler     = config.isProfilerEnabled() ? new Profiler() : null;
        this.tokenFactory = new QueryTokenFactory(
                cryptoService,
                keyService,
                new EvenLSH(Collections.max(dimensions), config.getNumShards()),
                1,  // numRowsPerBand
                1   // numBands (tune as you like)
        );
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);

        // 7) single pass loading in batches
        DefaultDataLoader loader = new DefaultDataLoader();
        for (int dim : dimensions) {
            List<double[]> batch;
            // loadData returns at most BATCH_SIZE vectors, or empty when done
            while (!(batch = loader.loadData(dataPath, dim, BATCH_SIZE)).isEmpty()) {
                batchInsert(batch, dim);
            }
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

    public void runEndToEnd(String dataPath, String queryPath, int dim, String groundtruthPath) throws Exception {
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dim);
        batchInsert(vectors, dim);

        List<double[]> queries = loader.loadData(queryPath, dim);
        GroundtruthManager groundtruth = new GroundtruthManager();
        groundtruth.load(groundtruthPath);

        ResultWriter rw = new ResultWriter(Path.of("results_table.txt"));

        for (int q = 0; q < queries.size(); q++) {
            double[] queryVec = queries.get(q);

            long clientStart = System.nanoTime();
            QueryToken token = tokenFactory.create(queryVec, DEFAULT_TOP_K);
            List<QueryEvaluationResult> evals = queryService.searchWithTopKVariants(token, q, groundtruth);
            long clientEnd = System.nanoTime();

            double clientMs = (clientEnd - clientStart) / 1_000_000.0;
            double serverMs = ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0;
            double avgRatio = evals.stream().mapToDouble(QueryEvaluationResult::getRatio).average().orElse(0.0);

            if (profiler != null)
                profiler.recordQueryMetric("Q" + q, serverMs, clientMs, avgRatio);

            for (QueryEvaluationResult r : evals) {
                profiler.recordTopKVariants(
                        "Q" + q,
                        r.getTopKRequested(),
                        r.getRetrieved(),
                        r.getRatio(),
                        r.getRecall(),
                        r.getTimeMs()
                );
            }

            rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ")",
                    new String[]{"TopK", "Retrieved", "Ratio", "Recall", "TimeMs"},
                    evals.stream()
                            .map(r -> new String[]{
                                    String.valueOf(r.getTopKRequested()),
                                    String.valueOf(r.getRetrieved()),
                                    String.format("%.4f", r.getRatio()),
                                    String.format("%.4f", r.getRecall()),
                                    String.valueOf(r.getTimeMs())
                            })
                            .collect(Collectors.toList()));
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

    public QueryService getQueryService() {
        return this.queryService;
    }

    public Profiler getProfiler() {
        return this.profiler;
    }

    public static class ResultWriter {
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
        // 1) Print summary stats
        System.out.printf(
                "\n=== System Shutdown ===%n" +
                        "Total indexing time: %d ms%n" +
                        "Total query time: %d ms%n%n",
                TimeUnit.NANOSECONDS.toMillis(totalIndexingTime),
                TimeUnit.NANOSECONDS.toMillis(totalQueryTime)
        );

        try {
            // 2) Normal shutdown sequence without forcing VM exit
            if (indexService != null) {
                indexService.flushBuffers();
                indexService.shutdown();
            }

            if (profiler != null) {
                profiler.exportToCSV("profiler_metrics.csv");
                profiler.exportQueryMetrics("query_metrics.csv");
                topKProfiler.export("topk_evaluation.csv");

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

            System.out.printf("Total vectors flushed: %d%n", pointBuffer.getTotalFlushedPoints());
            System.out.printf("Final indexed count: %d%n", getIndexedVectorCount());

            if (metadataManager != null) {
                metadataManager.printSummary();
                metadataManager.logStats();
            }

            // 3) Final flush for any remaining data
            logger.info("🔧 Performing final flushAll()");
            flushAll();

        } catch (Exception e) {
            logger.error("Unexpected error during shutdown", e);
        } finally {
            System.gc();
            logger.info("✅ Shutdown complete");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: <configPath> <dataPath> <queryPath> <keysFilePath> <dimensions> <metadataPath> <groundtruthPath>");
            System.exit(1);
        }

        String configFile = args[0];
        String dataPath = args[1];
        String queryPath = args[2];
        String keysFile = args[3];
        List<Integer> dimensions = Arrays.stream(args[4].split(",")).map(Integer::parseInt).collect(Collectors.toList());
        Path metadataPath = Path.of(args[5]);
        String groundtruthPath = args[6];
        int batchSize = args.length >= 8 ? Integer.parseInt(args[7]) : 10000;

        Files.createDirectories(metadataPath);

        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(metadataPath.toString());

        KeyManager keyManager = new KeyManager(keysFile);
        KeyRotationPolicy policy = new KeyRotationPolicy(100000, 999_999);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, metadataPath.toString(), metadataManager, null);

        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile, dataPath, keysFile, dimensions, metadataPath, false,
                metadataManager, cryptoService, batchSize
        );

        int dim = dimensions.get(0);
        sys.runEndToEnd(dataPath, queryPath, dim, groundtruthPath);

        System.out.print("\n==== QUERY PERFORMANCE METRICS ====\n");
        System.out.printf("Average Query Fetch Time: %.2f ms\n",
                sys.getProfiler().getAllClientQueryTimes().stream().mapToDouble(d -> d).average().orElse(0.0));

        System.out.printf("Recall Ratio (matched / total): %.4f\n",
                sys.getProfiler().getAllQueryRatios().stream().mapToDouble(d -> d).average().orElse(0.0));

        System.out.print("Expected < 1000ms/query and Recall ≈ 1.0\n");

        long start = System.currentTimeMillis();
        logger.info("Calling system.shutdown()...");
        sys.shutdown();
        logger.info("Shutdown completed in {} ms", System.currentTimeMillis() - start);
    }
}