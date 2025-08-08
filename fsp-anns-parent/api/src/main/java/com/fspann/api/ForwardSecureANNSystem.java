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
import com.fspann.loader.FormatLoader;
import com.fspann.loader.GroundtruthManager;
import com.fspann.loader.StreamingBatchLoader;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.core.TopKProfiler;
import com.fspann.query.service.QueryService;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);
    private static final double DEFAULT_NOISE_SCALE = 0.01;

    private final SecureLSHIndexService indexService;
    private final QueryTokenFactory tokenFactory;
    private final QueryService queryService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final SystemConfig config;
    private final ConcurrentMap<QueryToken, List<QueryResult>> cache;
    private final Profiler profiler;
    private final boolean verbose;
    private final List<Long> batchDurations = Collections.synchronizedList(new ArrayList<>());
    private final RocksDBMetadataManager metadataManager;
    private final TopKProfiler topKProfiler;

    private final int BATCH_SIZE;
    private long totalIndexingTime = 0;
    private long totalQueryTime = 0;
    private int indexingCount = 0;
    private int totalInserted = 0;

    private final ExecutorService executor;
    private boolean exitOnShutdown = false;

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
    ) throws IOException {
        Objects.requireNonNull(configPath, "Config path cannot be null");
        Objects.requireNonNull(dataPath, "Data path cannot be null");
        Objects.requireNonNull(keysFilePath, "Keys file path cannot be null");
        Objects.requireNonNull(dimensions, "Dimensions cannot be null");
        Objects.requireNonNull(metadataPath, "Metadata path cannot be null");
        if (batchSize <= 0) throw new IllegalArgumentException("Batch size must be positive");
        if (dimensions.isEmpty()) throw new IllegalArgumentException("Dimensions list cannot be empty");

        this.verbose = verbose;
        this.BATCH_SIZE = batchSize;
        this.executor = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

        // Load config early
        SystemConfig cfg;
        try {
            ApiSystemConfig apiCfg = new ApiSystemConfig(sanitizePath(configPath));
            cfg = apiCfg.getConfig();
        } catch (IOException e) {
            logger.error("Failed to initialize configuration: {}", configPath, e);
            throw e;
        }
        this.config = cfg;

        // Layout: API owns the directories
        Path keysPath   = metadataPath.resolve("keys");
        Path pointsPath = metadataPath.resolve("points");
        Path metaDBPath = metadataPath.resolve("metadata");
        Files.createDirectories(keysPath);
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);

        // Core services (injected)
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        this.cryptoService   = Objects.requireNonNull(cryptoService, "CryptoService cannot be null");

        // Key service: use the one carried by CryptoService if present, otherwise create our own
        KeyLifeCycleService ks = cryptoService.getKeyService();
        if (ks == null) {
            KeyManager keyManager = new KeyManager(keysPath.toString());
            KeyRotationPolicy policy = new KeyRotationPolicy((int) config.getOpsThreshold(), config.getAgeThresholdMs());
            ks = new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, cryptoService);
            // if CryptoService supports it, wire back
            try {
                // AesGcmCryptoService has setKeyService; if not present, this is a no-op via reflection
                cryptoService.getClass().getMethod("setKeyService", KeyLifeCycleService.class).invoke(cryptoService, ks);
            } catch (Exception ignore) { /* best-effort */ }
        }
        this.keyService = ks;

        // One shared buffer owned by API, injected into index
        EncryptedPointBuffer sharedBuffer = new EncryptedPointBuffer(pointsPath.toString(), metadataManager, 500);

        // One shared LSH instance used by BOTH index & token factory (this fixes bucket mismatches)
        int maxDim = Collections.max(dimensions);
        EvenLSH sharedLsh = new EvenLSH(maxDim, config.getNumShards());

        // Build index service with DI; pass shared LSH and shared buffer
        this.indexService = new SecureLSHIndexService(
                cryptoService,
                keyService,
                metadataManager,
                /* index */ null,
                /* lsh   */ sharedLsh,
                /* buffer*/ sharedBuffer
        );

        if (keyService instanceof KeyRotationServiceImpl kr) {
            kr.setIndexService(indexService);
        }

        this.cache = new ConcurrentMapCache(config.getNumShards() * 1000);
        this.profiler = config.isProfilerEnabled() ? new MicrometerProfiler(new SimpleMeterRegistry()) : null;

        // Token factory uses the SAME LSH
        this.tokenFactory = new QueryTokenFactory(
                cryptoService,
                keyService,
                sharedLsh,
                Math.max(1, config.getNumShards() / 4),
                /* numTables */ 1
        );

        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);
        this.topKProfiler = new TopKProfiler(metadataPath.toString());

        // Eager-load data
        DefaultDataLoader loader = new DefaultDataLoader();
        Path dataFile = Paths.get(sanitizePath(dataPath));
        for (int dim : dimensions) {
            FormatLoader fl = loader.lookup(dataFile);
            StreamingBatchLoader batchLoader = new StreamingBatchLoader(fl.openVectorIterator(dataFile), batchSize);
            List<double[]> batch;
            int batchCount = 0;
            while (!(batch = batchLoader.nextBatch()).isEmpty()) {
                logger.debug("Loaded batch {} for dim={} with {} vectors", ++batchCount, dim, batch.size());
                for (double[] vec : batch) {
                    if (vec == null || vec.length != dim) {
                        logger.warn("Invalid vector in batch {} for dim={}: {}", batchCount, dim, Arrays.toString(vec));
                    }
                }
                batchInsert(batch, dim);
            }
            if (batchCount == 0) {
                logger.warn("No batches loaded for dim={} from dataPath={}", dim, dataPath);
            }
        }
    }

    private String sanitizePath(String path) {
        if ("true".equals(System.getProperty("test.env"))) {
            return Paths.get(path).normalize().toString();
        }
        Path normalized = Paths.get(path).normalize();
        Path base = Paths.get(System.getProperty("user.dir")).normalize();
        if (!normalized.startsWith(base)) {
            logger.error("Path traversal attempt: {}", path);
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        return normalized.toString();
    }

    private static class ConcurrentMapCache extends ConcurrentHashMap<QueryToken, List<QueryResult>> {
        private final int maxSize;
        private final ConcurrentMap<QueryToken, Long> timestamps = new ConcurrentHashMap<>();
        private static final long CACHE_EXPIRY_MS = 10 * 60 * 1000;

        ConcurrentMapCache(int maxSize) { this.maxSize = maxSize; }

        @Override
        public List<QueryResult> put(QueryToken key, List<QueryResult> value) {
            cleanExpiredEntries();
            if (size() >= maxSize) {
                removeOldestEntry();
            }
            timestamps.put(key, System.currentTimeMillis());
            return super.put(key, value);
        }

        private void cleanExpiredEntries() {
            long now = System.currentTimeMillis();
            timestamps.entrySet().removeIf(e -> now - e.getValue() > CACHE_EXPIRY_MS);
        }

        private void removeOldestEntry() {
            timestamps.entrySet().stream()
                    .min(Map.Entry.comparingByValue())
                    .ifPresent(e -> {
                        remove(e.getKey());
                        timestamps.remove(e.getKey());
                    });
        }
    }

    public void batchInsert(List<double[]> vectors, int dim) {
        Objects.requireNonNull(vectors, "Vectors cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vectors.isEmpty()) {
            logger.warn("batchInsert called with empty vector list for dim={}", dim);
            return;
        }

        long start = System.nanoTime();
        if (profiler != null) profiler.start("batchInsert");

        List<List<double[]>> partitions = partitionList(vectors, BATCH_SIZE);
        List<String> allIds = new ArrayList<>();

        for (List<double[]> batch : partitions) {
            List<String> ids = new ArrayList<>(BATCH_SIZE);
            List<double[]> validBatch = new ArrayList<>(BATCH_SIZE);
            for (double[] vec : batch) {
                if (vec == null) continue;
                if (vec.length != dim) {
                    logger.warn("Skipping vector with dim {} (expected {}) - vector: {}", vec.length, dim, Arrays.toString(vec));
                    continue;
                }
                ids.add(UUID.randomUUID().toString());
                validBatch.add(vec);
            }

            if (!ids.isEmpty()) {
                long batchStart = System.nanoTime();
                indexService.batchInsert(ids, validBatch);
                totalInserted += ids.size();
                batchDurations.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStart));
                allIds.addAll(ids);
                logger.debug("Batch[{}]: {} pts - {} ms",
                        (totalInserted / BATCH_SIZE), ids.size(),
                        (System.nanoTime() - batchStart) / 1_000_000);
            }
        }

        if (allIds.isEmpty()) {
            logger.warn("batchInsert completed but no valid vectors were inserted for dim={}", dim);
        }

        if (profiler != null) {
            profiler.stop("batchInsert");
            List<Long> timings = profiler.getTimings("batchInsert");
            long durationMs = !timings.isEmpty() ? timings.getLast() : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            totalIndexingTime += TimeUnit.MILLISECONDS.toNanos(durationMs);
            indexingCount += vectors.size();
        }
    }

    private <T> List<List<T>> partitionList(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    public int getIndexedVectorCount() {
        EncryptedPointBuffer buf = indexService.getPointBuffer();
        return totalInserted + (buf != null ? buf.getTotalFlushedPoints() : 0);
    }

    public void insert(String id, double[] vector, int dim) {
        Objects.requireNonNull(id, "ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vector.length != dim) throw new IllegalArgumentException("Vector length must match dimension");

        if (profiler != null) profiler.start("insert");

        try {
            keyService.incrementOperation();

            // If we have the rotating service, get possibly updated points back to the index
            if (keyService instanceof KeyRotationServiceImpl) {
                List<EncryptedPoint> updatedPoints = ((KeyRotationServiceImpl) keyService).rotateIfNeededAndReturnUpdated();
                for (EncryptedPoint pt : updatedPoints) {
                    indexService.updateCachedPoint(pt);
                }
            } else {
                keyService.rotateIfNeeded();
            }

            indexService.insert(id, vector);
        } catch (Exception e) {
            logger.error("Insert failed for id={}", id, e);
            throw e;
        } finally {
            if (profiler != null) {
                profiler.stop("insert");
                List<Long> timings = profiler.getTimings("insert");
                if (!timings.isEmpty()) {
                    long duration = timings.getLast();
                    totalIndexingTime += TimeUnit.MILLISECONDS.toNanos(duration);
                    indexingCount++;
                    if (verbose) logger.debug("Insert complete for id={} in {} ms", id, duration / 1_000_000.0);
                }
            }
        }
    }

    public void insertFakePointsInBatches(int total, int dim) {
        if (total <= 0) throw new IllegalArgumentException("Total must be positive");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        int remaining = total;
        Random rnd = new Random();
        int batchCount = 0;

        while (remaining > 0) {
            int size = Math.min(BATCH_SIZE, remaining);
            List<String> ids = new ArrayList<>(size);
            List<double[]> batch = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                double[] v = new double[dim];
                for (int j = 0; j < dim; j++) v[j] = rnd.nextDouble();
                batch.add(v);
                ids.add(UUID.randomUUID().toString());
            }

            logger.debug("Inserting fake batch {} with {} points for dim={}", ++batchCount, size, dim);
            indexService.batchInsert(ids, batch);
            remaining -= size;

            if (profiler != null) profiler.logMemory("After fake batch, remaining=" + remaining);
        }

        logger.info("Inserted {} fake points for dim={}", total, dim);
    }

    public QueryToken cloakQuery(double[] queryVector, int dim, int topK) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");

        double noiseScale = topK <= 20 ? DEFAULT_NOISE_SCALE / 2 : DEFAULT_NOISE_SCALE;
        double[] cloaked = new double[queryVector.length];
        Random r = new Random();
        for (int i = 0; i < queryVector.length; i++) {
            cloaked[i] = queryVector[i] + (r.nextGaussian() * noiseScale);
        }
        return tokenFactory.create(cloaked, topK);
    }

    public List<QueryResult> query(double[] queryVector, int topK, int dim) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");

        long start = System.nanoTime();
        QueryToken token = tokenFactory.create(queryVector, topK);

        List<QueryResult> cached = cache.get(token);
        if (cached != null) {
            if (profiler != null) {
                profiler.recordQueryMetric("Q_cache_" + topK, 0, (System.nanoTime() - start) / 1_000_000.0, 0);
            }
            return cached;
        }

        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);

        long elapsed = System.nanoTime() - start;
        totalQueryTime += elapsed;
        if (profiler != null) {
            profiler.recordQueryMetric("Q_" + topK,
                    ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0,
                    elapsed / 1_000_000.0,
                    0);
        }
        return results;
    }

    public List<QueryResult> queryWithCloak(double[] queryVector, int topK, int dim) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");

        long start = System.nanoTime();
        QueryToken token = cloakQuery(queryVector, dim, topK);

        List<QueryResult> cached = cache.get(token);
        if (cached != null) {
            if (profiler != null) {
                profiler.recordQueryMetric("Q_cloak_cache_" + topK, 0, (System.nanoTime() - start) / 1_000_000.0, 0);
            }
            return cached;
        }

        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);

        long elapsed = System.nanoTime() - start;
        totalQueryTime += elapsed;
        if (profiler != null) {
            profiler.recordQueryMetric("Q_cloak_" + topK,
                    ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0,
                    elapsed / 1_000_000.0,
                    0);
        }
        if (verbose) logger.debug("Cloaked query returned {} results for topK={}", results.size(), topK);
        return results;
    }

    public void runEndToEnd(String dataPath, String queryPath, int dim, String groundtruthPath) throws IOException {
        Objects.requireNonNull(dataPath, "Data path cannot be null");
        Objects.requireNonNull(queryPath, "Query path cannot be null");
        Objects.requireNonNull(groundtruthPath, "Groundtruth path cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(sanitizePath(dataPath), dim);
        batchInsert(vectors, dim);

        List<double[]> queries = loader.loadData(sanitizePath(queryPath), dim);
        GroundtruthManager groundtruth = new GroundtruthManager();
        groundtruth.load(sanitizePath(groundtruthPath));

        ResultWriter rw = new ResultWriter(Paths.get("results_table.txt"));
        int[] topKValues = {1, 20, 40, 60, 80, 100};

        for (int q = 0; q < queries.size(); q++) {
            double[] queryVec = queries.get(q);

            for (int topK : topKValues) {
                long clientStart = System.nanoTime();
                QueryToken token = tokenFactory.create(queryVec, topK);
                List<QueryEvaluationResult> evals = queryService.searchWithTopKVariants(token, q, groundtruth);
                long clientEnd = System.nanoTime();

                double clientMs = (clientEnd - clientStart) / 1_000_000.0;
                double serverMs = ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0;
                double avgRatio  = evals.stream().mapToDouble(QueryEvaluationResult::getRatio).average().orElse(0.0);
                double avgRecall = evals.stream().mapToDouble(QueryEvaluationResult::getRecall).average().orElse(0.0);

                if (profiler != null) {
                    profiler.recordQueryMetric("Q" + q + "_topK" + topK, serverMs, clientMs, avgRatio);
                    for (QueryEvaluationResult r : evals) {
                        profiler.recordTopKVariants("Q" + q + "_topK" + topK,
                                r.getTopKRequested(), r.getRetrieved(), r.getRatio(), r.getRecall(), r.getTimeMs());
                    }
                }

                topKProfiler.record("Q" + q + "_topK" + topK, evals);

                rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ", topK=" + topK + ")",
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
    }

    public IndexService getIndexService() {
        return this.indexService;
    }

    public QueryService getQueryService() {
        return this.queryService;
    }

    public Profiler getProfiler() {
        return this.profiler;
    }

    /** Disable System.exit on shutdown (for tests). */
    public void setExitOnShutdown(boolean exitOnShutdown) {
        this.exitOnShutdown = exitOnShutdown;
    }

    public void flushAll() throws IOException {
        logger.info("ForwardSecureANNSystem flushAll started");
        EncryptedPointBuffer buf = indexService.getPointBuffer();
        if (buf != null) buf.flushAll();
        metadataManager.saveIndexVersion(keyService.getCurrentVersion().getVersion());
        logger.info("ForwardSecureANNSystem flushAll completed");
    }

    public void shutdown() {
        System.out.printf(
                "%n=== System Shutdown ===%nTotal indexing time: %d ms%nTotal query time: %d ms%n%n",
                TimeUnit.NANOSECONDS.toMillis(totalIndexingTime),
                TimeUnit.NANOSECONDS.toMillis(totalQueryTime)
        );
        try {
            logger.info("Shutdown sequence started");

            // Flush BEFORE closing any stores
            logger.info("Performing final flushAll()");
            flushAll();

            if (indexService != null) {
                logger.info("Flushing indexService buffers...");
                indexService.flushBuffers();
                logger.info("Shutting down indexService...");
                indexService.shutdown();
                logger.info("IndexService shutdown complete");
            }

            if (profiler != null) {
                logger.info("Exporting profiler data...");
                profiler.exportToCSV("profiler_metrics.csv");
                topKProfiler.export("topk_evaluation.csv");
                logger.info("Profiler CSVs exported");
            }

            if (metadataManager != null) {
                logger.info("Printing metadata summary...");
                metadataManager.printSummary();
                logger.info("Logging metadata stats...");
                metadataManager.logStats();
                logger.info("Closing metadataManager...");
                metadataManager.close();
                logger.info("metadataManager closed successfully.");
            }

        } catch (Exception e) {
            logger.error("Unexpected error during shutdown", e);
        } finally {
            logger.info("Shutting down executor service...");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("Executor did not terminate within timeout, forcing shutdown");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted during executor shutdown", e);
                executor.shutdownNow();
            }
            logger.info("Requesting GC cleanup...");
            System.gc();
            logger.info("Shutdown complete");
            if (exitOnShutdown && !"true".equals(System.getProperty("test.env"))) {
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: <configPath> <dataPath> <queryPath> <keysFilePath> <dimensions> <metadataPath> <groundtruthPath>");
            System.exit(1);
        }

        String configFile  = args[0];
        String dataPath    = args[1];
        String queryPath   = args[2];
        String keysFile    = args[3];
        List<Integer> dimensions = Arrays.stream(args[4].split(",")).map(Integer::parseInt).collect(Collectors.toList());
        Path metadataPath  = Paths.get(args[5]);
        String groundtruthPath = args[6];
        int batchSize = args.length >= 8 ? Integer.parseInt(args[7]) : 10000;

        Files.createDirectories(metadataPath);
        Path keysPath   = metadataPath.resolve("keys");
        Path pointsPath = metadataPath.resolve("points");
        Path metaDBPath = metadataPath.resolve("metadata");
        Files.createDirectories(keysPath);
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);

        // Use factory to avoid ctor access shenanigans
        RocksDBMetadataManager metadataManager =
                RocksDBMetadataManager.create(metaDBPath.toString(), pointsPath.toString());

        KeyManager keyManager = new KeyManager(keysPath.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(100000, 999_999);
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, null);

        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile, dataPath, keysFile, dimensions, metadataPath, false,
                metadataManager, cryptoService, batchSize
        );

        if ("true".equals(System.getProperty("disable.exit"))) {
            sys.setExitOnShutdown(false);
        }

        int dim = dimensions.get(0);
        sys.runEndToEnd(dataPath, queryPath, dim, groundtruthPath);

        System.out.print("\n==== QUERY PERFORMANCE METRICS ====\n");
        System.out.printf("Average Query Fetch Time: %.2f ms\n",
                sys.getProfiler().getAllClientQueryTimes().stream().mapToDouble(d -> d).average().orElse(0.0));
        System.out.printf("Average Recall: %.4f\n",
                sys.getProfiler().getAllQueryRatios().stream().mapToDouble(d -> d).average().orElse(0.0));
        System.out.print("Expected < 500ms/query and Recall ≈ 1.0\n");

        long start = System.currentTimeMillis();
        logger.info("Calling system.shutdown()...");
        sys.shutdown();
        logger.info("Shutdown completed in {} ms", System.currentTimeMillis() - start);
    }
}