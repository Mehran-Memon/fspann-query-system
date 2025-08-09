package com.fspann.api;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
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
    private final Map<Integer, QueryTokenFactory> tokenFactories = new ConcurrentHashMap<>();
    private final QueryService queryService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final SystemConfig config;
    private final ConcurrentMap<QueryToken, List<QueryResult>> cache;
    private final Profiler profiler;
    private final boolean verbose;
    private final RocksDBMetadataManager metadataManager;
    private final TopKProfiler topKProfiler;

    private final int BATCH_SIZE;
    private long totalIndexingTime = 0;
    private long totalQueryTime = 0;
    private final java.util.concurrent.atomic.AtomicInteger indexedCount = new java.util.concurrent.atomic.AtomicInteger();
    private int totalInserted = 0;
    private final int tokenNumTables;

    // artifact/export helpers (now aligned with FsPaths)
    private final Path metaDBPath;
    private final Path pointsPath;
    private final Path keyStorePath;
    private final java.util.List<double[]> recentQueries =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    private final ExecutorService executor;
    private boolean exitOnShutdown = false;

    public ForwardSecureANNSystem(
            String configPath,
            String /* unused in ctor now, kept for symmetry */ dataPath,
            String keysFilePath,
            List<Integer> dimensions,
            Path metadataPath,
            boolean verbose,
            RocksDBMetadataManager metadataManager,
            CryptoService cryptoService,
            int batchSize
    ) throws IOException {
        Objects.requireNonNull(configPath, "Config path cannot be null");
        Objects.requireNonNull(keysFilePath, "Keys file path cannot be null");
        Objects.requireNonNull(dimensions, "Dimensions cannot be null");
        Objects.requireNonNull(metadataPath, "Metadata path cannot be null");
        if (batchSize <= 0) throw new IllegalArgumentException("Batch size must be positive");
        if (dimensions.isEmpty()) throw new IllegalArgumentException("Dimensions list cannot be empty");

        this.verbose = verbose;
        this.BATCH_SIZE = batchSize;
        this.executor = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

        // Load config
        SystemConfig cfg;
        try {
            ApiSystemConfig apiCfg = new ApiSystemConfig(normalizePath(configPath));
            cfg = apiCfg.getConfig();
        } catch (IOException e) {
            logger.error("Failed to initialize configuration: {}", configPath, e);
            throw e;
        }
        this.config = cfg;

        // ---- Centralize paths via FsPaths, but keep test-time @TempDir compatibility ----
        // Ensure FsPaths resolves to the provided metadataPath (temp dir in tests).
        System.setProperty(FsPaths.BASE_DIR_PROP, metadataPath.toString());
        System.setProperty(FsPaths.METADB_PROP, metadataPath.resolve("metadata").toString());
        System.setProperty(FsPaths.POINTS_PROP, metadataPath.resolve("points").toString());

        // Resolve and persist fields
        this.pointsPath = FsPaths.pointsDir();
        this.metaDBPath = FsPaths.metadataDb();
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);

        // Injected core services
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        this.cryptoService   = Objects.requireNonNull(cryptoService, "CryptoService cannot be null");

        // Key service (also align FsPaths KEYSTORE_PROP)
        Path derivedKeyRoot = resolveKeyStorePath(keysFilePath, metadataPath);
        System.setProperty(FsPaths.KEYSTORE_PROP, derivedKeyRoot.toString());
        KeyLifeCycleService ks = cryptoService.getKeyService();
        if (ks == null) {
            Files.createDirectories(derivedKeyRoot.getParent());
            KeyManager keyManager = new KeyManager(derivedKeyRoot.toString());
            int opsCap = (int) Math.min(Integer.MAX_VALUE, config.getOpsThreshold());
            KeyRotationPolicy policy = new KeyRotationPolicy(opsCap, config.getAgeThresholdMs());
            ks = new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, cryptoService);
            try {
                cryptoService.getClass().getMethod("setKeyService", KeyLifeCycleService.class).invoke(cryptoService, ks);
            } catch (Exception ignore) { /* no-op */ }
        }
        this.keyService = ks;

        // Capture actual keystore path for export
        Path ksp = derivedKeyRoot;
        try {
            if (keyService instanceof KeyRotationServiceImpl kr) {
                try {
                    var m = kr.getClass().getMethod("getKeyManager");
                    Object km = m.invoke(kr);
                    var mp = km.getClass().getMethod("getStorePath");
                    String p = (String) mp.invoke(km);
                    if (p != null && !p.isBlank()) ksp = Paths.get(p);
                } catch (Throwable ignore) { /* fallback remains derivedKeyRoot */ }
            }
        } catch (Throwable ignore) {}
        this.keyStorePath = ksp;

        // Build index service
        this.indexService = SecureLSHIndexService.fromConfig(cryptoService, keyService, metadataManager, config);

        // per-token tables (fallback to 4 if index doesn't expose it)
        int tmpTables = 4;
        try {
            // If indexService exposes number of tables, assign here.
            // tmpTables = indexService.getNumTables();
        } catch (Throwable ignore) {}
        this.tokenNumTables = tmpTables;

        if (keyService instanceof KeyRotationServiceImpl kr) {
            kr.setIndexService(indexService);
        }

        this.cache = new ConcurrentMapCache(config.getNumShards() * 1000);
        this.profiler = config.isProfilerEnabled() ? new MicrometerProfiler(new SimpleMeterRegistry()) : null;

        // Token factories by dimension
        for (int dim : dimensions) {
            EvenLSH lshForDim = indexService.getLshForDimension(dim);
            tokenFactories.put(dim, new QueryTokenFactory(
                    cryptoService, keyService, lshForDim,
                    Math.max(1, config.getNumShards() / 4),
                    tokenNumTables
            ));
        }

        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);
        this.topKProfiler = new TopKProfiler(metadataPath.toString());
    }

        /* ---------------------- Indexing API ---------------------- */

    public void indexStream(String dataPath, int dim) throws IOException {
        Objects.requireNonNull(dataPath, "Data path cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        DefaultDataLoader loader = new DefaultDataLoader();
        Path dataFile = Paths.get(normalizePath(dataPath));
        FormatLoader fl = loader.lookup(dataFile);

        StreamingBatchLoader batchLoader = new StreamingBatchLoader(fl.openVectorIterator(dataFile), BATCH_SIZE);
        List<double[]> batch;
        int batchCount = 0;

        while (!(batch = batchLoader.nextBatch()).isEmpty()) {
            logger.debug("Loaded batch {} for dim={} with {} vectors", ++batchCount, dim, batch.size());
            batchInsert(batch, dim);
        }

        if (batchCount == 0) {
            logger.warn("No batches loaded for dim={} from dataPath={}", dim, dataPath);
        }
    }

    public void indexAllDimensions(String dataPath, List<Integer> dimensions) throws IOException {
        for (int dim : dimensions) indexStream(dataPath, dim);
    }

    public void batchInsert(List<double[]> vectors, int dim) {
        Objects.requireNonNull(vectors, "Vectors cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vectors.isEmpty()) return;

        long startNs = System.nanoTime();
        if (profiler != null) profiler.start("batchInsert");

        for (int i = 0; i < vectors.size(); i += BATCH_SIZE) {
            keyService.rotateIfNeeded();
            List<double[]> slice = vectors.subList(i, Math.min(i + BATCH_SIZE, vectors.size()));
            List<String> ids = new ArrayList<>(slice.size());
            List<double[]> valid = new ArrayList<>(slice.size());
            for (double[] v : slice) {
                if (v == null) continue;
                if (v.length != dim) {
                    logger.warn("Skipping vector dim={} (expected {})", (v != null ? v.length : -1), dim);
                    continue;
                }
                ids.add(UUID.randomUUID().toString());
                valid.add(v);
            }
            if (!valid.isEmpty()) {
                indexService.batchInsert(ids, valid);
                indexedCount.addAndGet(valid.size());
                totalInserted += valid.size();
            }
        }

        if (profiler != null) {
            profiler.stop("batchInsert");
            List<Long> timings = profiler.getTimings("batchInsert");
            long durationNs = !timings.isEmpty() ? timings.getLast() : (System.nanoTime() - startNs);
            totalIndexingTime += durationNs;
        }
    }

    public int getIndexedVectorCount() {
        return indexedCount.get();
    }

    public void insert(String id, double[] vector, int dim) {
        Objects.requireNonNull(id, "ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vector.length != dim) throw new IllegalArgumentException("Vector length must match dimension");

        if (profiler != null) profiler.start("insert");
        try {
            // REMOVE this line: keyService.incrementOperation();

            if (keyService instanceof KeyRotationServiceImpl) {
                List<EncryptedPoint> updated = ((KeyRotationServiceImpl) keyService).rotateIfNeededAndReturnUpdated();
                for (EncryptedPoint pt : updated) indexService.updateCachedPoint(pt);
            } else {
                keyService.rotateIfNeeded();
            }

            indexService.insert(id, vector); // index layer will increment ops
            indexedCount.incrementAndGet();
        } finally {
            if (profiler != null) {
                profiler.stop("insert");
                List<Long> timings = profiler.getTimings("insert");
                if (!timings.isEmpty()) {
                    long durationNs = timings.getLast();
                    totalIndexingTime += durationNs;
                    if (verbose) logger.debug("Insert complete for id={} in {} ms", id, durationNs / 1_000_000.0);
                }
            }
        }
    }

    /* ---------------------- Query API ---------------------- */

    private QueryTokenFactory factoryForDim(int dim) {
        return tokenFactories.computeIfAbsent(dim, d -> {
            EvenLSH lsh = indexService.getLshForDimension(d);
            return new QueryTokenFactory(cryptoService, keyService, lsh,
                    Math.max(1, config.getNumShards() / 4),
                    tokenNumTables
            );
        });
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
        return factoryForDim(dim).create(cloaked, topK);
    }

    public List<QueryResult> query(double[] queryVector, int topK, int dim) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");

        long start = System.nanoTime();
        QueryToken token = factoryForDim(dim).create(queryVector, topK);

        // record plaintext query (for artifact export) — safe for tests/local runs
        recordRecent(queryVector);

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

        // record pre-cloak version; you could also store the cloaked vector if you prefer
        recordRecent(queryVector);

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

    /* ---------------------- E2E Runner ---------------------- */

    public void runEndToEnd(String dataPath, String queryPath, int dim, String groundtruthPath) throws IOException {
        Objects.requireNonNull(dataPath, "Data path cannot be null");
        Objects.requireNonNull(queryPath, "Query path cannot be null");
        Objects.requireNonNull(groundtruthPath, "Groundtruth path cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        // Index (streaming) then evaluate
        indexStream(dataPath, dim);

        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> queries = loader.loadData(normalizePath(queryPath), dim);
        GroundtruthManager groundtruth = new GroundtruthManager();
        groundtruth.load(normalizePath(groundtruthPath));

        ResultWriter rw = new ResultWriter(Paths.get("results", "results_table.txt"));
        int[] topKValues = {1, 20, 40, 60, 80, 100};

        for (int q = 0; q < queries.size(); q++) {
            double[] queryVec = queries.get(q);

            for (int topK : topKValues) {
                long clientStart = System.nanoTime();
                QueryToken token = factoryForDim(dim).create(queryVec, topK);
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

    /* ---------------------- Utilities & Lifecycle ---------------------- */

    /** Generate and insert `total` synthetic points for the given dimension. */
    public void insertFakePointsInBatches(int total, int dim) {
        if (total <= 0) throw new IllegalArgumentException("Total must be positive");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        java.util.Random rnd = new java.util.Random();
        int remaining = total;

        while (remaining > 0) {
            int size = Math.min(BATCH_SIZE, remaining);
            java.util.List<String> ids = new java.util.ArrayList<>(size);
            java.util.List<double[]> batch = new java.util.ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                double[] v = new double[dim];
                for (int j = 0; j < dim; j++) v[j] = rnd.nextDouble();
                batch.add(v);
                ids.add(java.util.UUID.randomUUID().toString());
            }

            indexService.batchInsert(ids, batch);
            indexedCount.addAndGet(size);
            remaining -= size;
        }
    }

    private static Path resolveKeyStorePath(String keysFilePath, Path metadataBase) {
        Path p = Paths.get(keysFilePath);
        return p.isAbsolute() ? p.normalize() : metadataBase.resolve(p).normalize();
    }

    private static String normalizePath(String path) {
        return Paths.get(path).normalize().toString();
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

    private void recordRecent(double[] v) {
        double[] copy = v.clone();
        synchronized (recentQueries) {
            if (recentQueries.size() >= 1000) recentQueries.remove(0);
            recentQueries.add(copy);
        }
    }

    /** Dump profiler CSVs, query list, and copies of keys/metadata/points to outDir. */
    public void exportArtifacts(Path outDir) throws IOException {
        Files.createDirectories(outDir);

        // 1) timers and query rows
        if (profiler != null) {
            profiler.exportToCSV(outDir.resolve("profiler_metrics.csv").toString());
            if (profiler instanceof MicrometerProfiler mp) {
                mp.exportQueryMetricsCSV(outDir.resolve("query_metrics.csv").toString());
            }
            if (profiler instanceof MicrometerProfiler mp) {
                mp.exportMetersCSV(outDir.resolve("micrometer_meters.csv").toString());
            }
            var client = profiler.getAllClientQueryTimes();
            var ratio  = profiler.getAllQueryRatios();
            double art = client.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double avgRatio = ratio.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            Files.writeString(outDir.resolve("metrics_summary.txt"),
                    String.format("ART(ms)=%.3f%nAvgRatio=%.6f%n", art, avgRatio));
        }

        // 2) Top-K evaluation (if used)
        try { topKProfiler.export(outDir.resolve("topk_evaluation.csv").toString()); }
        catch (Exception ignore) { /* ok if not used */ }

        // 3) dump recent queries
        if (!recentQueries.isEmpty()) {
            var p = outDir.resolve("queries.csv");
            try (var w = Files.newBufferedWriter(p)) {
                for (double[] q : recentQueries) {
                    for (int i = 0; i < q.length; i++) {
                        if (i > 0) w.write(",");
                        w.write(Double.toString(q[i]));
                    }
                    w.write("\n");
                }
            }
        }

        // 4) Copy keystore + metadata directories
        tryCopyPath(keyStorePath, outDir.resolve("keys"));
        tryCopyPath(pointsPath, outDir.resolve("points"));
        tryCopyPath(metaDBPath, outDir.resolve("metadata"));
    }

    private void tryCopyPath(Path src, Path dst) {
        try {
            if (src == null || !Files.exists(src)) return;

            if (Files.isDirectory(src)) {
                Files.walk(src).forEach(sp -> {
                    try {
                        Path rp = dst.resolve(src.relativize(sp).toString());
                        if (Files.isDirectory(sp)) Files.createDirectories(rp);
                        else { Files.createDirectories(rp.getParent()); Files.copy(sp, rp, StandardCopyOption.REPLACE_EXISTING); }
                    } catch (IOException e) { logger.warn("Copy failed for {}", sp, e); }
                });
            } else {
                // src is a single file: copy into dst directory
                Path dstDir = dst;
                if (!Files.exists(dstDir) || !Files.isDirectory(dstDir)) {
                    Files.createDirectories(dstDir);
                }
                Path target = dstDir.resolve(src.getFileName());
                Files.copy(src, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception e) {
            logger.warn("Failed copying {} → {}", src, dst, e);
        }
    }

    public IndexService getIndexService() { return this.indexService; }
    public QueryService getQueryService() { return this.queryService; }
    public Profiler getProfiler() { return this.profiler; }

    /** Disable System.exit on shutdown (for tests). */
    public void setExitOnShutdown(boolean exitOnShutdown) { this.exitOnShutdown = exitOnShutdown; }

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
            logger.info("Performing final flushAll()");
            flushAll();

            if (indexService != null) {
                logger.info("Flushing indexService buffers...");
                indexService.flushBuffers();
                logger.info("Shutting down indexService...");
                indexService.shutdown();
                logger.info("IndexService shutdown complete");
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
            System.err.println("Usage: <configPath> <dataPath> <queryPath> <keysFilePath> <dimensions> <metadataPath> <groundtruthPath> [batchSize]");
            System.exit(1);
        }

        String configFile  = args[0];
        String dataPath    = args[1];
        String queryPath   = args[2];
        String keysFile    = args[3];
        List<Integer> dimensions = Arrays.stream(args[4].split(",")).map(Integer::parseInt).collect(Collectors.toList());
        Path metadataPath  = Paths.get(args[5]);
        String groundtruthPath = args[6];
        int batchSize = args.length >= 8 ? Integer.parseInt(args[7]) : 100_000;

        Files.createDirectories(metadataPath);
        Path pointsPath = metadataPath.resolve("points");
        Path metaDBPath = metadataPath.resolve("metadata");
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);

        RocksDBMetadataManager metadataManager =
                RocksDBMetadataManager.create(metaDBPath.toString(), pointsPath.toString());

        // AesGcmCryptoService wiring via KeyRotationService
        Path resolvedKeyStore = resolveKeyStorePath(keysFile, metadataPath);
        Files.createDirectories(resolvedKeyStore.getParent());
        KeyManager keyManager = new KeyManager(resolvedKeyStore.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(100_000, 999_999);
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, null);

        CryptoService cryptoService = new com.fspann.crypto.AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
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

        sys.exportArtifacts(Paths.get("results"));

        if (sys.getProfiler() != null) {
            System.out.print("\n==== QUERY PERFORMANCE METRICS ====\n");
            System.out.printf("Average Client Query Time: %.2f ms\n",
                    sys.getProfiler().getAllClientQueryTimes().stream().mapToDouble(d -> d).average().orElse(0.0));
            System.out.printf("Average Ratio: %.4f\n",
                    sys.getProfiler().getAllQueryRatios().stream().mapToDouble(d -> d).average().orElse(0.0));
            System.out.print("Expected < 500ms/query and Recall ≈ 1.0\n");
        }

        long start = System.currentTimeMillis();
        logger.info("Calling system.shutdown()...");
        sys.shutdown();
        logger.info("Shutdown completed in {} ms", System.currentTimeMillis() - start);
    }
}
