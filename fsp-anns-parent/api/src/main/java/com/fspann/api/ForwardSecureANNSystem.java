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
import java.util.concurrent.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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
    private final EncryptedPointBuffer pointBuffer;
    private final RocksDBMetadataManager metadataManager;
    private final TopKProfiler topKProfiler;
    private int currentVersion;
    private int totalInserted = 0;
    private final int BATCH_SIZE;
    private long totalIndexingTime = 0;
    private long totalQueryTime = 0;
    private int indexingCount = 0;
    private final ExecutorService executor;

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

        SystemConfig tempConfig;
        try {
            ApiSystemConfig apiConfig = new ApiSystemConfig(sanitizePath(configPath));
            tempConfig = apiConfig.getConfig();
        } catch (IOException e) {
            logger.error("Failed to initialize configuration: {}", configPath, e);
            throw e;
        }
        this.config = tempConfig;

        Path keysPath = metadataPath.resolve("keys");
        Path pointsPath = metadataPath.resolve("points");
        Path metaDBPath = metadataPath.resolve("metadata");

        Files.createDirectories(keysPath);
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);


        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService cannot be null");
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        KeyLifeCycleService injected = cryptoService.getKeyService();
        if (injected != null) {
            this.keyService = injected;
        } else {
            KeyManager keyManager = new KeyManager(keysPath.toString());
            KeyRotationPolicy policy = new KeyRotationPolicy((int) config.getOpsThreshold(), config.getAgeThresholdMs());
            this.keyService = new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, cryptoService);
        }

        this.pointBuffer = new EncryptedPointBuffer(pointsPath.toString(), metadataManager, 500);
        this.indexService = new SecureLSHIndexService(cryptoService, keyService, metadataManager);
        if (keyService instanceof KeyRotationServiceImpl) {
            ((KeyRotationServiceImpl) keyService).setIndexService(indexService);
        }

        this.cache = new ConcurrentMapCache(config.getNumShards() * 1000);
        this.profiler = config.isProfilerEnabled() ? new MicrometerProfiler(new SimpleMeterRegistry()) : null;
        this.tokenFactory = new QueryTokenFactory(cryptoService, keyService,
                new EvenLSH(Collections.max(dimensions), config.getNumShards()),
                Math.max(1, config.getNumShards() / 4), 1);
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);
        this.topKProfiler = new TopKProfiler(metadataPath.toString());

        // Loading data...
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
        // Allow all paths in test mode (e.g., set by JUnit or Maven)
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

        ConcurrentMapCache(int maxSize) {
            this.maxSize = maxSize;
        }

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
            timestamps.entrySet().removeIf(entry -> now - entry.getValue() > CACHE_EXPIRY_MS);
        }

        private void removeOldestEntry() {
            timestamps.entrySet().stream()
                    .min(Map.Entry.comparingByValue())
                    .ifPresent(entry -> {
                        remove(entry.getKey());
                        timestamps.remove(entry.getKey());
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
        if (profiler != null) {
            logger.debug("Starting profiler for batchInsert");
            profiler.start("batchInsert");
        }

        List<List<double[]>> partitions = partitionList(vectors, BATCH_SIZE);
        List<String> allIds = Collections.synchronizedList(new ArrayList<>());

        partitions.parallelStream().forEach(batch -> {
            List<String> ids = new ArrayList<>(BATCH_SIZE);
            List<double[]> validBatch = new ArrayList<>(BATCH_SIZE);
            for (double[] vec : batch) {
                if (vec == null) {
                    logger.warn("Skipping null vector!");
                    continue;
                }
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
                synchronized (this) {
                    totalInserted += ids.size();
                    batchDurations.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStart));
                }
                allIds.addAll(ids);
                logger.debug("Batch[{}]: {} pts - {} ms", (totalInserted / BATCH_SIZE), ids.size(),
                        (System.nanoTime() - batchStart) / 1_000_000);
            }
        });

        if (allIds.isEmpty()) {
            logger.warn("batchInsert completed but no valid vectors were inserted for dim={}", dim);
        }

        if (profiler != null) {
            profiler.stop("batchInsert");
            logger.debug("Stopped profiler for batchInsert");
            List<Long> timings = profiler.getTimings("batchInsert");
            if (!timings.isEmpty()) {
                long duration = timings.getLast();
                totalIndexingTime += duration;
                indexingCount += vectors.size();
                logger.debug("batchInsert timing recorded: {} ms", duration / 1_000_000.0);
            } else {
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                logger.warn("No timings recorded for batchInsert (likely empty or all invalid data). Using fallback duration: {} ms", duration);
                totalIndexingTime += duration; // Fallback to manual timing
                indexingCount += vectors.size();
            }
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
        return totalInserted + pointBuffer.getTotalFlushedPoints();
    }

    public void insert(String id, double[] vector, int dim) {
        Objects.requireNonNull(id, "ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vector.length != dim) throw new IllegalArgumentException("Vector length must match dimension");

        if (profiler != null) profiler.start("insert");

        try {
            keyService.incrementOperation();
            List<EncryptedPoint> updatedPoints = ((KeyRotationServiceImpl) keyService).rotateIfNeededAndReturnUpdated();
            for (EncryptedPoint pt : updatedPoints) {
                indexService.updateCachedPoint(pt);
            }
            indexService.insert(id, vector);
        } catch (Exception e) {
            logger.error("Insert failed for id={}", id, e);
            throw e;
        } finally {
            if (profiler != null) {
                profiler.stop("insert");
                List<Long> timings = profiler.getTimings("insert");
                if (timings.isEmpty()) {
                    logger.warn("No timings recorded for insert of id={}", id);
                } else {
                    long duration = timings.getLast();
                    totalIndexingTime += duration;
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
        List<String> ids = new ArrayList<>(BATCH_SIZE);
        List<double[]> batch = new ArrayList<>(BATCH_SIZE);
        Random random = new Random();
        int batchCount = 0;

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
        double[] cloakedQuery = new double[queryVector.length];
        Random random = new Random();
        for (int i = 0; i < queryVector.length; i++) {
            cloakedQuery[i] = queryVector[i] + (random.nextGaussian() * noiseScale);
        }
        return tokenFactory.create(cloakedQuery, topK);
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
            logger.debug("Cache hit for query token, topK={}", topK);
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
            profiler.recordQueryMetric("Q_" + topK, ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0,
                    elapsed / 1_000_000.0, 0);
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
            logger.debug("Cache hit for cloaked query token, topK={}", topK);
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
            profiler.recordQueryMetric("Q_cloak_" + topK, ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0,
                    elapsed / 1_000_000.0, 0);
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
                double avgRatio = evals.stream().mapToDouble(QueryEvaluationResult::getRatio).average().orElse(0.0);
                double avgRecall = evals.stream().mapToDouble(QueryEvaluationResult::getRecall).average().orElse(0.0);

                if (profiler != null) {
                    profiler.recordQueryMetric("Q" + q + "_topK" + topK, serverMs, clientMs, avgRatio);
                    for (QueryEvaluationResult r : evals) {
                        profiler.recordTopKVariants(
                                "Q" + q + "_topK" + topK,
                                r.getTopKRequested(),
                                r.getRetrieved(),
                                r.getRatio(),
                                r.getRecall(),
                                r.getTimeMs()
                        );
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
        if (a.length != b.length) throw new IllegalArgumentException("Vector dimension mismatch");
        double sum = 0.0;
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

    public void flushAll() throws IOException {
        logger.info("ForwardSecureANNSystem flushAll started");
        pointBuffer.flushAll();
        indexService.flushBuffers();
        metadataManager.saveIndexVersion(currentVersion);
        logger.info("ForwardSecureANNSystem flushAll completed");
    }

    public void shutdown() {
        System.out.printf(
                "\n=== System Shutdown ===%n" +
                        "Total indexing time: %d ms%n" +
                        "Total query time: %d ms%n%n",
                TimeUnit.NANOSECONDS.toMillis(totalIndexingTime),
                TimeUnit.NANOSECONDS.toMillis(totalQueryTime)
        );
        try {
            logger.info("Shutdown sequence started");

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

            // ... Other shutdown steps ...

            if (metadataManager != null) {
                logger.info("Printing metadata summary...");
                metadataManager.printSummary();
                logger.info("Logging metadata stats...");
                metadataManager.logStats();
//                metadataManager.close();
            }

            logger.info("Performing final flushAll()");
            flushAll();

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
        Path metadataPath = Paths.get(args[5]);
        String groundtruthPath = args[6];
        int batchSize = args.length >= 8 ? Integer.parseInt(args[7]) : 10000;

        Files.createDirectories(metadataPath);

        Path keysPath = metadataPath.resolve("keys");
        Path pointsPath = metadataPath.resolve("points");
        Path metaDBPath = metadataPath.resolve("metadata");

        Files.createDirectories(keysPath);
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);

        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(metaDBPath.toString(), pointsPath.toString());

        KeyManager keyManager = new KeyManager(keysPath.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(100000, 999_999);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, null);

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
        System.out.printf("Average Recall: %.4f\n",
                sys.getProfiler().getAllQueryRatios().stream().mapToDouble(d -> d).average().orElse(0.0));
        System.out.print("Expected < 500ms/query and Recall ≈ 1.0\n");

        long start = System.currentTimeMillis();
        logger.info("Calling system.shutdown()...");
        sys.shutdown();
        logger.info("Shutdown completed in {} ms", System.currentTimeMillis() - start);
    }
}