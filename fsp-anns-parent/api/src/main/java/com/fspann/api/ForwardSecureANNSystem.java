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
import com.fspann.common.KeyLifeCycleService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.nio.file.StandardOpenOption;

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
    private volatile boolean queryOnlyMode = false;
    private BaseVectorReader baseReader = null;

    // Config-driven toggles (replacing scattered -D flags)
    private final boolean computePrecision;
    private final boolean writeGlobalRecallCsv;
    private final int[] K_VARIANTS;
    private final boolean auditEnable;
    private final int auditK;
    private final int auditSampleEvery;
    private final int auditWorstKeep;
    private final Path resultsDir;
    private final double configuredNoiseScale;

    enum RatioSource { AUTO, GT, BASE }
    private final RatioSource ratioSource;

    static final class TrueNN { final int id; final double d; TrueNN(int id, double d){this.id=id; this.d=d;} }

    private final int BATCH_SIZE;
    private long totalIndexingTime = 0;
    private long totalQueryTime = 0;
    private int totalInserted = 0;
    private final java.util.concurrent.atomic.AtomicInteger indexedCount = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicLong fileOrdinal = new java.util.concurrent.atomic.AtomicLong(0);

    // Legacy override hooks (still honored if set), otherwise use config.getLsh()
    private final int OVERRIDE_TABLES;
    private final int OVERRIDE_ROWS;
    private final int OVERRIDE_PROBE;

    // artifact/export helpers (aligned with FsPaths)
    private final Path metaDBPath;
    private final Path pointsPath;
    private final Path keyStorePath;
    private final java.util.List<double[]> recentQueries =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    private final ExecutorService executor;
    private boolean exitOnShutdown = false;

    // remember previous FsPaths props to restore on shutdown
    private final String prevBaseProp;
    private final String prevMetaProp;
    private final String prevPointsProp;
    private final String prevKeyStoreProp;

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

        // Load config (JSON/YAML)
        SystemConfig cfg;
        try {
            ApiSystemConfig apiCfg = new ApiSystemConfig(normalizePath(configPath));
            cfg = apiCfg.getConfig();
        } catch (IOException e) {
            logger.error("Failed to initialize configuration: {}", configPath, e);
            throw e;
        }
        this.config = cfg;

        // ---- Materialize commonly used settings from config ----
        this.computePrecision     = propOr(
                config.getEval().computePrecision,
                "eval.computePrecision", "computePrecision");

        this.writeGlobalRecallCsv = propOr(
                config.getEval().writeGlobalRecallCsv,
                "eval.writeGlobalRecallCsv", "eval.writeGlobalRecall", "writeGlobalRecallCsv");

        this.K_VARIANTS          = (config.getEval().kVariants != null && config.getEval().kVariants.length > 0)
                ? config.getEval().kVariants.clone()
                : new int[]{1,20,40,60,80,100};
        this.auditEnable         = config.getAudit().enable;
        this.auditK              = config.getAudit().k;
        this.auditSampleEvery    = config.getAudit().sampleEvery;
        this.auditWorstKeep      = config.getAudit().worstKeep;
        String resultsDirStr = (config.getOutput() != null && config.getOutput().resultsDir != null && !config.getOutput().resultsDir.isBlank())
                ? config.getOutput().resultsDir
                : System.getProperty("results.dir", "results");
        this.resultsDir = Paths.get(resultsDirStr);
        this.configuredNoiseScale= Math.max(0.0, config.getCloak().noise);

        // ratio source
        String rs = (config.getRatio().source == null ? "auto" : config.getRatio().source).toLowerCase(Locale.ROOT);
        this.ratioSource = switch (rs) {
            case "gt"   -> RatioSource.GT;
            case "base" -> RatioSource.BASE;
            default     -> RatioSource.AUTO;
        };

        // legacy override hooks (fallback if config.lsh not set)
        this.OVERRIDE_TABLES = (config.getLsh().numTables > 0)
                ? config.getLsh().numTables
                : Integer.getInteger("lsh.tables", -1);
        this.OVERRIDE_ROWS = (config.getLsh().rowsPerBand > 0)
                ? config.getLsh().rowsPerBand
                : Integer.getInteger("lsh.rowsPerBand", -1);
        this.OVERRIDE_PROBE = (config.getLsh().probeShards > 0)
                ? config.getLsh().probeShards
                : Integer.getInteger("probe.shards", -1);

        try { Files.createDirectories(resultsDir); } catch (IOException ioe) {
            logger.warn("Could not create resultsDir {}; falling back to CWD", resultsDir, ioe);
        }

        // ---- Centralize paths via FsPaths, but keep test-time @TempDir compatibility ----
        this.prevBaseProp   = System.getProperty(FsPaths.BASE_DIR_PROP);
        this.prevMetaProp   = System.getProperty(FsPaths.METADB_PROP);
        this.prevPointsProp = System.getProperty(FsPaths.POINTS_PROP);
        System.setProperty(FsPaths.BASE_DIR_PROP,  metadataPath.toString());
        System.setProperty(FsPaths.METADB_PROP,    metadataPath.resolve("metadata").toString());
        System.setProperty(FsPaths.POINTS_PROP,    metadataPath.resolve("points").toString());

        this.pointsPath = FsPaths.pointsDir();
        this.metaDBPath = FsPaths.metadataDb();
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);

        // Injected core services
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        this.cryptoService   = Objects.requireNonNull(cryptoService, "CryptoService cannot be null");

        // Key service (also align FsPaths KEYSTORE_PROP)
        Path derivedKeyRoot = resolveKeyStorePath(keysFilePath, metadataPath);
        this.prevKeyStoreProp = System.getProperty(FsPaths.KEYSTORE_PROP);
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

        // Capture an actual keystore path for export
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

        // Optional paper engine (config-driven)
        if (config.getPaper().enabled) {
            int m        = config.getPaper().m;
            int lambda   = config.getPaper().lambda;
            int divisions= config.getPaper().divisions;
            long seed    = config.getPaper().seed;

            com.fspann.index.paper.PartitionedIndexService paperEngine =
                    new com.fspann.index.paper.PartitionedIndexService(m, lambda, divisions, seed);
            indexService.setPaperEngine(paperEngine);
            logger.info("Paper engine enabled (m={}, λ={}, ℓ={}, seed={})", m, lambda, divisions, seed);
        }

        if (keyService instanceof KeyRotationServiceImpl kr) {
            kr.setIndexService(indexService);
        }

        this.cache = new ConcurrentMapCache(config.getNumShards() * 1000);
        this.profiler = config.isProfilerEnabled() ? new MicrometerProfiler(new SimpleMeterRegistry()) : null;

        // Token factories by dimension (apply LSH overrides if present)
        for (int dim : dimensions) {
            EvenLSH lshForDim = indexService.getLshForDimension(dim);

            // m override (rows per band / hash funcs per table)
            if (OVERRIDE_ROWS > 0) {
                try {
                    lshForDim.getClass().getMethod("setRowsPerBand", int.class).invoke(lshForDim, OVERRIDE_ROWS);
                } catch (Throwable ignore) {
                    try {
                        lshForDim.getClass().getMethod("setNumHashFuncsPerTable", int.class).invoke(lshForDim, OVERRIDE_ROWS);
                    } catch (Throwable ignored2) { /* optional */ }
                }
            }

            // tables override
            int tables = (OVERRIDE_TABLES > 0) ? OVERRIDE_TABLES : numTablesFor(lshForDim, config);

            // probe-shards override
            int shards = shardsToProbe();

            tokenFactories.put(dim, new QueryTokenFactory(
                    cryptoService, keyService, lshForDim, shards, tables
            ));
            logger.info("TokenFactory created: dim={} tables={} shardsToProbe={} (m override: {})",
                    dim, tables, shards, (OVERRIDE_ROWS > 0 ? OVERRIDE_ROWS : "default"));
        }

        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);

        String resultsRoot = resultsDir.toString();
        this.topKProfiler = new TopKProfiler(resultsRoot);

        String basePathProp = System.getProperty("base.path", "").trim();
        if (!basePathProp.isEmpty()) {
            Path basePath = Paths.get(basePathProp);
            boolean isBvecs = basePath.toString().toLowerCase(Locale.ROOT).endsWith(".bvecs");
            int dimProbe = dimensions.get(0); // expected fixed dim for this run
            try {
                this.baseReader = BaseVectorReader.open(basePath, dimProbe, isBvecs);
                logger.info("BaseVectorReader mapped: {} (dim={}, type={})", basePath, dimProbe, (isBvecs ? "bvecs" : "fvecs"));
            } catch (IOException ioe) {
                logger.warn("Failed to map base.path={}, distance-ratio will be skipped", basePath, ioe);
            }
        } else {
            logger.info("No -Dbase.path provided; distance-ratio will be skipped.");
        }
    }

    /* ---------------------- Indexing API ---------------------- */
    public void indexStream(String dataPath, int dim) throws IOException {
        fileOrdinal.set(0);

        if ("POINTS_ONLY".equalsIgnoreCase(dataPath)) {
            logger.info("Query-only mode: skipping indexing");
            return;
        }

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

        logger.info("Indexing completed for dim={} : indexedInThisRun={}, totalIndexedCounter={}, bufferSize={}, flushedTotal={}",
                dim,
                totalInserted,
                getIndexedVectorCount(),
                (indexService.getPointBuffer() != null ? indexService.getPointBuffer().getBufferSize() : -1),
                (indexService.getPointBuffer() != null ? indexService.getPointBuffer().getTotalFlushedPoints() : -1));
    }

    public void batchInsert(List<double[]> vectors, int dim) {
        Objects.requireNonNull(vectors, "Vectors cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vectors.isEmpty()) return;

        long startNs = System.nanoTime();
        if (profiler != null) profiler.start("batchInsert");

        for (int offset = 0; offset < vectors.size(); offset += BATCH_SIZE) {
            List<double[]> slice = vectors.subList(offset, Math.min(offset + BATCH_SIZE, vectors.size()));

            // Keep mapping from ORIGINAL RECORD INDEX → vector (skip invalid but never re-pack ordinals)
            List<double[]> valid = new ArrayList<>(slice.size());
            List<String>   ids   = new ArrayList<>(slice.size());

            for (int j = 0; j < slice.size(); j++) {
                double[] v = slice.get(j);
                long ord = fileOrdinal.getAndIncrement(); // <-- original file position for this record

                if (v == null) {
                    logger.warn("Skipping null vector at ordinal={}", ord);
                    continue;
                }
                if (v.length != dim) {
                    logger.warn("Skipping vector at ordinal={} with dim={} (expected {})", ord, v.length, dim);
                    continue;
                }
                // Use the ORIGINAL ordinal as the ID so it matches ground-truth indices
                ids.add(Long.toString(ord));
                valid.add(v);
            }

            if (valid.isEmpty()) continue;

            keyService.rotateIfNeeded();

            indexService.batchInsert(ids, valid);
            indexedCount.addAndGet(valid.size());
            totalInserted += valid.size();
        }

        if (profiler != null) {
            profiler.stop("batchInsert");
            List<Long> timings = profiler.getTimings("batchInsert");
            long durationNs = (!timings.isEmpty() ? timings.get(timings.size() - 1) : (System.nanoTime() - startNs));
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
                    long durationNs = timings.get(timings.size() - 1);
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
            int tables = (OVERRIDE_TABLES > 0) ? OVERRIDE_TABLES : numTablesFor(lsh, config);
            int shards = shardsToProbe();
            logger.info("TokenFactory (lazy): dim={} tables={} shardsToProbe={}", d, tables, shards);
            return new QueryTokenFactory(cryptoService, keyService, lsh, shards, tables);
        });
    }

    public QueryToken cloakQuery(double[] queryVector, int dim, int topK) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");

        // If config sets noise > 0 use it; else retain old adaptive default
        double noiseScale = (configuredNoiseScale > 0.0)
                ? configuredNoiseScale
                : (topK <= 20 ? DEFAULT_NOISE_SCALE / 2 : DEFAULT_NOISE_SCALE);

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

    // === compute distance-ratio@K; returns NaN if we can't compute
    /** Distance-ratio at K: (best retrieved distance) / (true NN distance). */
    private double distanceRatioAtK(double[] q,
                                    List<QueryResult> retrievedTopK,
                                    int[] groundtruthAtK,
                                    int k) {
        if (baseReader == null) return Double.NaN;
        if (q == null || retrievedTopK == null || retrievedTopK.isEmpty()
                || groundtruthAtK == null || groundtruthAtK.length == 0 || k <= 0) {
            return Double.NaN;
        }

        final int trueIdx = groundtruthAtK[0];
        final double denom = baseReader.l2(q, trueIdx);
        final double eps = 1e-12;
        if (Double.isNaN(denom)) return Double.NaN;
        if (denom <= eps) return 1.0;

        double num = Double.POSITIVE_INFINITY;
        String bestId = "NA";
        final int upto = Math.min(k, retrievedTopK.size());
        for (int i = 0; i < upto; i++) {
            String id = retrievedTopK.get(i).getId();
            int baseIdx;
            try { baseIdx = Integer.parseInt(id); } catch (NumberFormatException nfe) { continue; }
            double d = baseReader.l2(q, baseIdx);
            if (!Double.isNaN(d) && d < num) { num = d; bestId = id; }
        }
        if (!Double.isFinite(num)) return Double.NaN;

        double ratio = num / denom;
        if (ratio + 1e-12 < 1.0) {
            logger.warn("Distance ratio < 1.0 ({}). Possible ID/GT misalignment. " +
                            "Denom(trueNN)={}, Num(bestRetrieved)={}, k={}, trueIdx={}, bestRetrievedId={}",
                    String.format(Locale.ROOT, "%.6f", ratio),
                    denom, num, k, trueIdx, bestId);
        }
        return ratio;
    }

    /** Literature ratio@K = best(returned K) / trueNN(base). Does not rely on GT. */
    private double distanceRatioFromBase(double[] q, List<QueryResult> retrievedTopK, int k) {
        if (baseReader == null || q == null || retrievedTopK == null || retrievedTopK.isEmpty() || k <= 0) {
            return Double.NaN;
        }

        final double eps = 1e-12;

        double denom = Double.POSITIVE_INFINITY;
        int    trueIdx = -1;
        final int N = baseReader.count;
        for (int i = 0; i < N; i++) {
            double d = baseReader.l2(q, i);
            if (!Double.isNaN(d) && d < denom) {
                denom = d;
                trueIdx = i;
            }
        }
        if (Double.isNaN(denom) || !Double.isFinite(denom)) return Double.NaN;
        if (denom <= eps) return 1.0;

        double num = Double.POSITIVE_INFINITY;
        String bestId = "NA";
        final int upto = Math.min(k, retrievedTopK.size());
        for (int i = 0; i < upto; i++) {
            String id = retrievedTopK.get(i).getId();
            int baseIdx;
            try { baseIdx = Integer.parseInt(id); } catch (NumberFormatException nfe) { continue; }
            double d = baseReader.l2(q, baseIdx);
            if (!Double.isNaN(d) && d < num) { num = d; bestId = id; }
        }
        if (!Double.isFinite(num)) return Double.NaN;

        double ratio = num / denom;
        if (ratio + 1e-12 < 1.0) {
            logger.warn("Distance ratio < 1.0 ({}). This should not happen with base-scan. " +
                            "Denom(trueNN)={}, Num(bestRetrieved)={}, k={}, trueIdx={}, bestRetrievedId={}",
                    String.format(Locale.ROOT, "%.6f", ratio),
                    denom, num, k, trueIdx, bestId);
        }
        return ratio;
    }

    // ---------------------- E2E Runner ----------------------
    public void runEndToEnd(String dataPath, String queryPath, int dim, String groundtruthPath) throws IOException {
        Objects.requireNonNull(dataPath, "Data path cannot be null");
        Objects.requireNonNull(queryPath, "Query path cannot be null");
        Objects.requireNonNull(groundtruthPath, "Groundtruth path cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        // 1) Index the corpus
        indexStream(dataPath, dim);

        // 2) Seal for search
        try {
            indexService.flushBuffers();
            finalizeForSearch();
        } catch (Exception e) {
            logger.warn("Pre-query flush/optimize failed; continuing", e);
        }

        // 3) Load queries and ground-truth
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> queries = new ArrayList<>();
        while (true) {
            List<double[]> batch = loader.loadData(normalizePath(queryPath), dim);
            if (batch == null || batch.isEmpty()) break;
            queries.addAll(batch);
        }
        logger.info("Loaded {} queries (dim={})", queries.size(), dim);

        GroundtruthManager groundtruth = new GroundtruthManager();
        groundtruth.load(normalizePath(groundtruthPath));

        if (!queries.isEmpty()) {
            Map<Integer, Double> fanout = indexService.evaluateFanoutRatio(
                    queries.get(0), K_VARIANTS);
            logger.info("Candidate fanout ratios for Q0: {}", fanout);
        }

        if (groundtruth.size() != queries.size()) {
            logger.warn("Groundtruth rows ({}) != number of queries ({}). " +
                            "Ratios/recall may be unreliable if orders differ.",
                    groundtruth.size(), queries.size());
        }

        int datasetSize = (baseReader != null)
                ? baseReader.count
                : Math.max(indexService.getIndexedVectorCount(), totalInserted);

        groundtruth.normalizeIndexBaseIfNeeded(datasetSize);
        boolean idsOk   = groundtruth.isConsistentWithDatasetSize(datasetSize);
        boolean dimsOk  = (baseReader != null) && sampleDimsOk(baseReader);
        boolean sampleOk = (baseReader != null) &&
                sampleGtMatchesTrueNN(queries, groundtruth, baseReader,
                        Math.max(0, config.getRatio().gtSample),
                        Math.max(0.0, Math.min(1.0, config.getRatio().gtMismatchTolerance)));

        boolean gtTrusted = (baseReader != null) && idsOk && dimsOk && sampleOk;
        logger.info("Ratio trust gate: gtTrusted={}, idsOk={}, dimsOk={}, sampleOk={}", gtTrusted, idsOk, dimsOk, sampleOk);
        if (baseReader == null) {
            logger.warn("No baseReader: ratio computation will be NaN unless you set -Dbase.path");
        }

        // 4) Writer & audit helpers
        ResultWriter rw = new ResultWriter(resultsDir.resolve("results_table.txt"));
        final int baseKForToken = Math.max(auditK, 100);
        final RetrievedAudit audit = auditEnable ? new RetrievedAudit(resultsDir) : null;
        PriorityQueue<Worst> worstPQ = new PriorityQueue<>(Comparator.comparingDouble(Worst::ratio).reversed());

        Map<Integer, Long> globalMatches = new HashMap<>();
        Map<Integer, Long> globalTruth   = new HashMap<>();
        for (int k : K_VARIANTS) { globalMatches.put(k, 0L); globalTruth.put(k, 0L); }

        int indexedNow = indexService.getIndexedVectorCount();
        if (indexedNow <= 0) {
            logger.warn("No points in memory before querying! Check indexing/logs or restoreFromDisk. Continuing for diagnostics.");
        }
        int dimCountMem = indexService.getVectorCountForDimension(dim);
        if (dimCountMem <= 0) {
            logger.warn("No points registered for dim={} before querying. Continuing anyway.", dim);
        }

        // 5) Query loop
        for (int q = 0; q < queries.size(); q++) {
            double[] queryVec = queries.get(q);
            QueryToken baseToken = factoryForDim(dim).create(queryVec, baseKForToken);

            // Choose denominator once per query
            double denom = Double.NaN;
            int trueIdx = -1;
            if (baseReader != null) {
                switch (ratioSource) {
                    case GT -> {
                        int[] gt1 = groundtruth.getGroundtruth(q, 1);
                        if (gt1.length > 0) {
                            trueIdx = gt1[0];
                            denom = baseReader.l2(queryVec, trueIdx);
                        }
                    }
                    case BASE -> {
                        TrueNN dn = computeTrueNNFromBase(queryVec, baseReader);
                        denom = dn.d; trueIdx = dn.id;
                    }
                    case AUTO -> {
                        if (gtTrusted) {
                            int[] gt1 = groundtruth.getGroundtruth(q, 1);
                            if (gt1.length > 0) {
                                trueIdx = gt1[0];
                                denom = baseReader.l2(queryVec, trueIdx);
                            }
                        } else {
                            TrueNN dn = computeTrueNNFromBase(queryVec, baseReader);
                            denom = dn.d; trueIdx = dn.id;
                        }
                    }
                }
            }

            long clientStart = System.nanoTime();
            List<QueryEvaluationResult> evals =
                    queryService.searchWithTopKVariants(baseToken, q, groundtruth);
            long clientEnd = System.nanoTime();
            double clientMs = (clientEnd - clientStart) / 1_000_000.0;
            double serverMs = ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0;

            EncryptedPointBuffer buf = indexService.getPointBuffer();
            long insertTimeMs   = buf != null ? buf.getLastBatchInsertTimeMs() : 0;
            int  totalFlushed   = buf != null ? buf.getTotalFlushedPoints()   : 0;
            int  flushThreshold = buf != null ? buf.getFlushThreshold()        : 0;
            int  tokenSizeBytes = QueryServiceImpl.estimateTokenSizeBytes(baseToken);
            int  vectorDim      = baseToken.getDimension();

            List<QueryEvaluationResult> enriched = new ArrayList<>(evals.size());
            for (QueryEvaluationResult r : evals) {
                int k = r.getTopKRequested();
                QueryToken tk = factoryForDim(dim).derive(baseToken, k);
                List<QueryResult> retrieved = queryService.search(tk);  // cached by impl

                int[] truth = groundtruth.getGroundtruth(q, k); // for precision only
                double distRatio = (baseReader == null)
                        ? Double.NaN
                        : ratioGivenDenom(denom, queryVec, retrieved, k, baseReader);

                if (profiler != null && computePrecision) {
                    Set<String> truthSet = Arrays.stream(truth).mapToObj(String::valueOf).collect(Collectors.toSet());
                    long matches = retrieved.stream().map(QueryResult::getId).filter(truthSet::contains).count();
                    double precision = (k == 0) ? Double.NaN : (matches / (double) k);
                    if (!Double.isNaN(precision)) {
                        profiler.recordQueryMetric("PrecisionAt" + k, serverMs, clientMs, precision);
                    }
                }

                enriched.add(new QueryEvaluationResult(
                        k, r.getRetrieved(),
                        distRatio,
                        r.getRecall(),  // carries precision@K from QueryServiceImpl
                        r.getTimeMs(), insertTimeMs, r.getCandidateCount(),
                        tokenSizeBytes, vectorDim, totalFlushed, flushThreshold
                ));
            }

            QueryServiceImpl qs = (QueryServiceImpl) queryService;
            topKProfiler.record("Q" + q, enriched,
                    qs.getLastCandTotal(),
                    qs.getLastCandKeptVersion(),
                    qs.getLastCandDecrypted(),
                    qs.getLastReturned());

            rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ")",
                    new String[]{"TopK","Retrieved","LiteratureRatio","Precision","TimeMs",
                            "InsertTimeMs","Candidates","TokenSize","VectorDim","TotalFlushed","FlushThreshold"},
                    enriched.stream().map(r -> new String[]{
                            String.valueOf(r.getTopKRequested()),
                            String.valueOf(r.getRetrieved()),
                            (Double.isNaN(r.getRatio()) ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getRatio())),
                            String.format(Locale.ROOT, "%.4f", r.getRecall()),
                            String.valueOf(r.getTimeMs()),
                            String.valueOf(r.getInsertTimeMs()),
                            String.valueOf(r.getCandidateCount()),
                            String.valueOf(r.getTokenSizeBytes()),
                            String.valueOf(r.getVectorDim()),
                            String.valueOf(r.getTotalFlushedPoints()),
                            String.valueOf(r.getFlushThreshold())
                    }).collect(Collectors.toList())
            );

            if (computePrecision) {
                for (int k : K_VARIANTS) {
                    QueryToken tk = factoryForDim(dim).derive(baseToken, k);
                    List<QueryResult> retrieved = queryService.search(tk);
                    int[] truth = groundtruth.getGroundtruth(q, k);
                    Set<String> truthSet = Arrays.stream(truth).mapToObj(String::valueOf).collect(Collectors.toSet());
                    long matches = retrieved.stream().map(QueryResult::getId).filter(truthSet::contains).count();
                    globalMatches.put(k, globalMatches.get(k) + matches);
                    globalTruth.put(k,   globalTruth.get(k)   + truth.length);
                }
            }

            QueryEvaluationResult atK = enriched.stream()
                    .filter(e -> e.getTopKRequested() == auditK)
                    .findFirst()
                    .orElseGet(() -> enriched.get(enriched.size() - 1));

            if (profiler != null) profiler.recordQueryMetric("Q" + q, serverMs, clientMs, atK.getRatio());
            totalQueryTime += (long)(clientMs * 1_000_000L);
        }

        if (writeGlobalRecallCsv && computePrecision) {
            writeGlobalRecallCsv(resultsDir, dim, K_VARIANTS, globalMatches, globalTruth);
        }
    }

    public void runQueries(String queryPath, int dim, String groundtruthPath) throws IOException {
        Objects.requireNonNull(queryPath, "Query path cannot be null");
        Objects.requireNonNull(groundtruthPath, "Groundtruth path cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        try {
            indexService.flushBuffers();
            finalizeForSearch();
        } catch (Exception e) {
            logger.warn("Pre-query flush/optimize failed; continuing", e);
        }

        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> queries = new ArrayList<>();
        while (true) {
            List<double[]> batch = loader.loadData(normalizePath(queryPath), dim);
            if (batch == null || batch.isEmpty()) break;
            queries.addAll(batch);
        }
        logger.info("Loaded {} queries (dim={})", queries.size(), dim);

        GroundtruthManager groundtruth = new GroundtruthManager();
        groundtruth.load(normalizePath(groundtruthPath));

        if (groundtruth.size() != queries.size()) {
            logger.warn("Groundtruth rows ({}) != number of queries ({}). " +
                            "Ratios/recall may be unreliable if orders differ.",
                    groundtruth.size(), queries.size());
        }

        int datasetSize = (baseReader != null)
                ? baseReader.count
                : Math.max(indexService.getIndexedVectorCount(), totalInserted);

        groundtruth.normalizeIndexBaseIfNeeded(datasetSize);
        boolean idsOk   = groundtruth.isConsistentWithDatasetSize(datasetSize);
        boolean dimsOk  = (baseReader != null) && sampleDimsOk(baseReader);
        boolean sampleOk = (baseReader != null) &&
                sampleGtMatchesTrueNN(queries, groundtruth, baseReader,
                        Math.max(0, config.getRatio().gtSample),
                        Math.max(0.0, Math.min(1.0, config.getRatio().gtMismatchTolerance)));

        boolean gtTrusted = (baseReader != null) && idsOk && dimsOk && sampleOk;
        logger.info("Ratio trust gate: gtTrusted={}, idsOk={}, dimsOk={}, sampleOk={}", gtTrusted, idsOk, dimsOk, sampleOk);
        if (baseReader == null) {
            logger.warn("No baseReader: ratio computation will be NaN unless you set -Dbase.path");
        }

        ResultWriter rw = new ResultWriter(resultsDir.resolve("results_table.txt"));
        final int baseKForToken = Math.max(auditK, 100);
        final RetrievedAudit audit = auditEnable ? new RetrievedAudit(resultsDir) : null;
        PriorityQueue<Worst> worstPQ = new PriorityQueue<>(Comparator.comparingDouble(Worst::ratio).reversed());

        Map<Integer, Long> globalMatches = new HashMap<>();
        Map<Integer, Long> globalTruth   = new HashMap<>();
        for (int k : K_VARIANTS) { globalMatches.put(k, 0L); globalTruth.put(k, 0L); }

        int indexedNow = indexService.getIndexedVectorCount();
        if (indexedNow <= 0) {
            logger.warn("No points in memory before querying! Check indexing/logs or restoreFromDisk. Continuing for diagnostics.");
        }
        int dimCountMem = indexService.getVectorCountForDimension(dim);
        if (dimCountMem <= 0) {
            logger.warn("No points registered for dim={} before querying. Continuing anyway.", dim);
        }

        for (int q = 0; q < queries.size(); q++) {
            double[] queryVec = queries.get(q);
            QueryToken baseToken = factoryForDim(dim).create(queryVec, baseKForToken);

            double denom = Double.NaN;
            int trueIdx = -1;
            if (baseReader != null) {
                switch (ratioSource) {
                    case GT -> {
                        int[] gt1 = groundtruth.getGroundtruth(q, 1);
                        if (gt1.length > 0) {
                            trueIdx = gt1[0];
                            denom = baseReader.l2(queryVec, trueIdx);
                        }
                    }
                    case BASE -> {
                        TrueNN dn = computeTrueNNFromBase(queryVec, baseReader);
                        denom = dn.d; trueIdx = dn.id;
                    }
                    case AUTO -> {
                        if (gtTrusted) {
                            int[] gt1 = groundtruth.getGroundtruth(q, 1);
                            if (gt1.length > 0) {
                                trueIdx = gt1[0];
                                denom = baseReader.l2(queryVec, trueIdx);
                            }
                        } else {
                            TrueNN dn = computeTrueNNFromBase(queryVec, baseReader);
                            denom = dn.d; trueIdx = dn.id;
                        }
                    }
                }
            }

            long clientStart = System.nanoTime();
            List<QueryEvaluationResult> evals =
                    queryService.searchWithTopKVariants(baseToken, q, groundtruth);
            long clientEnd = System.nanoTime();
            double clientMs = (clientEnd - clientStart) / 1_000_000.0;
            double serverMs = ((QueryServiceImpl) queryService).getLastQueryDurationNs() / 1_000_000.0;

            EncryptedPointBuffer buf = indexService.getPointBuffer();
            long insertTimeMs   = buf != null ? buf.getLastBatchInsertTimeMs() : 0;
            int  totalFlushed   = buf != null ? buf.getTotalFlushedPoints()   : 0;
            int  flushThreshold = buf != null ? buf.getFlushThreshold()        : 0;
            int  tokenSizeBytes = QueryServiceImpl.estimateTokenSizeBytes(baseToken);
            int  vectorDim      = baseToken.getDimension();

            List<QueryEvaluationResult> enriched = new ArrayList<>(evals.size());
            for (QueryEvaluationResult r : evals) {
                int k = r.getTopKRequested();
                QueryToken tk = factoryForDim(dim).derive(baseToken, k);
                List<QueryResult> retrieved = queryService.search(tk);

                int[] truth = groundtruth.getGroundtruth(q, k); // for precision only
                double distRatio = (baseReader == null)
                        ? Double.NaN
                        : ratioGivenDenom(denom, queryVec, retrieved, k, baseReader);

                if (profiler != null && computePrecision) {
                    Set<String> truthSet = Arrays.stream(truth).mapToObj(String::valueOf).collect(Collectors.toSet());
                    long matches = retrieved.stream().map(QueryResult::getId).filter(truthSet::contains).count();
                    double precision = (k == 0) ? Double.NaN : (matches / (double) k);
                    if (!Double.isNaN(precision)) {
                        profiler.recordQueryMetric("PrecisionAt" + k, serverMs, clientMs, precision);
                    }
                }

                enriched.add(new QueryEvaluationResult(
                        k, r.getRetrieved(),
                        distRatio,
                        r.getRecall(),
                        r.getTimeMs(), insertTimeMs, r.getCandidateCount(),
                        tokenSizeBytes, vectorDim, totalFlushed, flushThreshold
                ));
            }

            QueryServiceImpl qs = (QueryServiceImpl) queryService;
            topKProfiler.record("Q" + q, enriched,
                    qs.getLastCandTotal(),
                    qs.getLastCandKeptVersion(),
                    qs.getLastCandDecrypted(),
                    qs.getLastReturned());

            rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ")",
                    new String[]{"TopK","Retrieved","LiteratureRatio","Precision","TimeMs",
                            "InsertTimeMs","Candidates","TokenSize","VectorDim","TotalFlushed","FlushThreshold"},
                    enriched.stream().map(r -> new String[]{
                            String.valueOf(r.getTopKRequested()),
                            String.valueOf(r.getRetrieved()),
                            (Double.isNaN(r.getRatio()) ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getRatio())),
                            String.format(Locale.ROOT, "%.4f", r.getRecall()),
                            String.valueOf(r.getTimeMs()),
                            String.valueOf(r.getInsertTimeMs()),
                            String.valueOf(r.getCandidateCount()),
                            String.valueOf(r.getTokenSizeBytes()),
                            String.valueOf(r.getVectorDim()),
                            String.valueOf(r.getTotalFlushedPoints()),
                            String.valueOf(r.getFlushThreshold())
                    }).collect(Collectors.toList())
            );

            if (computePrecision) {
                for (int k : K_VARIANTS) {
                    QueryToken tk = factoryForDim(dim).derive(baseToken, k);
                    List<QueryResult> retrieved = queryService.search(tk);
                    int[] truth = groundtruth.getGroundtruth(q, k);
                    Set<String> truthSet = Arrays.stream(truth).mapToObj(String::valueOf).collect(Collectors.toSet());
                    long matches = retrieved.stream().map(QueryResult::getId).filter(truthSet::contains).count();
                    globalMatches.put(k, globalMatches.get(k) + matches);
                    globalTruth.put(k,   globalTruth.get(k)   + truth.length);
                }
            }

            QueryEvaluationResult atK = enriched.stream()
                    .filter(e -> e.getTopKRequested() == auditK)
                    .findFirst()
                    .orElseGet(() -> enriched.get(enriched.size() - 1));

            if (profiler != null) profiler.recordQueryMetric("Q" + q, serverMs, clientMs, atK.getRatio());
            totalQueryTime += (long)(clientMs * 1_000_000L);
        }

        if (writeGlobalRecallCsv && computePrecision) {
            writeGlobalRecallCsv(resultsDir, dim, K_VARIANTS, globalMatches, globalTruth);
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

    public int restoreIndexFromDisk(int version) throws IOException {
        logger.info("Restoring in-memory index from metadata for v{} ...", version);
        int restored = 0;
        boolean prevWT = indexService.isWriteThrough();
        indexService.setWriteThrough(false);
        try {
            for (EncryptedPoint ep : metadataManager.getAllEncryptedPoints()) {
                if (ep == null || ep.getVersion() != version) continue;
                indexService.addPointToIndexOnly(ep); // <-- direct in-memory add
                restored++;
            }
        } finally {
            indexService.setWriteThrough(prevWT);
        }
        logger.info("Restore complete: {} points", restored);
        return restored;
    }

    static int detectLatestVersion(Path pointsRoot) throws IOException {
        try (var s = java.nio.file.Files.list(pointsRoot)) {
            OptionalInt latest = s.filter(java.nio.file.Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .filter(n -> n.startsWith("v"))
                    .map(n -> n.substring(1))
                    .filter(str -> str.matches("\\d+"))
                    .mapToInt(Integer::parseInt)
                    .max();

            return latest.orElse(-1);
        }
    }

    private static Path resolveKeyStorePath(String keysFilePath, Path metadataBase) {
        Path p = Paths.get(keysFilePath);
        return p.isAbsolute() ? p.normalize() : metadataBase.resolve(p).normalize();
    }

    private static String normalizePath(String path) {
        return Paths.get(path).normalize().toString();
    }

    private static String fmt4(double v) {
        return Double.isNaN(v) ? "-" : String.format(Locale.ROOT, "%.4f", v);
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

        @Override
        public List<QueryResult> get(Object key) {
            List<QueryResult> v = super.get(key);
            if (v != null) {
                timestamps.put((QueryToken) key, System.currentTimeMillis());
            }
            cleanExpiredEntries();
            return v;
        }

        private void cleanExpiredEntries() {
            long now = System.currentTimeMillis();
            timestamps.entrySet().removeIf(e -> {
                boolean expired = now - e.getValue() > CACHE_EXPIRY_MS;
                if (expired) {
                    super.remove(e.getKey());
                }
                return expired;
            });
        }

        private void removeOldestEntry() {
            timestamps.entrySet().stream()
                    .min(Map.Entry.comparingByValue())
                    .ifPresent(e -> {
                        super.remove(e.getKey());
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

    public void setQueryOnlyMode(boolean b) { this.queryOnlyMode = b; }

    public void finalizeForSearch() {
        try {
            var m = indexService.getClass().getMethod("finalizeForSearch");
            m.invoke(indexService);
            logger.info("IndexService finalized for search");
            return;
        } catch (NoSuchMethodException ignore) {
            // fall through
        } catch (Exception e) {
            logger.warn("Error calling finalizeForSearch()", e);
        }
        try {
            var opt = indexService.getClass().getMethod("optimize");
            opt.invoke(indexService);
            logger.info("IndexService optimized for search");
        } catch (NoSuchMethodException ignore) {
            logger.info("No finalize/optimize hook on indexService; continuing");
        } catch (Exception e) {
            logger.warn("Error calling optimize()", e);
        }
    }

    public static final class Worst {
        private final int qIndex;
        private final double ratio;

        public Worst(int qIndex, double ratio) {
            this.qIndex = qIndex;
            this.ratio = ratio;
        }

        public int qIndex() { return qIndex; }
        public double ratio() { return ratio; }
    }

    // lightweight, zero-copy random-access reader
    static final class BaseVectorReader implements AutoCloseable {
        private final FileChannel ch;
        private final MappedByteBuffer map;
        private final boolean bvecs;
        private final int dim;
        private final int recordBytes;
        private final boolean streaming;
        private final ThreadLocal<ByteBuffer> tlBuf;
        private final int count;

        static BaseVectorReader open(Path path, int dim, boolean bvecs) throws IOException {
            FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
            long size = ch.size();
            int rec = 4 + (bvecs ? dim : dim * 4);
            int cnt = (int) (size / rec);

            if (size <= (long) Integer.MAX_VALUE) {
                MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_ONLY, 0, size).load();
                map.order(ByteOrder.LITTLE_ENDIAN);
                return new BaseVectorReader(ch, map, bvecs, dim, rec, cnt, false);
            } else {
                return new BaseVectorReader(ch, null, bvecs, dim, rec, cnt, true);
            }
        }

        private BaseVectorReader(FileChannel ch, MappedByteBuffer map, boolean bvecs, int dim,
                                 int rec, int cnt, boolean streaming) {
            this.ch = ch;
            this.map = map;
            this.bvecs = bvecs;
            this.dim = dim;
            this.recordBytes = rec;
            this.count = cnt;
            this.streaming = streaming;
            this.tlBuf = streaming
                    ? ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(recordBytes).order(ByteOrder.LITTLE_ENDIAN))
                    : null;
        }

        double l2(double[] q, int id) {
            final int skipDimBytes = 4;
            if (!streaming) {
                int offset = id * recordBytes;
                int pos = offset + skipDimBytes;
                double sum = 0.0;
                if (bvecs) {
                    for (int i = 0; i < dim; i++) {
                        int ui = map.get(pos + i) & 0xFF;
                        double d = q[i] - ui;
                        sum += d * d;
                    }
                } else {
                    for (int i = 0; i < dim; i++) {
                        float v = map.getFloat(pos + i * 4);
                        double d = q[i] - v;
                        sum += d * d;
                    }
                }
                return Math.sqrt(sum);
            } else {
                try {
                    long offset = (long) id * (long) recordBytes;
                    ByteBuffer buf = tlBuf.get();
                    buf.clear();
                    int n = ch.read(buf, offset);
                    if (n < recordBytes) throw new IOException("Short read at id=" + id + " bytes=" + n);
                    buf.flip();
                    buf.getInt(); // skip stored dim
                    double sum = 0.0;
                    if (bvecs) {
                        for (int i = 0; i < dim; i++) {
                            int ui = buf.get() & 0xFF;
                            double d = q[i] - ui;
                            sum += d * d;
                        }
                    } else {
                        for (int i = 0; i < dim; i++) {
                            float v = buf.getFloat();
                            double d = q[i] - v;
                            sum += d * d;
                        }
                    }
                    return Math.sqrt(sum);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
        }

        int storedDim(int id) {
            final int off = id * recordBytes;
            if (!streaming) {
                return map.getInt(off);
            } else {
                try {
                    ByteBuffer buf = tlBuf.get();
                    buf.clear();
                    int n = ch.read(buf, (long) off);
                    if (n < 4) throw new IOException("Short read (dim) at id=" + id);
                    buf.flip();
                    return buf.getInt();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        boolean storedDimOk(int id) { return storedDim(id) == this.dim; }

        @Override
        public void close() throws IOException {
            ch.close();
        }
    }

    /** Dump profiler CSVs, query list, and copies of keys/metadata/points to outDir. */
    public void exportArtifacts(Path outDir) throws IOException {
        Files.createDirectories(outDir);

        if (profiler != null) {
            if (!config.getOutput().suppressLegacyMetrics) {
                profiler.exportToCSV(outDir.resolve("profiler_metrics.csv").toString());
                if (profiler instanceof MicrometerProfiler mp) {
                    mp.exportQueryMetricsCSV(outDir.resolve("query_metrics.csv").toString());
                    mp.exportMetersCSV(outDir.resolve("micrometer_meters.csv").toString());
                }
            }
            var client = profiler.getAllClientQueryTimes();
            var server = profiler.getAllServerQueryTimes();
            var ratio  = profiler.getAllQueryRatios();
            double art = IntStream.range(0, Math.min(client.size(), server.size()))
                    .mapToDouble(i -> client.get(i) + server.get(i))
                    .average().orElse(0.0);
            double avgRatio = ratio.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            Files.writeString(outDir.resolve("metrics_summary.txt"),
                    String.format("ART(ms)=%.3f%nAvgRatio=%.6f%n", art, avgRatio));
        }

        try { topKProfiler.export(outDir.resolve("topk_evaluation.csv").toString());
        } catch (Exception ignore) { /* ok if not used */ }

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
    }

    /** Append global recall rows to results/global_recall.csv (creates header once). */
    private static void writeGlobalRecallCsv(
            Path outDir,
            int dim,
            int[] kVariants,
            Map<Integer, Long> globalMatches,
            Map<Integer, Long> globalTruth
    ) {
        try {
            Files.createDirectories(outDir);
            Path p = outDir.resolve("global_recall.csv");
            if (!Files.exists(p)) {
                Files.writeString(p, "dimension,topK,global_recall,matches,truth\n",
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            try (var w = Files.newBufferedWriter(p, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                for (int k : kVariants) {
                    long truth = globalTruth.getOrDefault(k, 0L);
                    long matches = globalMatches.getOrDefault(k, 0L);
                    double grecall = (truth == 0) ? 0.0 : (double) matches / (double) truth;

                    w.write(String.format(Locale.ROOT, "%d,%d,%.6f,%d,%d%n", dim, k, grecall, matches, truth));

                    String line = String.format(Locale.ROOT,
                            "GlobalRecall@K=%d (dim=%d): %.6f  [matches=%d, truth=%d]",
                            k, dim, grecall, matches, truth);
                    logger.info(line);
                    System.out.println(line);
                }
            }
        } catch (IOException ioe) {
            logger.warn("Failed to write global_recall.csv", ioe);
        }
    }

    private TrueNN computeTrueNNFromBase(double[] q, BaseVectorReader br) {
        double best = Double.POSITIVE_INFINITY;
        int bestId = -1;
        for (int i = 0; i < br.count; i++) {
            double d = br.l2(q, i);
            if (d < best) { best = d; bestId = i; }
        }
        return new TrueNN(bestId, best);
    }

    private static double ratioGivenDenom(double denom, double[] q, List<QueryResult> retrieved, int k, BaseVectorReader br) {
        final double eps = 1e-12;
        if (!Double.isFinite(denom)) return Double.NaN;
        if (denom <= eps) return 1.0;

        double num = Double.POSITIVE_INFINITY;
        int upto = Math.min(k, retrieved.size());
        for (int i = 0; i < upto; i++) {
            String id = retrieved.get(i).getId();
            int idx; try { idx = Integer.parseInt(id); } catch (NumberFormatException e) { continue; }
            double d = br.l2(q, idx);
            if (d < num) num = d;
        }
        if (!Double.isFinite(num)) return Double.NaN;

        double r = num / denom;
        if (r + 1e-12 < 1.0) {
            logger.warn("Distance ratio < 1.0 ({}). Denom={}, Num={}, k={}",
                    String.format(Locale.ROOT, "%.6f", r), denom, num, k);
        }
        return r;
    }

    public IndexService getIndexService() { return this.indexService; }
    public QueryService getQueryService() { return this.queryService; }
    public Profiler getProfiler() { return this.profiler; }

    private static int numTablesFor(EvenLSH lsh, SystemConfig cfg) {
        try {
            var m = lsh.getClass().getMethod("getNumTables");
            Object v = m.invoke(lsh);
            if (v instanceof Integer n && n > 0) return n;
        } catch (Throwable ignore) {}

        try {
            var m = cfg.getClass().getMethod("getNumTables");
            Object v = m.invoke(cfg);
            if (v instanceof Integer n && n > 0) return n;
        } catch (Throwable ignore) {}

        return 4; // fallback
    }

    private int shardsToProbe() {
        if (OVERRIDE_PROBE > 0) return OVERRIDE_PROBE;
        try {
            var m = indexService.getClass().getMethod("getNumShards");
            Object v = m.invoke(indexService);
            if (v instanceof Integer n && n > 0) return n;
        } catch (Throwable ignore) {}
        return Math.max(1, config.getNumShards());
    }

    private boolean sampleDimsOk(BaseVectorReader br) {
        int[] probes = {0, Math.max(0, br.count/2), Math.max(0, br.count-1)};
        for (int idx : probes) {
            if (!br.storedDimOk(idx)) return false;
        }
        return true;
    }

    private boolean sampleGtMatchesTrueNN(List<double[]> queries,
                                          GroundtruthManager gt,
                                          BaseVectorReader br,
                                          int sampleSize, double tol) {
        if (queries.isEmpty() || gt.size() == 0) return false;
        int n = Math.min(sampleSize, Math.min(queries.size(), gt.size()));
        int mismatches = 0;

        java.util.Random rnd = new java.util.Random(13);
        for (int t = 0; t < n; t++) {
            int qi = rnd.nextInt(Math.min(queries.size(), gt.size()));
            int[] row = gt.getGroundtruth(qi, 1);
            if (row.length == 0) { mismatches++; continue; }
            int gtId = row[0];

            TrueNN dn = computeTrueNNFromBase(queries.get(qi), br);
            if (dn.id != gtId) mismatches++;
        }
        double rate = mismatches / (double) n;
        if (rate > tol) {
            logger.warn("GT trust check failed: mismatch rate={}/{} ({})", mismatches, n, rate);
            return false;
        }
        logger.info("GT trust check passed: mismatch rate={}/{} ({})", mismatches, n, rate);
        return true;
    }

    /** Disable System.exit on shutdown (for tests). */
    public void setExitOnShutdown(boolean exitOnShutdown) { this.exitOnShutdown = exitOnShutdown; }

    private static boolean propOr(boolean defaultVal, String... keys) {
        for (String k : keys) {
            String v = System.getProperty(k);
            if (v != null) {
                // accept true/false/1/0/yes/no
                String s = v.trim().toLowerCase(Locale.ROOT);
                return s.equals("true") || s.equals("1") || s.equals("yes") || s.equals("y");
            }
        }
        return defaultVal;
    }

    // ====== retrieved IDs auditor ======
    private static final class RetrievedAudit {
        private final Path samplesCsv;
        private final Path worstCsv;

        RetrievedAudit(Path outDir) throws IOException {
            Files.createDirectories(outDir);
            this.samplesCsv = outDir.resolve("retrieved_samples.csv");
            this.worstCsv   = outDir.resolve("retrieved_worst.csv");
            writeHeader(samplesCsv);
            writeHeader(worstCsv);
        }

        private static void writeHeader(Path p) throws IOException {
            if (!Files.exists(p)) {
                Files.writeString(p, "qIndex,k,ratio,recall,retrieved_ids,groundtruth_ids\n");
            }
        }

        private static String joinInts(int[] a) {
            if (a == null || a.length == 0) return "";
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < a.length; i++) {
                if (i > 0) sb.append(';');
                sb.append(a[i]);
            }
            return sb.toString();
        }

        private static String joinIds(List<QueryResult> rs) {
            if (rs == null || rs.isEmpty()) return "";
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rs.size(); i++) {
                if (i > 0) sb.append(';');
                sb.append(rs.get(i).getId());
            }
            return sb.toString();
        }

        void appendSample(int qIndex, int k, double ratio, double recall,
                          List<QueryResult> retrieved, int[] truth) throws IOException {
            String line = String.format(Locale.ROOT, "%d,%d,%.6f,%.6f,%s,%s%n",
                    qIndex, k, ratio, recall, joinIds(retrieved), joinInts(truth));
            Files.writeString(samplesCsv, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }

        void appendWorst(int qIndex, int k, double ratio, double recall,
                         List<QueryResult> retrieved, int[] truth) throws IOException {
            String line = String.format(Locale.ROOT, "%d,%d,%.6f,%.6f,%s,%s%n",
                    qIndex, k, ratio, recall, joinIds(retrieved), joinInts(truth));
            Files.writeString(worstCsv, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    public void flushAll() throws IOException {
        logger.info("ForwardSecureANNSystem flushAll started");
        EncryptedPointBuffer buf = indexService.getPointBuffer();
        if (buf != null) buf.flushAll();

        if (queryOnlyMode) {
            logger.info("Query-only mode: skipping metadata version save");
        } else {
            metadataManager.saveIndexVersion(keyService.getCurrentVersion().getVersion());
        }
        logger.info("ForwardSecureANNSystem flushAll completed");
    }

    public void shutdown() {
        System.out.printf(
                "Total indexing time: %d ms%nTotal query time: %d ms%n%n",
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

            try { if (baseReader != null) baseReader.close(); } catch (Exception ignore) {}

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

            // restore FsPaths props
            if (prevBaseProp == null)  System.clearProperty(FsPaths.BASE_DIR_PROP); else System.setProperty(FsPaths.BASE_DIR_PROP,  prevBaseProp);
            if (prevMetaProp == null)  System.clearProperty(FsPaths.METADB_PROP);   else System.setProperty(FsPaths.METADB_PROP,    prevMetaProp);
            if (prevPointsProp == null)System.clearProperty(FsPaths.POINTS_PROP);   else System.setProperty(FsPaths.POINTS_PROP,    prevPointsProp);
            if (prevKeyStoreProp == null) System.clearProperty(FsPaths.KEYSTORE_PROP); else System.setProperty(FsPaths.KEYSTORE_PROP, prevKeyStoreProp);
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

        Path resolvedKeyStore = resolveKeyStorePath(keysFile, metadataPath);
        Files.createDirectories(resolvedKeyStore.getParent());
        KeyManager keyManager = new KeyManager(resolvedKeyStore.toString());

        boolean queryOnly = "POINTS_ONLY".equalsIgnoreCase(dataPath) || Boolean.getBoolean("query.only");
        int restoreVersion = Integer.getInteger("restore.version", -1);
        if (queryOnly && restoreVersion <= 0) {
            restoreVersion = detectLatestVersion(pointsPath);
        }

        // Load config once here as well for policy decisions in main
        ApiSystemConfig apiCfg = new ApiSystemConfig(configFile);
        SystemConfig cfg = apiCfg.getConfig();

        int  opsCap = (int) Math.min(Integer.MAX_VALUE, cfg.getOpsThreshold());
        long ageMs  = cfg.getAgeThresholdMs();

        // 🔒 freeze rotation in query-only
        KeyRotationPolicy policy = new KeyRotationPolicy(
                queryOnly ? Integer.MAX_VALUE : opsCap,
                queryOnly ? Long.MAX_VALUE : ageMs
        );

        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, null);
        CryptoService cryptoService =
                new com.fspann.crypto.AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        // IMPORTANT to activate the desired key version BEFORE building token factories
        if (queryOnly && restoreVersion > 0) {
            boolean ok = keyService.activateVersion(restoreVersion);
            if (!ok) {
                logger.warn("Could not activate key version v{}; current is v{}",
                        restoreVersion, keyService.getCurrentVersion().getVersion());
            }
        }
        logger.info("Key current version at startup: v{}", keyService.getCurrentVersion().getVersion());

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile, dataPath, keysFile, dimensions, metadataPath, false,
                metadataManager, cryptoService, batchSize
        );

        if (queryOnly) {
            sys.setQueryOnlyMode(true); // skip writes during shutdown
            int restored = sys.restoreIndexFromDisk(restoreVersion);
            sys.finalizeForSearch();

            logger.info("Ready to query: restored {} points for dim={} (requested)", restored, dimensions.get(0));

            sys.runQueries(queryPath, dimensions.get(0), groundtruthPath);

            if (cfg.getOutput().exportArtifacts) {
                sys.exportArtifacts(sys.resultsDir);
            }

            sys.shutdown();
            return;
        }

        if ("true".equals(System.getProperty("disable.exit"))) {
            sys.setExitOnShutdown(false);
        }

        int dim = dimensions.get(0);
        sys.runEndToEnd(dataPath, queryPath, dim, groundtruthPath);

        if (cfg.getOutput().exportArtifacts) {
            sys.exportArtifacts(sys.resultsDir);
        }

        if (sys.getProfiler() != null) {
            System.out.printf("Average Client Query Time: %.2f ms\n",
                    sys.getProfiler().getAllClientQueryTimes().stream().mapToDouble(d -> d).average().orElse(0.0));
            System.out.printf("Average Ratio: %.4f\n",
                    sys.getProfiler().getAllQueryRatios().stream().mapToDouble(d -> d).average().orElse(0.0));
        }

        long start = System.currentTimeMillis();
        logger.info("Calling system.shutdown()...");
        sys.shutdown();
        logger.info("Shutdown completed in {} ms", System.currentTimeMillis() - start);
    }
}