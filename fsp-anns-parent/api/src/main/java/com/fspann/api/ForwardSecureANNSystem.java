package com.fspann.api;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.crypto.SelectiveReencCoordinator;
import com.fspann.index.paper.EvenLSH;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.common.ReencryptReport;
import com.fspann.loader.DefaultDataLoader;
import com.fspann.loader.FormatLoader;
import com.fspann.loader.GroundtruthManager;
import com.fspann.loader.StreamingBatchLoader;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.common.QueryToken;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.common.QueryResult;
import com.fspann.query.core.TopKProfiler;
import com.fspann.query.service.QueryService;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Collections;

public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

    private final SecureLSHIndexService indexService;
    private final Map<Integer, QueryTokenFactory> tokenFactories = new ConcurrentHashMap<>();
    private final QueryService queryService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final SystemConfig config;
    private final StringKeyedCache cache;
    private final String configPath;
    private final Profiler profiler;
    private final boolean verbose;
    private final RocksDBMetadataManager metadataManager;
    private final TopKProfiler topKProfiler;
    private volatile boolean queryOnlyMode = false;
    private BaseVectorReader baseReader = null;
    private final ReencryptionTracker reencTracker;
    private final SelectiveReencCoordinator reencCoordinator;

    // Config-driven toggles
    private final boolean computePrecision;
    private final boolean writeGlobalPrecisionCsv;
    private final int[] K_VARIANTS;
    private final boolean auditEnable;
    private final int auditK;
    private final int auditSampleEvery;
    private final int auditWorstKeep;
    private final Path resultsDir;
    private final double configuredNoiseScale;
    private final RetrievedAudit retrievedAudit;
    private Path reencCsv;
    private final SystemConfig.KAdaptiveConfig kAdaptive;
    // key = dim|topK|vectorHash  (simple but enough for tests)
    private final Map<String, List<QueryResult>> queryCache = new ConcurrentHashMap<>();
    enum RatioSource {AUTO, GT, BASE}

    private final RatioSource ratioSource;

    static final class TrueNN {
        final int id;
        final double d;

        TrueNN(int id, double d) {
            this.id = id;
            this.d = d;
        }
    }

    private final int BATCH_SIZE;
    private long totalIndexingTimeNs = 0L;
    private long totalQueryTimeNs = 0L;
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
    private final boolean reencEnabled;
    private static final double RATIO_EPS = 1e-24;

    // remember previous FsPaths props to restore on shutdown
    private final String prevBaseProp;
    private final String prevMetaProp;
    private final String prevPointsProp;
    private final String prevKeyStoreProp;

    /**
     * Global "touched" accumulator of vector IDs encountered during the run.
     */
    private final Set<String> touchedGlobal = ConcurrentHashMap.newKeySet();
    private static final int FANOUT_WARN = Integer.getInteger("guard.fanout.warn", 2000);

    /**
     * Re-encryption mode:
     *   - "immediate": (legacy) perform re-encryption checks per query
     *   - "end": (default) accumulate touched IDs and re-encrypt once at the end of the run
     */
    private final String reencMode = System.getProperty("reenc.mode", "end");
    private static final Object REENC_CSV_LOCK = new Object();
    private final java.util.concurrent.atomic.AtomicBoolean reencRan = new java.util.concurrent.atomic.AtomicBoolean(false);

    public ForwardSecureANNSystem(
            String configPath,
            String /* unused */ dataPath,
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
        this.configPath = normalizePath(configPath);

        // Load config
        SystemConfig cfg;
        try {
            com.fspann.api.ApiSystemConfig apiCfg = new com.fspann.api.ApiSystemConfig(this.configPath);
            cfg = apiCfg.getConfig();
        } catch (IOException e) {
            logger.error("Failed to initialize configuration: {}", configPath, e);
            throw e;
        }
        this.config = cfg;

        this.kAdaptive = (config.getKAdaptive() != null)
                ? config.getKAdaptive()
                : new SystemConfig.KAdaptiveConfig();

        // ---- Materialize commonly used settings from config ----
        boolean defaultComputePrecision =
                (config.getEval() != null) ? config.getEval().computePrecision : true;
        this.computePrecision = propOr(defaultComputePrecision, "eval.computePrecision", "computePrecision");

        this.writeGlobalPrecisionCsv = propOr(
                config.getEval().writeGlobalPrecisionCsv,
                "eval.writeGlobalPrecisionCsv", "eval.writeGlobalPrecision", "writeGlobalPrecisionCsv");

        this.K_VARIANTS = (config.getEval().kVariants != null && config.getEval().kVariants.length > 0)
                ? config.getEval().kVariants.clone()
                : new int[]{1, 5, 10, 20, 40, 60, 80, 100};

        this.reencEnabled = propOr(
                (config != null ? config.isReencryptionGloballyEnabled() : true),
                "reenc.enabled", "reencryption.enabled", "reencrypt.enabled"
        );

        // ---- AUDIT (robust to missing top-level "audit")
        boolean enableAudit = false;
        int aK = 100;
        int aEvery = 100;
        int aWorst = 25;
        try {
            var ac = config.getAudit();
            if (ac != null) {
                enableAudit = ac.enable;
                if (ac.k > 0) aK = ac.k;
                if (ac.sampleEvery > 0) aEvery = ac.sampleEvery;
                if (ac.worstKeep > 0) aWorst = ac.worstKeep;
            }
        } catch (Throwable ignore) { /* fall back below */ }
        enableAudit = enableAudit || propOr(true, "output.audit", "audit");
        this.auditEnable = enableAudit;
        this.auditK = aK;
        this.auditSampleEvery = aEvery;
        this.auditWorstKeep = aWorst;

        // results directory
        String resultsDirStr = (config.getOutput() != null && config.getOutput().resultsDir != null && !config.getOutput().resultsDir.isBlank())
                ? config.getOutput().resultsDir : System.getProperty("results.dir", "results");
        this.resultsDir = Paths.get(resultsDirStr);
        try {
            Files.createDirectories(resultsDir);
        } catch (IOException ioe) {
            logger.warn("Could not create resultsDir {}; falling back to CWD", resultsDir, ioe);
        }
        this.topKProfiler = new TopKProfiler(resultsDir.toString());
        double defaultNoise = (config.getCloak() != null) ? Math.max(0.0, config.getCloak().noise) : 0.0;
        this.configuredNoiseScale = defaultNoise;

        // init per-query re-encryption CSV *after* resultsDir exists
        this.reencCsv = resultsDir.resolve("reencrypt_metrics.csv");
        initReencCsvIfNeeded(this.reencCsv);

        // ratio source
        String rs = (config.getRatio() != null && config.getRatio().source != null)
                ? config.getRatio().source.toLowerCase(Locale.ROOT)
                : "auto";
        this.ratioSource = switch (rs) {
            case "gt" -> RatioSource.GT;
            case "base" -> RatioSource.BASE;
            default -> RatioSource.AUTO;
        };

        // legacy override hooks
        this.OVERRIDE_TABLES = (config.getLsh().numTables > 0)
                ? config.getLsh().numTables
                : Integer.getInteger("lsh.tables", -1);
        this.OVERRIDE_ROWS = (config.getLsh().rowsPerBand > 0)
                ? config.getLsh().rowsPerBand
                : Integer.getInteger("lsh.rowsPerBand", -1);
        this.OVERRIDE_PROBE = (config.getLsh().probeShards > 0)
                ? config.getLsh().probeShards
                : Integer.getInteger("probe.shards", -1);

        // Initialize audit writer once (if enabled)
        RetrievedAudit ra = null;
        if (auditEnable) {
            try {
                ra = new RetrievedAudit(this.resultsDir);
            } catch (IOException ioe) {
                logger.warn("Audit writer init failed; audit disabled", ioe);
            }
        }
        this.retrievedAudit = ra;

        // ---- Centralize paths via FsPaths ----
        this.prevBaseProp = System.getProperty(FsPaths.BASE_DIR_PROP);
        this.prevMetaProp = System.getProperty(FsPaths.METADB_PROP);
        this.prevPointsProp = System.getProperty(FsPaths.POINTS_PROP);
        System.setProperty(FsPaths.BASE_DIR_PROP, metadataPath.toString());
        System.setProperty(FsPaths.METADB_PROP, metadataPath.resolve("metadata").toString());
        System.setProperty(FsPaths.POINTS_PROP, metadataPath.resolve("points").toString());

        this.pointsPath = FsPaths.pointsDir();
        this.metaDBPath = FsPaths.metadataDb();
        Files.createDirectories(pointsPath);
        Files.createDirectories(metaDBPath);

        // Injected core services
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService cannot be null");

        // Key service (also align FsPaths KEYSTORE_PROP)
        Path derivedKeyRoot = resolveKeyStorePath(keysFilePath, metadataPath);
        this.prevKeyStoreProp = System.getProperty(FsPaths.KEYSTORE_PROP);
        System.setProperty(FsPaths.KEYSTORE_PROP, derivedKeyRoot.toString());

        MeterRegistry meterRegistry = null;
        try {
            meterRegistry = new SimpleMeterRegistry();
        } catch (Throwable ignore) { /* allow null */ }

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
        } catch (Throwable ignore) {
        }
        this.keyStorePath = ksp;

        // Build index service
        this.indexService = SecureLSHIndexService.fromConfig(cryptoService, keyService, metadataManager, config);
        if (config.getPaper() != null && config.getPaper().enabled) {
            logger.info("Ideal-system partitioned mode: disabling LSH in index service.");
            indexService.disableLSHForAllDimensions();
        }



        if (keyService instanceof KeyRotationServiceImpl kr) {
            kr.setIndexService(indexService);
        }

        this.cache = new StringKeyedCache(config.getNumShards() * 1000);
        this.profiler = config.isProfilerEnabled()
                ? new com.fspann.api.MicrometerProfiler(new SimpleMeterRegistry())
                : null;

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

            // probe-shards override (for logging)
            int shards = shardsToProbe();

            int ell = (config != null && config.getPaper() != null) ? Math.max(1, config.getPaper().divisions) : 1;
            int m = (config != null && config.getPaper() != null) ? Math.max(1, config.getPaper().m) : 1;
            long seedBase = (config != null && config.getPaper() != null) ? config.getPaper().seed : 13L;

            tokenFactories.put(dim, new QueryTokenFactory(
                    cryptoService, keyService, lshForDim, /*numTables*/ tables, /*divisions*/ ell, /*m*/ m, /*seedBase*/ seedBase
            ));

            boolean partitionedMode = (config != null && config.getPaper() != null && config.getPaper().isEnabled());
            if (partitionedMode) {
                logger.info("TokenFactory created: dim={} divisions (ℓ)={} m(projections/division)={} shardsToProbe={}",
                        dim, ell, m, shards);
            } else {
                int mRows = -1;
                try {
                    mRows = (int) lshForDim.getClass().getMethod("getRowsPerBand").invoke(lshForDim);
                } catch (Throwable ignore) {
                    try {
                        mRows = (int) lshForDim.getClass().getMethod("getNumHashFuncsPerTable").invoke(lshForDim);
                    } catch (Throwable ignored2) { /* ok */ }
                }
                int logM = (OVERRIDE_ROWS > 0 ? OVERRIDE_ROWS : mRows);
                logger.info("TokenFactory created: dim={} LSH tables (L)={} m(rows/table)={} shardsToProbe={}",
                        dim, tables, logM, shards);
            }
        }

        // Primary dim
        int primaryDim = dimensions.get(0);
        QueryTokenFactory qtf = tokenFactories.getOrDefault(primaryDim, null);

        // Query service
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService, qtf);

        this.reencTracker = new ReencryptionTracker();
        // Ensure QS continues marking touches even without LSH
        if (this.queryService instanceof QueryServiceImpl qs) {
            qs.setReencryptionTracker(this.reencTracker);
        }
        this.reencCoordinator = new SelectiveReencCoordinator(
                this.indexService,
                this.cryptoService,
                ks,
                this.reencTracker,
                meterRegistry,
                this.resultsDir,
                () -> dirSize(this.pointsPath)   // StorageSizer: bytes on disk for points
        );

        // Let the query layer mark touches
        if (this.queryService instanceof QueryServiceImpl qs) {
            qs.setReencryptionTracker(this.reencTracker);
        }

        String basePathProp = System.getProperty("base.path", "").trim();
        if (!basePathProp.isEmpty()) {
            Path basePath = Paths.get(basePathProp);
            boolean isBvecs = basePath.toString().toLowerCase(Locale.ROOT).endsWith(".bvecs");
            int dimProbe = dimensions.get(0); // expected fixed dim for this run
            try {
                this.baseReader = BaseVectorReader.open(basePath, dimProbe, isBvecs);
                logger.info("BaseVectorReader mapped: {} (dim={}, type={})",
                        basePath, dimProbe, (isBvecs ? "bvecs" : "fvecs"));
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

            List<double[]> valid = new ArrayList<>(slice.size());
            List<String> ids = new ArrayList<>(slice.size());

            for (int j = 0; j < slice.size(); j++) {
                double[] v = slice.get(j);
                long ord = fileOrdinal.getAndIncrement(); // original file position for this record

                if (v == null) {
                    logger.warn("Skipping null vector at ordinal={}", ord);
                    continue;
                }
                if (v.length != dim) {
                    logger.warn("Skipping vector at ordinal={} with dim={} (expected {})", ord, v.length, dim);
                    continue;
                }
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
            totalIndexingTimeNs += durationNs;
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
            keyService.rotateIfNeeded();
            indexService.insert(id, vector);
            indexedCount.incrementAndGet();
        } finally {
            if (profiler != null) {
                profiler.stop("insert");
                List<Long> timings = profiler.getTimings("insert");
                if (!timings.isEmpty()) {
                    long durationNs = timings.get(timings.size() - 1);
                    totalIndexingTimeNs += durationNs;
                    if (verbose) logger.debug("Insert complete for id={} in {} ms", id, durationNs / 1_000_000.0);
                }
            }
        }
    }

    /* ---------------------- Query API ---------------------- */

    private QueryTokenFactory factoryForDim(int dim) {
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        boolean partitioned = (config.getPaper() != null && config.getPaper().enabled);
        if (partitioned) {
            // NEW SYSTEM / IDEAL-SYSTEM: LSH is forbidden.
            var pc = config.getPaper();
            int ell = Math.max(1, pc.divisions);
            int m = Math.max(1, pc.m);
            long seed = pc.seed;

            logger.info("TokenFactory (ideal/partitioned): dim={} ell={} m={} seed={}", dim, ell, m, seed);

            return new QueryTokenFactory(
                    cryptoService,
                    keyService,
                    null,          // << NO LSH
                    0,             // numTables = unused
                    ell,
                    m,
                    seed
            );
        }

        // Legacy multiprobe path (kept intact because you said: DO NOT BREAK OLD SYSTEM)
        return tokenFactories.computeIfAbsent(dim, d -> {
            EvenLSH lsh = indexService.getLshForDimension(d);
            int tables = (OVERRIDE_TABLES > 0)
                    ? OVERRIDE_TABLES
                    : numTablesFor(lsh, config);

            int ell = 1;
            int m = 1;
            long seed = 13L;

            logger.info("TokenFactory (legacy multiprobe): dim={} LSH tables={} m={} seed={}", d, tables, m, seed);

            return new QueryTokenFactory(
                    cryptoService,
                    keyService,
                    lsh,
                    tables,
                    ell,
                    m,
                    seed
            );
        });
    }

    public QueryToken cloakQuery(double[] queryVector, int dim, int topK) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");

        return factoryForDim(dim).create(queryVector, topK);
    }

    public List<QueryResult> query(double[] queryVector, int topK, int dim) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");

        final long start = System.nanoTime();
        final QueryToken token = factoryForDim(dim).create(queryVector, topK);
        final String ckey = cacheKeyOf(token);
        recordRecent(queryVector);

        // cache check
        List<QueryResult> cached = cache.get(ckey);
        if (cached != null) {
            long ns = Math.max(0L, System.nanoTime() - start);
            totalQueryTimeNs += ns;  // accumulate client time (cache hit)
            if (profiler != null) profiler.recordQueryMetric("Q_cache_" + topK, 0.0, ns / 1e6, 0.0);
            return cached;
        }

        // miss → go to service
        List<QueryResult> results = queryService.search(token);
        long end = System.nanoTime();
        cache.put(ckey, results);

        long clientNs = Math.max(0L, end - start);
        long serverNs = ((QueryServiceImpl)queryService).getLastQueryDurationNs();
        totalQueryTimeNs += Math.max(0L, clientNs + serverNs);

        if (profiler != null && queryService instanceof QueryServiceImpl qs) {
            double serverMs = boundedServerMs(qs, start, end);
            double clientMs = clientNs / 1_000_000.0;
            profiler.recordQueryMetric("Q_" + topK, serverMs, clientMs, 0.0);
        }
        return results;
    }

    public List<QueryResult> queryWithCloak(double[] queryVector, int topK, int dim) {
        Objects.requireNonNull(queryVector, "Query vector cannot be null");
        if (topK <= 0) throw new IllegalArgumentException("TopK must be positive");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (queryVector.length != dim) throw new IllegalArgumentException("Query vector length must match dimension");

        // With noise disabled, just use the standard path
        if (configuredNoiseScale <= 0.0) {
            return query(queryVector, topK, dim);
        }

        final long start = System.nanoTime();
        final QueryToken token = cloakQuery(queryVector, dim, topK);
        final String ckey = cacheKeyOf(token);
        recordRecent(queryVector);

        List<QueryResult> cached = cache.get(ckey);
        if (cached != null) {
            long clientNs = Math.max(0L, System.nanoTime() - start);
            long serverNs = ((QueryServiceImpl)queryService).getLastQueryDurationNs();
            totalQueryTimeNs += Math.max(0L, clientNs + serverNs);
            if (profiler != null) profiler.recordQueryMetric("Q_cache_" + topK, 0.0, clientNs / 1_000_000.0, 0.0);
            return cached;
        }

        List<QueryResult> results = queryService.search(token);
        long end = System.nanoTime();
        cache.put(ckey, results);

        long clientNs = Math.max(0L, end - start);
        long serverNs = ((QueryServiceImpl)queryService).getLastQueryDurationNs();
        totalQueryTimeNs += Math.max(0L, clientNs + serverNs);

        if (profiler != null && queryService instanceof QueryServiceImpl qs) {
            double serverMs = boundedServerMs(qs, start, end);
            double clientMs = clientNs / 1_000_000.0;
            profiler.recordQueryMetric("Q_" + topK, serverMs, clientMs, 0.0);
        }
        if (verbose) logger.debug("Cloaked query returned {} results for topK={}", results.size(), topK);
        return results;
    }

    // ---------------------- E2E Runner ---------------------- //

    public void runEndToEnd(String dataPath, String queryPath, int dim, String groundtruthPath) throws IOException {
        Objects.requireNonNull(dataPath, "Data path cannot be null");
        Objects.requireNonNull(queryPath, "Query path cannot be null");
        Objects.requireNonNull(groundtruthPath, "Groundtruth path cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        // Local view of K-adaptive config (with sane defaults if null)
        SystemConfig.KAdaptiveConfig kCfg = (config.getKAdaptive() != null)
                ? config.getKAdaptive()
                : new SystemConfig.KAdaptiveConfig();

        // Freeze rotation while indexing
        if (keyService instanceof KeyRotationServiceImpl kr) kr.freezeRotation(true);

        // 1) Index + seal
        indexStream(dataPath, dim);
        indexService.flushBuffers();
        finalizeForSearch();
        if (keyService instanceof KeyRotationServiceImpl kr) {
            kr.freezeRotation(false);
        }

        // 2) Load queries + GT
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

        int datasetSize = (baseReader != null) ? baseReader.count
                : Math.max(indexService.getIndexedVectorCount(), totalInserted);
        groundtruth.normalizeIndexBaseIfNeeded(datasetSize);

        boolean idsOk = groundtruth.isConsistentWithDatasetSize(datasetSize);
        boolean dimsOk = (baseReader != null) && sampleDimsOk(baseReader);
        boolean sampleOk = (baseReader != null) && sampleGtMatchesTrueNN(
                queries, groundtruth, baseReader,
                Math.max(0, config.getRatio().gtSample),
                Math.max(0.0, Math.min(1.0, config.getRatio().gtMismatchTolerance)));

        boolean gtTrusted = (baseReader != null) && idsOk && dimsOk && sampleOk;
        logger.info("Ratio trust gate: gtTrusted={}, idsOk={}, dimsOk={}, sampleOk={}",
                gtTrusted, idsOk, dimsOk, sampleOk);
        if (baseReader == null) {
            logger.warn("No baseReader: ratio computation will be NaN unless you set -Dbase.path");
        }

        ResultWriter rw = new ResultWriter(resultsDir.resolve("results_table.csv"));
        try {
            topKProfiler.setDatasetSize(datasetSize);
        } catch (Throwable ignore) {
        }

        final boolean doAudit = auditEnable && (retrievedAudit != null);
        final int baseKForToken = Math.max(
                Math.max(auditK, 100),
                Arrays.stream(K_VARIANTS).max().orElse(100)
        );

        final int keepWorst = Math.max(0, auditWorstKeep);

        class WorstRec {
            final int qIndex, k;
            final double ratio, precision;

            WorstRec(int q, int k, double r, double p) {
                this.qIndex = q;
                this.k = k;
                this.ratio = r;
                this.precision = p;
            }
        }

        PriorityQueue<WorstRec> worstPQ =
                new PriorityQueue<>(Comparator.comparingDouble(w -> w.ratio)); // min-heap

        Map<Integer, Long> globalMatches = new HashMap<>();
        Map<Integer, Long> globalReturned = new HashMap<>();
        for (int k : K_VARIANTS) {
            globalMatches.put(k, 0L);
            globalReturned.put(k, 0L);
        }
        final int queriesCount = queries.size();

        // Quick fanout probe on Q0
        if (!queries.isEmpty()) computeAndLogFanoutForQ0(queries.get(0), dim, K_VARIANTS);

        // 4) Query loop
        for (int q = 0; q < queries.size(); q++) {
            final int qIndex = q;
            double[] queryVec = queries.get(q);

            // --- K-adaptive query execution ---
            final int Kmax = Arrays.stream(K_VARIANTS).max().orElse(100);
            QueryServiceImpl qs = (QueryServiceImpl) queryService;
            QueryToken baseToken = null;
            List<QueryResult> baseReturned = Collections.emptyList();

            long clientStart = System.nanoTime();
            long clientEnd = clientStart;
            double clientMs = 0.0;
            double serverMs = 0.0;

            int candTotal = -1;
            int candKeptVersion = -1;
            int candDecrypted = -1;
            int returned = -1;

            int round = 0;
            while (true) {
                baseToken = factoryForDim(dim).create(queryVec, baseKForToken);
                baseReturned = queryService.search(baseToken);
                clientEnd = System.nanoTime();

                clientMs = (clientEnd - clientStart) / 1_000_000.0;
                serverMs = boundedServerMs(qs, clientStart, clientEnd);

                candTotal = qs.getLastCandTotal();
                candKeptVersion = qs.getLastCandKeptVersion();
                candDecrypted = qs.getLastCandDecrypted();
                returned = qs.getLastReturned();

                int uptoMax = Math.min(Kmax, baseReturned.size());
                double rr = (Kmax > 0) ? (double) uptoMax / (double) Kmax : 0.0;
                double fanout = (returned > 0)
                        ? candTotal / (double) returned
                        : Double.POSITIVE_INFINITY;

                if (!kCfg.enabled) {
                    break;
                }

                boolean rrOk = rr >= kCfg.targetReturnRate;
                boolean fanoutTooHigh = fanout >= kCfg.maxFanout;
                boolean roundsExceeded = round >= kCfg.maxRounds;

                if (rrOk || fanoutTooHigh || roundsExceeded) {
                    if (rrOk) {
                        logger.debug("q{} k-adaptive: return-rate {} reached target {}", qIndex, rr, kCfg.targetReturnRate);
                    } else if (fanoutTooHigh) {
                        logger.debug("q{} k-adaptive: fanout {} exceeded max {}", qIndex, fanout, kCfg.maxFanout);
                    } else {
                        logger.debug("q{} k-adaptive: maxRounds {} reached", qIndex, kCfg.maxRounds);
                    }
                    break;
                }

                round++;
                int curShards = shardsToProbe();
                int widened = (int) Math.ceil(curShards * kCfg.probeFactor);
                System.setProperty("probe.shards", Integer.toString(widened));
                logger.info("q{} k-adaptive round {}: rr={} fanout={} – widening probe.shards {} -> {}",
                        qIndex,
                        round,
                        String.format(Locale.ROOT, "%.4f", rr),
                        (Double.isFinite(fanout) ? String.format(Locale.ROOT, "%.1f", fanout) : "inf"),
                        curShards,
                        widened);
            }
            // --- end K-adaptive query execution ---

            // Post-query invariants and guards
            guardCandidateInvariants(qIndex, candTotal, candKeptVersion, candDecrypted, returned);

            if (returned > 0) {
                double fanout = candTotal / (double) returned;
                double fanoutWarn = Double.parseDouble(System.getProperty("guard.fanout.warn", "2000"));
                if (fanout > fanoutWarn) {
                    logger.warn("q{} excessive fanout: scanned/returned ≈ {} ({} / {})",
                            qIndex, String.format(Locale.ROOT, "%.1f", fanout), candTotal, returned);
                }
            }

            int uptoMax = Math.min(Kmax, baseReturned.size());
            double rrFinal = (Kmax > 0) ? (double) uptoMax / (double) Kmax : 0.0;
            double minRR = Double.parseDouble(System.getProperty("guard.returnrate.min", "0.70"));
            if (rrFinal < minRR) {
                logger.warn("q{} low return-rate at Kmax={}: returned={} ({}% of K). Consider increasing probes/tables.",
                        qIndex, Kmax, uptoMax, String.format(Locale.ROOT, "%.0f", 100.0 * rrFinal));
            }

            // Re-encryption + touch metrics use *final* candidate set
            ReencOutcome rep = maybeReencryptTouched("Q" + q, qs);
            int svcCum = qs.getLastTouchedCumulativeUnique();
            if (svcCum != rep.cumulativeUnique) {
                logger.debug("q{} touch-set divergence: svc={} sys={}", qIndex, svcCum, rep.cumulativeUnique);
            }

            int touchedCount = (qs.getLastCandidateIds() != null) ? qs.getLastCandidateIds().size() : 0;
            var last = qs.getLastCandidateIds();
            if (last != null && !last.isEmpty()) {
                touchedGlobal.addAll(last);
            }

            com.fspann.common.EncryptedPointBuffer buf = indexService.getPointBuffer();
            long insertTimeMs = buf != null ? buf.getLastBatchInsertTimeMs() : 0;
            int totalFlushed = buf != null ? buf.getTotalFlushedPoints() : 0;
            int flushThreshold = buf != null ? buf.getFlushThreshold() : 0;
            int tokenSizeBytes = QueryServiceImpl.estimateTokenSizeBytes(baseToken);
            int vectorDim = baseToken.getDimension();

            List<QueryEvaluationResult> enriched = new ArrayList<>(K_VARIANTS.length);
            for (int k : K_VARIANTS) {
                int upto = Math.min(k, baseReturned.size());
                List<QueryResult> prefix = baseReturned.subList(0, upto);

                double distRatio = (baseReader == null) ? Double.NaN
                        : ratioAtKMetric(queryVec, prefix, k, qIndex, groundtruth, baseReader, gtTrusted);

                int[] truth = groundtruth.getGroundtruth(q, k);

                int matches = 0;
                for (int i = 0; i < prefix.size() && i < truth.length; i++) {
                    int id;
                    try {
                        id = Integer.parseInt(prefix.get(i).getId());
                    } catch (NumberFormatException nfe) {
                        continue;
                    }
                    if (containsInt(truth, id)) matches++;
                }
                double precAtK = (k > 0 ? (double) matches / (double) k : 0.0);

                globalMatches.put(k, globalMatches.get(k) + matches);
                globalReturned.put(k, globalReturned.get(k) + upto);

                String candMode = (candTotal >= 0 && candKeptVersion >= 0 && candDecrypted >= 0 && returned >= 0)
                        ? "full" : "partial";

                enriched.add(new QueryEvaluationResult(
                        k, upto, distRatio, precAtK,
                        Math.round(serverMs),
                        insertTimeMs,
                        candDecrypted,
                        tokenSizeBytes, vectorDim,
                        totalFlushed, flushThreshold,
                        /* touchedCount      */ touchedCount,
                        /* reencryptedCount  */ rep.rep.reencryptedCount,
                        /* reencTimeMs       */ rep.rep.timeMs,
                        /* reencBytesDelta   */ rep.rep.bytesDelta,
                        /* reencBytesAfter   */ rep.rep.bytesAfter,
                        /* ratioDenomSource  */ ratioDenomLabel(gtTrusted),
                        /* clientTimeMs      */ Math.round(clientMs),
                        /* tokenK            */ baseKForToken,
                        /* tokenKBase        */ baseKForToken,
                        /* qIndexZeroBased   */ qIndex,
                        /* candMetricsMode   */ candMode
                ));
            }
            // ---- make captured vars effectively final for the lambda ----
            final int qIndexF = qIndex;
            final int candTotalF = candTotal;
            final int candKeptVersionF = candKeptVersion;
            final int candDecryptedF = candDecrypted;
            final int returnedF = returned;
            // -------------------------------------------------------------

            topKProfiler.record("Q" + q, enriched, candTotal, candKeptVersion, candDecrypted, returned);

            if (doAudit) {
                final int kAudit = Math.max(1, auditK);
                final int uptoA = Math.min(kAudit, baseReturned.size());
                final List<QueryResult> prefixA = baseReturned.subList(0, uptoA);
                final int[] truthA = groundtruth.getGroundtruth(qIndex, kAudit);

                final double ratioA = (baseReader == null) ? Double.NaN
                        : ratioAtKMetric(queryVec, prefixA, kAudit, qIndex, groundtruth, baseReader, gtTrusted);

                int hits = 0;
                for (int i = 0; i < uptoA && i < truthA.length; i++) {
                    try {
                        int id = Integer.parseInt(prefixA.get(i).getId());
                        if (containsInt(truthA, id)) hits++;
                    } catch (NumberFormatException ignore) {
                    }
                }
                final double precA = (kAudit > 0 ? (double) hits / (double) kAudit : 0.0);

                if (auditSampleEvery > 0 && (qIndex % auditSampleEvery) == 0) {
                    try {
                        retrievedAudit.appendSample(qIndex, kAudit, ratioA, precA, prefixA, truthA);
                    } catch (IOException ioe) {
                        logger.warn("Audit sample write failed q={},k={}", qIndex, kAudit, ioe);
                    }
                }
                if (auditWorstKeep > 0 && !Double.isNaN(ratioA)) {
                    worstPQ.offer(new WorstRec(qIndex, kAudit, ratioA, precA));
                    if (worstPQ.size() > keepWorst) worstPQ.poll();
                }
            }
            rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ")",
                    new String[]{
                            "QIndex0", "TopK", "Returned", "Ratio", "Precision", "RatioDenomSource",
                            "ServerTimeMs", "ClientTimeMs", "OverheadMs", "InsertTimeMs",
                            "ScannedCandidates", "CandKeptVersion", "CandDecrypted", "ReturnedAgain",
                            "TokenSize", "TokenK", "VectorDim", "TotalFlushed", "FlushThreshold",
                            "CandMetricsMode", "TokenKBase"
                    },
                    enriched.stream().map(er -> new String[]{
                            String.valueOf(qIndexF),
                            String.valueOf(er.getTopKRequested()),
                            String.valueOf(er.getRetrieved()),
                            (Double.isNaN(er.getRatio()) ? "NaN" :
                                    String.format(Locale.ROOT, "%.4f", er.getRatio())),
                            String.format(Locale.ROOT, "%.4f", er.getPrecision()),
                            (gtTrusted ? "gt" : "base"),
                            String.valueOf(er.getTimeMs()),
                            String.valueOf(er.getClientTimeMs()),
                            String.valueOf(Math.max(0, er.getClientTimeMs() - er.getTimeMs())),
                            String.valueOf(er.getInsertTimeMs()),
                            String.valueOf(candTotalF),
                            String.valueOf(candKeptVersionF),
                            String.valueOf(candDecryptedF),
                            String.valueOf(returnedF),
                            String.valueOf(er.getTokenSizeBytes()),
                            String.valueOf(er.getTokenK()),
                            String.valueOf(er.getVectorDim()),
                            String.valueOf(er.getTotalFlushedPoints()),
                            String.valueOf(er.getFlushThreshold()),
                            er.getCandMetricsMode(),
                            String.valueOf(er.getTokenKBase())
                    }).collect(Collectors.toList())
            );
            QueryEvaluationResult atK = enriched.stream()
                    .filter(e -> e.getTopKRequested() == Math.max(auditK, 1))
                    .findFirst().orElseGet(() -> enriched.get(enriched.size() - 1));

            if (profiler != null) profiler.recordQueryMetric("Q" + q, serverMs, clientMs, atK.getRatio());
            long serverNs = ((QueryServiceImpl)queryService).getLastQueryDurationNs();
            totalQueryTimeNs += Math.max(0L, clientEnd - clientStart + serverNs);

            if (q < 3 && !baseReturned.isEmpty() && baseReader != null) {
                int topId = -1;
                try {
                    topId = Integer.parseInt(baseReturned.get(0).getId());
                } catch (Exception ignore) {
                }
                int gt1 = -1;
                int[] row = groundtruth.getGroundtruth(q, 1);
                if (row.length > 0) gt1 = row[0];
                double dTop = (topId >= 0) ? baseReader.l2(queryVec, topId) : Double.NaN;
                double dGt1 = (gt1 >= 0) ? baseReader.l2(queryVec, gt1) : Double.NaN;
                logger.info("Sanity q{}: topId={} dTop={}; gt1={} dGt1={}", q, topId, dTop, gt1, dGt1);
            }
        }

        if (writeGlobalPrecisionCsv && computePrecision) {
            writeGlobalPrecisionCsv(
                    resultsDir, dim, K_VARIANTS,
                    globalMatches, globalReturned, queriesCount
            );
        }

        if (doAudit && !worstPQ.isEmpty()) {
            List<WorstRec> worst = new ArrayList<>(worstPQ);
            worst.sort(Comparator.comparingDouble((WorstRec w) -> w.ratio).reversed());
            for (WorstRec w : worst) {
                double[] qv = queries.get(w.qIndex);
                QueryToken tk = factoryForDim(dim).create(qv, w.k);
                List<QueryResult> ret = queryService.search(tk);
                int[] truth = groundtruth.getGroundtruth(w.qIndex, w.k);
                double ratio = (baseReader == null) ? Double.NaN
                        : ratioAvgOverRanks(qv, ret.subList(0, Math.min(w.k, ret.size())), truth, baseReader);
                try {
                    retrievedAudit.appendWorst(w.qIndex, w.k, ratio, w.precision, ret, truth);
                } catch (IOException ioe) {
                    logger.warn("Audit worst write failed q={},k={}", w.qIndex, w.k, ioe);
                }
            }
        }
        // re-encryption-at-end runs in shutdown() via finalizeReencryptionAtEnd()
    }

    public void runQueries(String queryPath, int dim, String groundtruthPath) throws IOException {
        Objects.requireNonNull(queryPath, "Query path cannot be null");
        Objects.requireNonNull(groundtruthPath, "Groundtruth path cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");

        // Freeze rotation while querying
        if (keyService instanceof KeyRotationServiceImpl kr) kr.freezeRotation(true);

        indexService.flushBuffers();
        finalizeForSearch();
        if (keyService instanceof KeyRotationServiceImpl kr) {
            kr.freezeRotation(false);
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

        int datasetSize = (baseReader != null)
                ? baseReader.count
                : Math.max(indexService.getIndexedVectorCount(), totalInserted);

        groundtruth.normalizeIndexBaseIfNeeded(datasetSize);
        boolean idsOk = groundtruth.isConsistentWithDatasetSize(datasetSize);
        boolean dimsOk = (baseReader != null) && sampleDimsOk(baseReader);
        boolean sampleOk = (baseReader != null) &&
                sampleGtMatchesTrueNN(queries, groundtruth, baseReader,
                        Math.max(0, config.getRatio().gtSample),
                        Math.max(0.0, Math.min(1.0, config.getRatio().gtMismatchTolerance)));

        boolean gtTrusted = (baseReader != null) && idsOk && dimsOk && sampleOk;
        logger.info("Ratio trust gate: gtTrusted={}, idsOk={}, dimsOk={}, sampleOk={}",
                gtTrusted, idsOk, dimsOk, sampleOk);
        if (baseReader == null) {
            logger.warn("No baseReader: ratio computation will be NaN unless you set -Dbase.path");
        }

        ResultWriter rw = new ResultWriter(resultsDir.resolve("results_table.csv"));
        try {
            topKProfiler.setDatasetSize(datasetSize);
        } catch (Throwable ignore) {}

        final boolean doAudit = auditEnable && (retrievedAudit != null);

        // Base K for token (before k-adaptive widening)
        final int baseKForToken = Math.max(
                Math.max(auditK, 100),
                Arrays.stream(K_VARIANTS).max().orElse(100)
        );

        final int keepWorst = Math.max(0, auditWorstKeep);
        record WorstRec(int qIndex, int k, double ratio, double precision) {}
        PriorityQueue<WorstRec> worstPQ =
                new PriorityQueue<>(Comparator.comparingDouble(w -> w.ratio));

        Map<Integer, Long> globalMatches = new HashMap<>();
        Map<Integer, Long> globalReturned = new HashMap<>();
        for (int k : K_VARIANTS) {
            globalMatches.put(k, 0L);
            globalReturned.put(k, 0L);
        }
        final int queriesCount = queries.size();

        int indexedNow = indexService.getIndexedVectorCount();
        if (indexedNow <= 0) logger.warn("No points in memory before querying! Continuing for diagnostics.");
        int dimCountMem = indexService.getVectorCountForDimension(dim);
        if (dimCountMem <= 0) logger.warn("No points registered for dim={} before querying. Continuing anyway.", dim);

        if (!queries.isEmpty()) computeAndLogFanoutForQ0(queries.get(0), dim, K_VARIANTS);

        // --- system-property based K-adaptive controls ---
        final boolean kAdaptiveEnabled = Boolean.parseBoolean(
                System.getProperty("search.kadaptive.enabled", "false")
        );
        final double normLow = Double.parseDouble(
                System.getProperty("search.kadaptive.norm.low", "0.5")
        );
        final double normHigh = Double.parseDouble(
                System.getProperty("search.kadaptive.norm.high", "2.0")
        );
        final double widenLow = Double.parseDouble(
                System.getProperty("search.kadaptive.widen.low", "1.0")
        );
        final double widenHigh = Double.parseDouble(
                System.getProperty("search.kadaptive.widen.high", "2.0")
        );
        final int minKAdaptive = Integer.parseInt(
                System.getProperty("search.kadaptive.minK", "32")
        );
        final int maxKAdaptive = Integer.parseInt(
                System.getProperty(
                        "search.kadaptive.maxK",
                        Integer.toString(Math.max(baseKForToken, 1024))
                )
        );
        // -------------------------------------------------

        for (int q = 0; q < queries.size(); q++) {
            final int qIndex = q;

            double[] queryVec = queries.get(q);

            // ---- per-query K-adaptive widening ----
            int adaptiveKForToken = baseKForToken;
            if (kAdaptiveEnabled) {
                double norm = 0.0;
                for (double v : queryVec) norm += v * v;
                norm = Math.sqrt(norm);

                double factor;
                if (norm <= normLow) {
                    factor = widenLow;
                } else if (norm >= normHigh) {
                    factor = widenHigh;
                } else {
                    double t = (norm - normLow) / Math.max(1e-9, (normHigh - normLow));
                    factor = widenLow + t * (widenHigh - widenLow);
                }

                int candidateK = (int) Math.round(baseKForToken * factor);
                if (candidateK < minKAdaptive) candidateK = minKAdaptive;
                if (candidateK > maxKAdaptive) candidateK = maxKAdaptive;
                adaptiveKForToken = candidateK;
            }
            // ----------------------------------------

            long clientStart = System.nanoTime();
            QueryToken baseToken = factoryForDim(dim).create(queryVec, adaptiveKForToken);
            List<QueryResult> baseReturned = queryService.search(baseToken);
            long clientEnd = System.nanoTime();

            QueryServiceImpl qs = (QueryServiceImpl) queryService;
            double clientMs = (clientEnd - clientStart) / 1_000_000.0;
            double serverMs = boundedServerMs(qs, clientStart, clientEnd);

            ReencOutcome rep = maybeReencryptTouched("Q" + q, qs);
            int svcCum = ((QueryServiceImpl) queryService).getLastTouchedCumulativeUnique();
            if (svcCum != rep.cumulativeUnique) {
                logger.debug("q{} touch-set divergence: svc={} sys={}", qIndex, svcCum, rep.cumulativeUnique);
            }
            int touchedCount = (qs.getLastCandidateIds() != null) ? qs.getLastCandidateIds().size() : 0;
            var last = qs.getLastCandidateIds();
            if (last != null && !last.isEmpty()) {
                touchedGlobal.addAll(last);
            }
            com.fspann.common.EncryptedPointBuffer buf = indexService.getPointBuffer();
            long insertTimeMs = buf != null ? buf.getLastBatchInsertTimeMs() : 0;
            int totalFlushed = buf != null ? buf.getTotalFlushedPoints() : 0;
            int flushThreshold = buf != null ? buf.getFlushThreshold() : 0;
            int tokenSizeBytes = QueryServiceImpl.estimateTokenSizeBytes(baseToken);
            int vectorDim = baseToken.getDimension();

            int candTotal = qs.getLastCandTotal();
            int candKeptVersion = qs.getLastCandKeptVersion();
            int candDecrypted = qs.getLastCandDecrypted();
            int returned = qs.getLastReturned();

            guardCandidateInvariants(qIndex, candTotal, candKeptVersion, candDecrypted, returned);

            if (returned > 0) {
                double fanout = candTotal / (double) returned;
                double fanoutWarn = Double.parseDouble(System.getProperty("guard.fanout.warn", "2000"));
                if (fanout > fanoutWarn) {
                    logger.warn("q{} excessive fanout: scanned/returned ≈ {} ({} / {})",
                            qIndex, String.format(Locale.ROOT, "%.1f", fanout), candTotal, returned);
                }
            }

            int Kmax = Arrays.stream(K_VARIANTS).max().orElse(100);
            int uptoMax = Math.min(Kmax, baseReturned.size());
            double rr = (Kmax > 0) ? (double) uptoMax / (double) Kmax : 0.0;
            double minRR = Double.parseDouble(System.getProperty("guard.returnrate.min", "0.70"));
            if (rr < minRR) {
                logger.warn("q{} low return-rate at Kmax={}: returned={} ({}% of K). Consider increasing probes/tables.",
                        qIndex, Kmax, uptoMax, String.format(Locale.ROOT, "%.0f", 100.0 * rr));
            }

            List<QueryEvaluationResult> enriched = new ArrayList<>(K_VARIANTS.length);
            for (int k : K_VARIANTS) {
                int upto = Math.min(k, baseReturned.size());
                List<QueryResult> prefix = baseReturned.subList(0, upto);

                double distRatio = (baseReader == null) ? Double.NaN
                        : ratioAtKMetric(queryVec, prefix, k, qIndex, groundtruth, baseReader, gtTrusted);
                int[] truth = groundtruth.getGroundtruth(q, k);

                int matches = 0;
                for (int i = 0; i < prefix.size() && i < truth.length; i++) {
                    int id;
                    try {
                        id = Integer.parseInt(prefix.get(i).getId());
                    } catch (NumberFormatException nfe) {
                        continue;
                    }
                    if (containsInt(truth, id)) matches++;
                }
                double precAtK = (k > 0 ? (double) matches / (double) k : 0.0);

                globalMatches.put(k, globalMatches.get(k) + matches);
                globalReturned.put(k, globalReturned.get(k) + upto);

                String candMode = (candTotal >= 0 && candKeptVersion >= 0 && candDecrypted >= 0 && returned >= 0)
                        ? "full" : "partial";

                enriched.add(new QueryEvaluationResult(
                        k, upto, distRatio, precAtK,
                        Math.round(serverMs),
                        insertTimeMs,
                        candDecrypted,
                        tokenSizeBytes, vectorDim,
                        totalFlushed, flushThreshold,
                        /* touchedCount      */ touchedCount,
                        /* reencryptedCount  */ rep.rep.reencryptedCount,
                        /* reencTimeMs       */ rep.rep.timeMs,
                        /* reencBytesDelta   */ rep.rep.bytesDelta,
                        /* reencBytesAfter   */ rep.rep.bytesAfter,
                        /* ratioDenomSource  */ ratioDenomLabel(gtTrusted),
                        /* clientTimeMs      */ Math.round(clientMs),
                        /* tokenK            */ adaptiveKForToken,
                        /* tokenKBase        */ baseKForToken,
                        /* qIndexZeroBased   */ qIndex,
                        /* candMetricsMode   */ candMode
                ));
            }

            topKProfiler.record("Q" + q, enriched, candTotal, candKeptVersion, candDecrypted, returned);

            if (doAudit) {
                final int kAudit = Math.max(1, auditK);
                final int uptoA = Math.min(kAudit, baseReturned.size());
                final List<QueryResult> prefixA = baseReturned.subList(0, uptoA);
                final int[] truthA = groundtruth.getGroundtruth(qIndex, kAudit);

                final double ratioA = (baseReader == null) ? Double.NaN
                        : ratioAtKMetric(queryVec, prefixA, kAudit, qIndex, groundtruth, baseReader, gtTrusted);

                int hits = 0;
                for (int i = 0; i < uptoA && i < truthA.length; i++) {
                    try {
                        int id = Integer.parseInt(prefixA.get(i).getId());
                        if (containsInt(truthA, id)) hits++;
                    } catch (NumberFormatException ignore) {
                    }
                }
                final double precA = (kAudit > 0 ? (double) hits / (double) kAudit : 0.0);

                if (auditSampleEvery > 0 && (qIndex % auditSampleEvery) == 0) {
                    try {
                        retrievedAudit.appendSample(qIndex, kAudit, ratioA, precA, prefixA, truthA);
                    } catch (IOException ioe) {
                        logger.warn("Audit sample write failed q={},k={}", qIndex, kAudit, ioe);
                    }
                }
                if (auditWorstKeep > 0 && !Double.isNaN(ratioA)) {
                    worstPQ.offer(new WorstRec(qIndex, kAudit, ratioA, precA));
                    if (worstPQ.size() > keepWorst) worstPQ.poll();
                }
            }

            // make captured vars effectively final for the lambda
            final int qIndexF = qIndex;
            final int candTotalF = candTotal;
            final int candKeptVersionF = candKeptVersion;
            final int candDecryptedF = candDecrypted;
            final int returnedF = returned;

            rw.writeTable("Query " + (q + 1) + " Results (dim=" + dim + ")",
                    new String[]{
                            "QIndex0", "TopK", "Returned", "Ratio", "Precision", "RatioDenomSource",
                            "ServerTimeMs", "ClientTimeMs", "OverheadMs", "InsertTimeMs",
                            "ScannedCandidates", "CandKeptVersion", "CandDecrypted", "ReturnedAgain",
                            "TokenSize", "TokenK", "VectorDim", "TotalFlushed", "FlushThreshold",
                            "CandMetricsMode", "TokenKBase"
                    },
                    enriched.stream().map(er -> new String[]{
                            String.valueOf(qIndexF),
                            String.valueOf(er.getTopKRequested()),
                            String.valueOf(er.getRetrieved()),
                            (Double.isNaN(er.getRatio()) ? "NaN" :
                                    String.format(Locale.ROOT, "%.4f", er.getRatio())),
                            String.format(Locale.ROOT, "%.4f", er.getPrecision()),
                            (gtTrusted ? "gt" : "base"),
                            String.valueOf(er.getTimeMs()),
                            String.valueOf(er.getClientTimeMs()),
                            String.valueOf(Math.max(0, er.getClientTimeMs() - er.getTimeMs())),
                            String.valueOf(er.getInsertTimeMs()),
                            String.valueOf(candTotalF),
                            String.valueOf(candKeptVersionF),
                            String.valueOf(candDecryptedF),
                            String.valueOf(returnedF),
                            String.valueOf(er.getTokenSizeBytes()),
                            String.valueOf(er.getTokenK()),
                            String.valueOf(er.getVectorDim()),
                            String.valueOf(er.getTotalFlushedPoints()),
                            String.valueOf(er.getFlushThreshold()),
                            er.getCandMetricsMode(),
                            String.valueOf(er.getTokenKBase())
                    }).collect(Collectors.toList())
            );

            QueryEvaluationResult atK = enriched.stream()
                    .filter(e -> e.getTopKRequested() == Math.max(auditK, 1))
                    .findFirst().orElseGet(() -> enriched.get(enriched.size() - 1));

            if (profiler != null) profiler.recordQueryMetric("Q" + q, serverMs, clientMs, atK.getRatio());
            totalQueryTimeNs += Math.max(0L, clientEnd - clientStart);

            if (q < 3 && !baseReturned.isEmpty() && baseReader != null) {
                int topId = -1;
                try {
                    topId = Integer.parseInt(baseReturned.get(0).getId());
                } catch (Exception ignore) {
                }
                int gt1 = -1;
                int[] row = groundtruth.getGroundtruth(q, 1);
                if (row.length > 0) gt1 = row[0];
                double dTop = (topId >= 0) ? baseReader.l2(queryVec, topId) : Double.NaN;
                double dGt1 = (gt1 >= 0) ? baseReader.l2(queryVec, gt1) : Double.NaN;
                logger.info("Sanity q{}: topId={} dTop={}; gt1={} dGt1={}", q, topId, dTop, gt1, dGt1);
            }
        }

        if (writeGlobalPrecisionCsv && computePrecision) {
            writeGlobalPrecisionCsv(
                    resultsDir, dim, K_VARIANTS,
                    globalMatches, globalReturned, queriesCount
            );
        }

        if (doAudit && !worstPQ.isEmpty()) {
            List<WorstRec> worst = new ArrayList<>(worstPQ);
            worst.sort(Comparator.comparingDouble((WorstRec w) -> w.ratio).reversed());
            for (WorstRec w : worst) {
                double[] qv = queries.get(w.qIndex);
                QueryToken tk = factoryForDim(dim).create(qv, w.k);
                List<QueryResult> ret = queryService.search(tk);
                int[] truth = groundtruth.getGroundtruth(w.qIndex, w.k);
                double ratio = (baseReader == null) ? Double.NaN
                        : ratioAvgOverRanks(qv, ret.subList(0, Math.min(w.k, ret.size())), truth, baseReader);
                try {
                    retrievedAudit.appendWorst(w.qIndex, w.k, ratio, w.precision, ret, truth);
                } catch (IOException ioe) {
                    logger.warn("Audit worst write failed q={},k={}", w.qIndex, w.k, ioe);
                }
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

    public int restoreIndexFromDisk(int version) throws IOException {
        logger.info("Restoring in-memory index from metadata for v{} ...", version);
        int restored = 0;
        boolean prevWT = indexService.isWriteThrough();
        indexService.setWriteThrough(false);
        try {
            for (EncryptedPoint ep : metadataManager.getAllEncryptedPoints()) {
                if (ep == null || ep.getVersion() != version) continue;
                indexService.addPointToIndexOnly(ep);
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

    private static class StringKeyedCache extends ConcurrentHashMap<String, List<QueryResult>> {
        private final int maxSize;
        private final ConcurrentMap<String, Long> timestamps = new ConcurrentHashMap<>();
        private static final long CACHE_EXPIRY_MS = 10 * 60 * 1000;

        StringKeyedCache(int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        public List<QueryResult> put(String key, List<QueryResult> value) {
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
                timestamps.put((String) key, System.currentTimeMillis());
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

    public void setQueryOnlyMode(boolean b) {
        this.queryOnlyMode = b;
    }

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

    /** Empirically compute candidate fanout ratios for a single query vector. */
    private Map<Integer, Double> computeAndLogFanoutForQ0(double[] q0, int dim, int[] kVariants) {
        Map<Integer, Double> fanout = new LinkedHashMap<>();
        if (q0 == null || kVariants == null || kVariants.length == 0) return fanout;

        int baseK = Math.max(100, Arrays.stream(kVariants).max().orElse(100));
        QueryToken baseToken = factoryForDim(dim).create(q0, baseK);
        QueryServiceImpl qs = (QueryServiceImpl) queryService;

        // One real search only
        List<QueryResult> retMax = queryService.search(baseToken);
        int candTotalOnce = qs.getLastCandTotal();
        int returnedMax = (retMax != null ? retMax.size() : 0);

        if (returnedMax <= 0) {
            logger.warn("Fanout probe skipped: returnedMax=0 for baseK={}", baseK);
            return fanout;
        }

        for (int k : kVariants) {
            int returnedK = Math.min(k, returnedMax);
            double ratio = (returnedK == 0) ? Double.NaN : ((double) candTotalOnce / (double) returnedK);
            fanout.put(k, ratio);
        }

        boolean partitioned = (config.getPaper() != null && config.getPaper().enabled);
        if (partitioned) {
            var pc = config.getPaper();
            logger.info("Fanout(Q0, approx, partitioned; m={}, λ={}, ℓ={}): {}",
                    pc.m, pc.lambda, pc.divisions, fanout);
        } else {
            int L = -1, mRows = -1;
            try {
                EvenLSH lsh = indexService.getLshForDimension(dim);
                try {
                    L = (int) lsh.getClass().getMethod("getNumTables").invoke(lsh);
                } catch (Throwable ignore) {
                }
                try {
                    mRows = (int) lsh.getClass().getMethod("getRowsPerBand").invoke(lsh);
                } catch (Throwable e1) {
                    try {
                        mRows = (int) lsh.getClass().getMethod("getNumHashFuncsPerTable").invoke(lsh);
                    } catch (Throwable e2) {
                        /* ignore */
                    }
                }
            } catch (Throwable ignore) {
            }
            int shards = shardsToProbe();
            logger.info("Fanout(Q0, approx, multiprobe; L={}, m={}, probeShards={}): {}",
                    L, mRows, shards, fanout);
        }
        return fanout;
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

        double l2sq(double[] q, int id) {
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
                return sum;
            } else {
                try {
                    long off = (long) id * (long) recordBytes;
                    ByteBuffer buf = tlBuf.get();
                    buf.clear();
                    int n = ch.read(buf, off);
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
                    return sum;
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
        }

        double l2(double[] q, int id) {
            return Math.sqrt(l2sq(q, id));
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

        boolean storedDimOk(int id) {
            return storedDim(id) == this.dim;
        }

        @Override
        public void close() throws IOException {
            ch.close();
        }
    }

    private static final class DistId {
        final double dsq;
        final int id;

        DistId(double dsq, int id) {
            this.dsq = dsq;
            this.id = id;
        }
    }

    private static int[] topKFromBase(BaseVectorReader base, double[] q, int k) {
        PriorityQueue<DistId> pq = new PriorityQueue<>((a, b) -> {
            int cmp = Double.compare(b.dsq, a.dsq);  // larger distance first
            return (cmp != 0) ? cmp : Integer.compare(b.id, a.id);
        });
        for (int id = 0; id < base.count; id++) {
            double dsq = base.l2sq(q, id);
            if (pq.size() < k) pq.offer(new DistId(dsq, id));
            else if (dsq < pq.peek().dsq || (dsq == pq.peek().dsq && id < pq.peek().id)) {
                pq.poll();
                pq.offer(new DistId(dsq, id));
            }
        }
        List<DistId> items = new ArrayList<>(pq);
        items.sort((x, y) -> {
            int cmp = Double.compare(x.dsq, y.dsq);
            return (cmp != 0) ? cmp : Integer.compare(x.id, y.id);
        });
        int[] out = new int[items.size()];
        for (int i = 0; i < items.size(); i++) out[i] = items.get(i).id;
        return out;
    }

    /** Dump profiler CSVs, query list, and copies of keys/metadata/points to outDir. */
    public void exportArtifacts(Path outDir) throws IOException {
        Files.createDirectories(outDir);

        // --- profiler csvs / summaries ---
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
            var ratio = profiler.getAllQueryRatios();
            double art = IntStream.range(0, Math.min(client.size(), server.size()))
                    .mapToDouble(i -> client.get(i) + server.get(i))
                    .average().orElse(0.0);
            double avgRatio = ratio.stream().mapToDouble(Double::doubleValue).average().orElse(0);

            String mode = (config.getPaper() != null && config.getPaper().enabled) ? "partitioned" : "multiprobe";
            String cfgHash = "NA";
            try {
                Path cfgPath = Paths.get(this.configPath);
                if (Files.exists(cfgPath)) {
                    byte[] bytes = Files.readAllBytes(cfgPath);
                    MessageDigest md = MessageDigest.getInstance("SHA-256");
                    cfgHash = toHex(md.digest(bytes));
                }
            } catch (Exception ignore) {
            }

            int keyVer = -1;
            try {
                keyVer = keyService.getCurrentVersion().getVersion();
            } catch (Exception ignore) {
            }

            Files.writeString(outDir.resolve("metrics_summary.txt"),
                    String.format(Locale.ROOT,
                            "mode=%s  config_sha256=%s  key_version=v%d%nART(ms)=%.3f%nAvgRatio=%.6f%n",
                            mode, cfgHash, keyVer, art, avgRatio),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        try {
            topKProfiler.export(outDir.resolve("topk_evaluation.csv").toString());
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

        // === Storage summary (mode-aware) ===
        try {
            final int version = keyService.getCurrentVersion().getVersion();
            final int N = Math.max(indexService.getIndexedVectorCount(), totalInserted);

            int dimReport = -1;
            if (!tokenFactories.isEmpty()) {
                try {
                    dimReport = tokenFactories.keySet().iterator().next();
                } catch (Exception ignore) {
                }
            }

            boolean partitioned = (config.getPaper() != null && config.getPaper().enabled);

            int L = -1;
            int m_rows = -1;
            int ell = -1;
            int m_proj = -1;

            if (partitioned) {
                var pc = config.getPaper();
                ell = pc.divisions;
                m_proj = pc.m;
            } else {
                try {
                    if (dimReport > 0) {
                        EvenLSH lsh = indexService.getLshForDimension(dimReport);
                        try {
                            L = (int) lsh.getClass().getMethod("getNumTables").invoke(lsh);
                        } catch (Throwable ignore) {
                        }
                        if (L <= 0) L = numTablesFor(lsh, config);
                        try {
                            m_rows = (int) lsh.getClass().getMethod("getRowsPerBand").invoke(lsh);
                        } catch (Throwable e1) {
                            try {
                                m_rows = (int) lsh.getClass().getMethod("getNumHashFuncsPerTable").invoke(lsh);
                            } catch (Throwable e2) {
                                /* leave -1 */
                            }
                        }
                    }
                } catch (Throwable ignore) {
                }
            }

            final int shards = shardsToProbe();

            Path pointsVerDir = pointsPath.resolve("v" + version);
            long bytesPoints = dirSize(pointsVerDir);
            long bytesMeta = dirSize(metaDBPath);
            long bytesKeys = Files.exists(keyStorePath) ? safeSize(keyStorePath) : dirSize(keyStorePath.getParent());
            long totalBytes = bytesPoints + bytesMeta + bytesKeys;
            double bytesPerPt = (N > 0 ? (double) totalBytes / (double) N : Double.NaN);

            long indexTerms_LxN = (partitioned || L < 0) ? -1 : (long) L * (long) N;
            long vectorTerms_Nxd = (dimReport > 0 ? (long) N * (long) dimReport : -1);

            Path csv = outDir.resolve("storage_summary.csv");
            if (!Files.exists(csv)) {
                Files.writeString(csv,
                        "mode,version,dataset_size,dim,num_tables_L,divisions_ell,lsh_m_rows_per_table," +
                                "paper_m_projections,probe_shards,points_bytes,metadata_bytes,keystore_bytes," +
                                "total_bytes,bytes_per_point,index_terms_LxN,vector_terms_Nxd\n",
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            String row = String.format(Locale.ROOT,
                    "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.6f,%d,%d%n",
                    (partitioned ? "partitioned" : "multiprobe"),
                    version, N, dimReport,
                    (partitioned ? -1 : L),
                    (partitioned ? ell : -1),
                    (partitioned ? -1 : m_rows),
                    (partitioned ? m_proj : -1),
                    shards,
                    bytesPoints, bytesMeta, bytesKeys, totalBytes, bytesPerPt,
                    indexTerms_LxN, vectorTerms_Nxd);
            Files.writeString(csv, row, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            Path txt = outDir.resolve("storage_breakdown.txt");
            StringBuilder pretty = new StringBuilder();
            pretty.append("=== Storage Breakdown ===\n");
            pretty.append(String.format("Mode              : %s%n", (partitioned ? "partitioned" : "multiprobe")));
            pretty.append(String.format("Version           : v%d%n", version));
            pretty.append(String.format("Dataset Size (N)  : %d%n", N));
            pretty.append(String.format("Dimension (d)     : %d%n", dimReport));
            if (partitioned) {
                pretty.append(String.format("Paper divisions (ℓ)        : %d%n", ell));
                pretty.append(String.format("m (projections/division)   : %d%n", m_proj));
            } else {
                pretty.append(String.format("LSH tables (L)             : %d%n", L));
                pretty.append(String.format("m (rows per LSH table)     : %d%n", m_rows));
                pretty.append(String.format("Probe shards               : %d%n", shards));
            }
            pretty.append("\n");
            pretty.append(String.format("Points bytes      : %d%n", bytesPoints));
            pretty.append(String.format("Metadata bytes    : %d%n", bytesMeta));
            pretty.append(String.format("Keystore bytes    : %d%n", bytesKeys));
            pretty.append("--------------------------\n");
            pretty.append(String.format("TOTAL bytes       : %d%n", totalBytes));
            pretty.append(String.format("Bytes / point     : %.6f%n", bytesPerPt));
            pretty.append("\n");
            pretty.append("(Symbolic terms)\n");
            pretty.append(String.format("L·N (index terms) : %d%n", indexTerms_LxN));
            pretty.append(String.format("N·d (vector terms): %d%n", vectorTerms_Nxd));
            pretty.append("\n");
            Files.writeString(txt, pretty.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            Path readme = outDir.resolve("README_results_columns.txt");
            if (!Files.exists(readme)) {
                Files.writeString(readme,
                        """
                        Files & units:
                        - storage_summary.csv
                          * mode: partitioned|multiprobe
                          * *_bytes: long bytes; bytes_delta may be negative (compaction).
                        - storage_breakdown.txt: human-readable mirror of the CSV (mode-aware L vs ℓ, and m label).
                        - metrics_summary.txt:
                          * First line includes mode, config SHA-256, active key version.
                          * ART(ms) and AvgRatio are aggregates over the run.
                        - reencrypt_metrics.csv:
                          * TimeMs is long milliseconds. Bytes* are long bytes. BytesDelta may be negative.
                        """,
                        StandardOpenOption.CREATE_NEW);
            }

            logger.info("Storage summary written: {}, {}", csv, txt);
        } catch (Exception e) {
            logger.warn("Failed to write storage summary", e);
        }
    }

    /** Append global precision (single value) and return-rate to results/global_precision.csv */
    private static void writeGlobalPrecisionCsv(
            Path outDir,
            int dim,
            int[] kVariants,
            Map<Integer, Long> globalMatches,
            Map<Integer, Long> globalRetrieved,
            int queriesCount
    ) {
        try {
            Files.createDirectories(outDir);
            Path p = outDir.resolve("global_precision.csv");
            if (!Files.exists(p)) {
                Files.writeString(p,
                        "dimension,topK,precision,return_rate,matches_sum,returned_sum,queries\n",
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }

            try (var w = Files.newBufferedWriter(p, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                for (int k : kVariants) {
                    long retrievedSum = globalRetrieved.getOrDefault(k, 0L);
                    long matchesSum = globalMatches.getOrDefault(k, 0L);

                    // Single global precision across all queries at this K:
                    // precision = total matches / total retrieved (micro-style)
                    double precision = (retrievedSum == 0L)
                            ? 0.0
                            : (double) matchesSum / (double) retrievedSum;

                    double returnRate = (queriesCount <= 0 || k <= 0)
                            ? 0.0
                            : (double) retrievedSum / ((double) queriesCount * (double) k);

                    w.write(String.format(Locale.ROOT,
                            "%d,%d,%.6f,%.6f,%d,%d,%d%n",
                            dim, k, precision, returnRate, matchesSum, retrievedSum, queriesCount));

                    logger.info(
                            "Global@K={} (dim={}): precision={} returnRate={} [matches={}, returned={}, Q={}]",
                            k, dim,
                            String.format(Locale.ROOT, "%.6f", precision),
                            String.format(Locale.ROOT, "%.6f", returnRate),
                            matchesSum, retrievedSum, queriesCount
                    );
                }
            }
        } catch (IOException ioe) {
            logger.warn("Failed to write global_precision.csv", ioe);
        }
    }

    private TrueNN computeTrueNNFromBase(double[] q, BaseVectorReader br) {
        double bestSq = Double.POSITIVE_INFINITY;
        int bestId = -1;
        for (int i = 0; i < br.count; i++) {
            double dsq = br.l2sq(q, i);
            if (dsq < bestSq) {
                bestSq = dsq;
                bestId = i;
            }
        }
        return new TrueNN(bestId, Math.sqrt(bestSq));
    }

    /** Compute ratio@K(q) using ranked lists: average over i=1..upto of sqrt(d2(ret_i)/d2(gt_i)). */
    private static double ratioAtKFromLists(double[] q,
                                            List<QueryResult> retPrefix,
                                            int k,
                                            int[] gtTopK,
                                            BaseVectorReader br) {
        final double eps = 1e-24;
        final int upto = Math.min(k, Math.min(retPrefix.size(), gtTopK.length));
        if (upto <= 0) return 1.0;
        double sum = 0.0;
        int used = 0;
        for (int i = 0; i < upto; i++) {
            int ridx;
            try {
                ridx = Integer.parseInt(retPrefix.get(i).getId());
            } catch (NumberFormatException nfe) {
                continue;
            }
            double d2Num = br.l2sq(q, ridx);
            double d2Den = br.l2sq(q, gtTopK[i]);
            if (!Double.isFinite(d2Num) || !Double.isFinite(d2Den)) continue;
            if (d2Den <= eps) {
                sum += (d2Num <= eps) ? 1.0 : 1.0;
            } else {
                sum += Math.sqrt(d2Num / d2Den);
            }
            used++;
        }
        return (used == 0) ? 1.0 : (sum / used);
    }

    /** Dispatcher that chooses GT vs BASE for the denominator list and computes the per-rank average ratio. */
    private double ratioAtKMetric(double[] q,
                                  List<QueryResult> retPrefix,
                                  int k,
                                  int qIndex,
                                  GroundtruthManager gt,
                                  BaseVectorReader br,
                                  boolean gtTrusted) {
        if (br == null || retPrefix == null || retPrefix.isEmpty() || k <= 0) {
            return Double.NaN;
        }
        int[] gtTopK;
        switch (this.ratioSource) {
            case GT -> {
                gtTopK = gt.getGroundtruth(qIndex, k);
                if (gtTopK.length == 0) return Double.NaN;
                return ratioAtKFromLists(q, retPrefix, k, gtTopK, br);
            }
            case AUTO -> {
                if (gtTrusted) {
                    gtTopK = gt.getGroundtruth(qIndex, k);
                    if (gtTopK.length == 0) return Double.NaN;
                    return ratioAtKFromLists(q, retPrefix, k, gtTopK, br);
                } else {
                    int[] baseTopK = topKFromBase(br, q, k);
                    return ratioAtKFromLists(q, retPrefix, k, baseTopK, br);
                }
            }
            case BASE -> {
                int[] baseTopK = topKFromBase(br, q, k);
                return ratioAtKFromLists(q, retPrefix, k, baseTopK, br);
            }
            default -> {
                return Double.NaN;
            }
        }
    }

    // Paper-accurate ratio@K (average over ranks 1..K).
    private static double ratioAvgOverRanks(double[] q,
                                            List<QueryResult> retrievedPrefix,
                                            int[] truthTopK,
                                            BaseVectorReader br) {
        if (q == null || br == null || retrievedPrefix == null || truthTopK == null) return Double.NaN;
        final int upto = Math.min(retrievedPrefix.size(), truthTopK.length);
        if (upto <= 0) return Double.NaN;

        double sum = 0.0;
        int terms = 0;

        for (int i = 0; i < upto; i++) {
            QueryResult qr = retrievedPrefix.get(i);
            int rIdx;
            try {
                rIdx = Integer.parseInt(qr.getId());
            } catch (NumberFormatException ignored) {
                continue;
            }

            int oIdx = truthTopK[i];

            if (rIdx == oIdx) {
                sum += 1.0;
                terms++;
                continue;
            }

            double numSq = br.l2sq(q, rIdx);
            double denSq = br.l2sq(q, oIdx);
            if (!(Double.isFinite(numSq) && Double.isFinite(denSq))) continue;

            if (denSq <= RATIO_EPS) {
                sum += 1.0;
            } else {
                if (numSq < 0) numSq = 0.0;
                sum += Math.sqrt(numSq / denSq);
            }
            terms++;
        }
        return (terms == 0) ? Double.NaN : (sum / terms);
    }

    private static int numTablesFor(EvenLSH lsh, SystemConfig cfg) {
        try {
            var m = lsh.getClass().getMethod("getNumTables");
            Object v = m.invoke(lsh);
            if (v instanceof Integer n && n > 0) return n;
        } catch (Throwable ignore) {
        }

        try {
            var m = cfg.getClass().getMethod("getNumTables");
            Object v = m.invoke(cfg);
            if (v instanceof Integer n && n > 0) return n;
        } catch (Throwable ignore) {
        }

        return 4; // fallback
    }

    private int shardsToProbe() {
        int cfgVal = config.getLsh() != null ? config.getLsh().getProbeShards() : 0;
        String prop = System.getProperty("probe.shards");
        if (prop != null) {
            try {
                int v = Integer.parseInt(prop);
                if (v > 0) return v;
            } catch (NumberFormatException ignore) {}
        }
        if (cfgVal > 0) return cfgVal;
        return Math.max(1, config.getNumShards() / 4); // or whatever baseline you like
    }

    private boolean sampleDimsOk(BaseVectorReader br) {
        int[] probes = {0, Math.max(0, br.count / 2), Math.max(0, br.count - 1)};
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
            if (row.length == 0) {
                mismatches++;
                continue;
            }
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

    /** Disable System exit on shutdown (for tests). */
    public void setExitOnShutdown(boolean exitOnShutdown) {
        this.exitOnShutdown = exitOnShutdown;
    }

    private static boolean propOr(boolean defaultVal, String... keys) {
        for (String k : keys) {
            String v = System.getProperty(k);
            if (v != null) {
                String s = v.trim().toLowerCase(Locale.ROOT);
                return s.equals("true") || s.equals("1") || s.equals("yes") || s.equals("y");
            }
        }
        return defaultVal;
    }

    /**
     * Build a deterministic cache key from a QueryToken.
     * - Uses only logical fields (version, dim, topK, codes/buckets)
     * - Ignores IV/ciphertext so the same logical query hits the cache.
     */
    private String cacheKeyOf(QueryToken token) {
        if (token == null) return "null";

        StringBuilder sb = new StringBuilder(128);

        // Version + dimension + topK
        sb.append("v").append(token.getVersion())
                .append(":d").append(token.getDimension())
                .append(":k").append(token.getTopK());

        // Prefer partitioned-mode codes if present
        BitSet[] codes = token.getCodes();
        if (codes != null && codes.length > 0) {
            sb.append(":codes");
            for (BitSet bs : codes) {
                if (bs == null) {
                    sb.append("|-1");
                } else {
                    sb.append('|').append(bs.hashCode());
                }
            }
        } else {
            // Fallback: LSH per-table bucket hashes
            sb.append(":buckets");
            List<List<Integer>> tables = token.getTableBuckets();
            if (tables != null) {
                for (List<Integer> table : tables) {
                    if (table == null || table.isEmpty()) {
                        sb.append("|0");
                    } else {
                        int h = 1;
                        for (Integer v : table) {
                            h = 31 * h + (v == null ? 0 : v.hashCode());
                        }
                        sb.append('|').append(h);
                    }
                }
            }
        }

        return sb.toString();
    }

    private static double scanTrueNNSq(double[] q, BaseVectorReader br) {
        double best = Double.POSITIVE_INFINITY;
        for (int i = 0; i < br.count; i++) {
            double d2 = br.l2sq(q, i);
            if (d2 < best) best = d2;
        }
        return best;
    }

    // --- storage helpers ---
    private static long safeSize(Path p) {
        try {
            return Files.size(p);
        } catch (Exception ignore) {
            return 0L;
        }
    }

    private static long dirSize(Path root) {
        if (root == null || !Files.exists(root)) return 0L;
        try (var stream = Files.walk(root)) {
            return stream.filter(Files::isRegularFile).mapToLong(ForwardSecureANNSystem::safeSize).sum();
        } catch (IOException e) {
            logger.warn("Failed to size directory {}", root, e);
            return 0L;
        }
    }

    // Per-query hook: accumulate touched IDs; optionally do "immediate" re-encryption if requested.
    private ReencOutcome maybeReencryptTouched(String label, QueryServiceImpl qs) {
        boolean live = "live".equalsIgnoreCase(System.getProperty("reenc.mode", "end"));
        int touchedUnique = reencTracker.uniqueCount();
        ReencReport rep = ReencReport.empty();

        if (live && !reencRan.get()
                && touchedUnique >= Integer.getInteger("reenc.minTouched", 10_000)) {

            long t0 = System.nanoTime();
            reencCoordinator.runOnceIfNeeded();    // drains tracker; meters record counts
            long ms = (System.nanoTime() - t0) / 1_000_000L;

            reencRan.set(true);
            rep = rep.withTimeMs(ms);
        }

        return new ReencOutcome(touchedUnique, rep);
    }

    // CSV helpers
    private static void initReencCsvIfNeeded(Path p) {
        try {
            Files.createDirectories(p.getParent());
            if (!Files.exists(p)) {
                Files.writeString(p,
                        "QueryID,TargetVersion,Mode,Touched,NewUnique,CumulativeUnique," +
                                "Reencrypted,AlreadyCurrent,Retried,TimeMs,BytesDelta,BytesAfter\n",
                        StandardOpenOption.CREATE_NEW);
            }
        } catch (IOException e) {
            LoggerFactory.getLogger(ForwardSecureANNSystem.class)
                    .warn("Failed to init re-encryption CSV at {}", p, e);
        }
    }

    /**
     * End-of-run re-encryption over the union of touched IDs.
     */
    private void finalizeReencryptionAtEnd() {
        boolean enabled = reencEnabled;
        try {
            var rc = config.getReencryption();
            if (rc != null) enabled = enabled && rc.enabled;
        } catch (Throwable ignore) { /* ok */ }

        final List<String> allTouchedUnique = new ArrayList<>(touchedGlobal);
        final int uniqueCount = allTouchedUnique.size();

        final KeyRotationServiceImpl kr =
                (keyService instanceof KeyRotationServiceImpl) ? (KeyRotationServiceImpl) keyService : null;

        if (!enabled || kr == null) {
            appendReencCsv(reencCsv, "SUMMARY",
                    (keyService != null && keyService.getCurrentVersion() != null)
                            ? keyService.getCurrentVersion().getVersion() : -1,
                    reencMode,
                    uniqueCount, 0, uniqueCount,
                    new ReencryptReport(0, 0, 0L, 0L, 0L));
            return;
        }

        if (uniqueCount == 0) {
            appendReencCsv(reencCsv, "SUMMARY",
                    keyService.getCurrentVersion().getVersion(),
                    reencMode,
                    0, 0, 0,
                    new ReencryptReport(0, 0, 0L, 0L, 0L));
            return;
        }

        kr.forceRotateNow();
        final int targetVer = keyService.getCurrentVersion().getVersion();

        com.fspann.common.StorageSizer sizer = () -> dirSize(pointsPath);
        ReencryptReport rep = kr.reencryptTouched(allTouchedUnique, targetVer, sizer);

        appendReencCsv(reencCsv, "SUMMARY", targetVer, reencMode,
                uniqueCount, 0, uniqueCount, rep);

        boolean ok = true;
        long measuredBytesAfter = dirSize(pointsPath);
        if (rep.reencryptedCount() < 0) ok = false;
        if (rep.bytesAfter() != measuredBytesAfter) ok = false;
        if (rep.touchedCount() != uniqueCount) ok = false;

        if (!ok) {
            try {
                synchronized (REENC_CSV_LOCK) {
                    String line = String.format(Locale.ROOT,
                            "SUMMARY_CHECK,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                            targetVer, reencMode,
                            uniqueCount, 0, uniqueCount,
                            rep.reencryptedCount(),
                            tryGetLong(rep, "alreadyCurrentCount"),
                            tryGetLong(rep, "retriedCount"),
                            rep.timeMs(), rep.bytesDelta(), measuredBytesAfter
                    );
                    Files.writeString(reencCsv, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
            } catch (IOException e) {
                logger.warn("Failed to append SUMMARY_CHECK", e);
            }
        }
    }

    private static void appendReencCsv(Path p, String qid, int ver, String modeStr,
                                       int touched, int newUnique, int cumulativeUnique,
                                       ReencryptReport r) {
        try {
            long alreadyCur = tryGetLong(r, "alreadyCurrentCount");
            long retried = tryGetLong(r, "retriedCount");
            synchronized (REENC_CSV_LOCK) {
                String line = String.format(Locale.ROOT,
                        "%s,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                        qid, ver, modeStr, touched, newUnique, cumulativeUnique,
                        r.reencryptedCount(), alreadyCur, retried, r.timeMs(), r.bytesDelta(), r.bytesAfter());
                Files.writeString(p, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            LoggerFactory.getLogger(ForwardSecureANNSystem.class)
                    .warn("Failed to append re-encryption CSV for {}", qid, e);
        }
    }

    public SecureLSHIndexService getIndexService() {
        return this.indexService;
    }

    public QueryService getQueryService() {
        return this.queryService;
    }

    public Profiler getProfiler() {
        return this.profiler;
    }

    private static String toHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static double erToDoubleMs(long ms) {
        return (double) ms;
    }

    private static long tryGetLong(Object o, String fieldName) {
        if (o == null) return 0L;
        try {
            var f = o.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            Object v = f.get(o);
            if (v instanceof Number n) return n.longValue();
        } catch (Throwable ignore) {
        }
        return 0L;
    }

    private static boolean containsInt(int[] a, int v) {
        for (int x : a) if (x == v) return true;
        return false;
    }

    private static void guardCandidateInvariants(int qIndex, int candTotal, int kept, int dec, int ret) {
        if (candTotal < kept) {
            logger.warn("q{} invariant: ScannedCandidates ({}) < KeptVersion ({})", qIndex, candTotal, kept);
        }
        if (kept < dec) {
            logger.warn("q{} invariant: KeptVersion ({}) < Decrypted ({})", qIndex, kept, dec);
        }
        if (dec < ret) {
            logger.warn("q{} invariant: Decrypted ({}) < Returned ({})", qIndex, dec, ret);
        }
        if (ret < 0 || dec < 0 || kept < 0 || candTotal < 0) {
            logger.warn("q{} invariant: negative counters [scanned={}, kept={}, dec={}, ret={}]",
                    qIndex, candTotal, kept, dec, ret);
        }
    }

    private String ratioDenomLabel(boolean gtTrusted) {
        return switch (this.ratioSource) {
            case GT -> "gt";
            case BASE -> "base";
            case AUTO -> (gtTrusted ? "gt(auto)" : "base(auto)");
        };
    }

    private static final class ReencReport {
        final int reencryptedCount;
        final long timeMs;
        final long bytesDelta;
        final long bytesAfter;

        ReencReport(int reencryptedCount, long timeMs, long bytesDelta, long bytesAfter) {
            this.reencryptedCount = reencryptedCount;
            this.timeMs = timeMs;
            this.bytesDelta = bytesDelta;
            this.bytesAfter = bytesAfter;
        }

        static ReencReport empty() {
            return new ReencReport(0, 0L, 0L, 0L);
        }

        ReencReport withTimeMs(long ms) {
            return new ReencReport(this.reencryptedCount, ms, this.bytesDelta, this.bytesAfter);
        }
    }

    private static final class ReencOutcome {
        final int cumulativeUnique;
        final ReencReport rep;

        ReencOutcome(int cumulativeUnique, ReencReport rep) {
            this.cumulativeUnique = cumulativeUnique;
            this.rep = rep;
        }
    }

    public void disableLSHForAllDimensions() {
        try {
            Field f = this.getClass().getDeclaredField("lshPerDim");
            f.setAccessible(true);
            Map<Integer, EvenLSH> map = (Map<Integer, EvenLSH>) f.get(this);
            if (map != null) map.clear();
        } catch (Throwable ignore) {}
    }

    /** Server ms, gently bounded to the client window. */
    private static double boundedServerMs(QueryServiceImpl qs, long clientStartNs, long clientEndNs) {
        final long clientWin = Math.max(0L, clientEndNs - clientStartNs);
        long server = Math.max(0L, qs.getLastQueryDurationNs());

        if (clientWin > 0 && server > clientWin * 1.10) {
            server = clientWin;
        }
        return server / 1_000_000.0;
    }

    private static String buildQueryCacheKey(double[] q, int topK, int dim) {
        int h = 1;
        for (double v : q) {
            long bits = Double.doubleToLongBits(v);
            h = 31 * h + (int)(bits ^ (bits >>> 32));
        }
        return dim + "|" + topK + "|" + h;
    }


    // ====== retrieved IDs auditor ====== //
    private static final class RetrievedAudit {
        private final Path samplesCsv;
        private final Path worstCsv;

        RetrievedAudit(Path outDir) throws IOException {
            Files.createDirectories(outDir);
            this.samplesCsv = outDir.resolve("retrieved_samples.csv");
            this.worstCsv = outDir.resolve("retrieved_worst.csv");
            writeHeader(samplesCsv);
            writeHeader(worstCsv);
        }

        private static void writeHeader(Path p) throws IOException {
            if (!Files.exists(p)) {
                Files.writeString(p, "qIndex,k,ratio,precision,retrieved_ids,groundtruth_ids\n");
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

        void appendSample(int qIndex, int k, double ratio, double precision,
                          List<QueryResult> retrieved, int[] truth) throws IOException {
            String line = String.format(Locale.ROOT, "%d,%d,%.6f,%.6f,%s,%s%n",
                    qIndex, k, ratio, precision, joinIds(retrieved), joinInts(truth));
            Files.writeString(samplesCsv, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }

        void appendWorst(int qIndex, int k, double ratio, double precision,
                         List<QueryResult> retrieved, int[] truth) throws IOException {
            String line = String.format(Locale.ROOT, "%d,%d,%.6f,%.6f,%s,%s%n",
                    qIndex, k, ratio, precision, joinIds(retrieved), joinInts(truth));
            Files.writeString(worstCsv, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    public void flushAll() throws IOException {
        com.fspann.common.EncryptedPointBuffer buf = indexService.getPointBuffer();
        if (buf != null) buf.flushAll();

        if (queryOnlyMode) {
            logger.info("Query-only mode: skipping metadata version save");
        } else {
            metadataManager.saveIndexVersion(keyService.getCurrentVersion().getVersion());
        }
    }

    public void shutdown() {
        long idxMs = Math.round(totalIndexingTimeNs / 1_000_000.0);
        long qryMs = Math.round(totalQueryTimeNs / 1_000_000.0);
        System.out.println("Total indexing time: " + idxMs + " ms");
        System.out.println("Total querying time: " + qryMs + " ms");
        try {
            flushAll();

            if ("end".equalsIgnoreCase(System.getProperty("reenc.mode", "end"))) {
                try {
                    finalizeReencryptionAtEnd();
                } catch (Exception e) {
                    logger.warn("finalizeReencryptionAtEnd failed", e);
                }
            }

            if (indexService != null) {
                indexService.flushBuffers();
                indexService.shutdown();
            }

            if (metadataManager != null) {
                metadataManager.printSummary();
                metadataManager.logStats();
                metadataManager.close();
            }

            try {
                if (baseReader != null) baseReader.close();
            } catch (Exception ignore) {
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
            System.gc();

            if (exitOnShutdown && !"true".equals(System.getProperty("test.env"))) {
                System.exit(0);
            }

            if (prevBaseProp == null)
                System.clearProperty(FsPaths.BASE_DIR_PROP);
            else
                System.setProperty(FsPaths.BASE_DIR_PROP, prevBaseProp);

            if (prevMetaProp == null)
                System.clearProperty(FsPaths.METADB_PROP);
            else
                System.setProperty(FsPaths.METADB_PROP, prevMetaProp);

            if (prevPointsProp == null)
                System.clearProperty(FsPaths.POINTS_PROP);
            else
                System.setProperty(FsPaths.POINTS_PROP, prevPointsProp);

            if (prevKeyStoreProp == null)
                System.clearProperty(FsPaths.KEYSTORE_PROP);
            else
                System.setProperty(FsPaths.KEYSTORE_PROP, prevKeyStoreProp);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: <configPath> <dataPath> <queryPath> <keysFilePath> <dimensions> <metadataPath> <groundtruthPath> [batchSize]");
            System.exit(1);
        }

        String configFile = args[0];
        String dataPath = args[1];
        String queryPath = args[2];
        String keysFile = args[3];
        List<Integer> dimensions = Arrays.stream(args[4].split(",")).map(Integer::parseInt).collect(Collectors.toList());
        Path metadataPath = Paths.get(args[5]);
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

        com.fspann.api.ApiSystemConfig apiCfg = new com.fspann.api.ApiSystemConfig(configFile);
        SystemConfig cfg = apiCfg.getConfig();

        Path baseVecs = Paths.get(dataPath);
        Path queryVecs = Paths.get(queryPath);

        int kMax = 100;
        try {
            int[] ks = (cfg.getEval() != null && cfg.getEval().kVariants != null) ? cfg.getEval().kVariants : null;
            if (ks != null && ks.length > 0) {
                for (int v : ks) if (v > kMax) kMax = v;
            }
        } catch (Exception ignore) {}

        boolean needAutoGT = "AUTO".equalsIgnoreCase(groundtruthPath) || !Files.exists(Paths.get(groundtruthPath));
        if (needAutoGT) {
            if (!"POINTS_ONLY".equalsIgnoreCase(dataPath)) {
                int threads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
                Path outGt = com.fspann.api.GroundtruthPrecompute.defaultOutputForQuery(queryVecs);
                try {
                    Path gtComputed = com.fspann.api.GroundtruthPrecompute.run(baseVecs, queryVecs, outGt, kMax, threads);
                    groundtruthPath = gtComputed.toString();
                    System.out.println("Groundtruth auto-precomputed at: " + groundtruthPath);
                } catch (Exception e) {
                    System.err.println("GT precompute failed: " + e.getMessage());
                    e.printStackTrace();
                    System.exit(2);
                }
            } else {
                System.err.println("GT requested AUTO but dataPath=POINTS_ONLY (no base vectors). Provide a GT file.");
                System.exit(2);
            }
        }
        if (!"POINTS_ONLY".equalsIgnoreCase(dataPath) && Files.exists(baseVecs)) {
            System.setProperty("base.path", baseVecs.toString());
        }
        int opsCap = (int) Math.min(Integer.MAX_VALUE, cfg.getOpsThreshold());
        long ageMs = cfg.getAgeThresholdMs();

        KeyRotationPolicy policy = new KeyRotationPolicy(
                queryOnly ? Integer.MAX_VALUE : opsCap,
                queryOnly ? Long.MAX_VALUE : ageMs
        );

        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, null);
        CryptoService cryptoService =
                new com.fspann.crypto.AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        if (queryOnly && restoreVersion > 0) {
            boolean ok = keyService.activateVersion(restoreVersion);
            if (!ok) {
                logger.warn("Could not activate key version v{}; current is v{}",
                        restoreVersion, keyService.getCurrentVersion().getVersion());
            }
        }
        logger.info("Key current version at startup: v{}", keyService.getCurrentVersion().getVersion());

        try {
            cryptoService.getClass()
                    .getMethod("setKeyService", KeyLifeCycleService.class)
                    .invoke(cryptoService, keyService);
        } catch (Throwable t) {
            logger.warn("Could not setKeyService on cryptoService; using fallback wiring", t);
        }

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile, dataPath, keysFile, dimensions, metadataPath, false,
                metadataManager, cryptoService, batchSize
        );

        if (queryOnly) {
            sys.setQueryOnlyMode(true);
            if (restoreVersion > 0) {
                int restored = sys.restoreIndexFromDisk(restoreVersion);
                logger.info("Ready to query: restored {} points for dim={} (requested)", restored, dimensions.get(0));
            } else {
                logger.warn("No on-disk version found to restore; continuing with empty in-memory index");
            }
            sys.finalizeForSearch();
            sys.runQueries(queryPath, dimensions.get(0), groundtruthPath);
            sys.shutdown();
            if (cfg.getOutput() != null && cfg.getOutput().exportArtifacts) {
                sys.exportArtifacts(sys.resultsDir);
            }
            return;
        }

        if ("true".equals(System.getProperty("disable.exit"))) {
            sys.setExitOnShutdown(false);
        }

        int dim = dimensions.get(0);
        sys.runEndToEnd(dataPath, queryPath, dim, groundtruthPath);

        if (sys.getProfiler() != null) {
            var client = sys.getProfiler().getAllClientQueryTimes();
            var server = sys.getProfiler().getAllServerQueryTimes();
            var ratios = sys.getProfiler().getAllQueryRatios();

            double avgClient = client.stream().mapToDouble(d -> d).average().orElse(0.0);
            double avgServer = server.stream().mapToDouble(d -> d).average().orElse(0.0);
            double artMs = java.util.stream.IntStream.range(0, Math.min(client.size(), server.size()))
                    .mapToDouble(i -> client.get(i) + server.get(i))
                    .average().orElse(0.0);
            double avgRatio = ratios.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

            System.out.printf("Average Client Query Time: %.2f ms%n", avgClient);
            System.out.printf("Average Server Query Time: %.2f ms%n", avgServer);
            System.out.printf("Average Total Query Time (ART): %.2f ms%n", artMs);
            System.out.printf("Average Ratio: %.4f%n", avgRatio);
        }

        long start = System.currentTimeMillis();
        sys.shutdown();
        if (cfg.getOutput() != null && cfg.getOutput().exportArtifacts) {
            sys.exportArtifacts(sys.resultsDir);
        }
        logger.info("Shutdown completed in {} ms", System.currentTimeMillis() - start);
    }
}
