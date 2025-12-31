package com.fspann.api;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.crypto.SelectiveReencCoordinator;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.key.BackgroundReencryptionScheduler;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.loader.DefaultDataLoader;
import com.fspann.loader.FormatLoader;
import com.fspann.loader.GroundtruthManager;
import com.fspann.loader.StreamingBatchLoader;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.query.core.*;
import com.fspann.common.QueryToken;
import com.fspann.common.QueryResult;
import com.fspann.query.service.QueryService;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

    private final PartitionedIndexService indexService;
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
    private volatile boolean queryOnlyMode = false;
    private BaseVectorReader baseReader = null;
    private final ReencryptionTracker reencTracker;
    private final SelectiveReencCoordinator reencCoordinator;
    private final MicrometerProfiler microProfiler;

    // Config-driven toggles
    private final boolean computePrecision;
    private final boolean writeGlobalPrecisionCsv;
    private final int[] K_VARIANTS;
    private final Path resultsDir;
    private final double configuredNoiseScale;
    private Path reencCsv;
    private final SystemConfig.KAdaptiveConfig kAdaptive;
    // key = dim|topK|vectorHash  (simple but enough for tests)
    private final Map<String, List<QueryResult>> queryCache = new ConcurrentHashMap<>();
    enum RatioSource {AUTO, GT, BASE}
    private final BackgroundReencryptionScheduler backgroundReencryptor;
    private final SystemConfig.PaperConfig paper;
    private boolean forcedRotationDone = false;

    // Optional: use sharded metadata for large-scale deployments
    private final boolean useShardedMetadata = Boolean.parseBoolean(
            System.getProperty("metadata.sharded", "false")
    );

    private final ShardedMetadataManager shardedMetadata;
    private final DecoyQueryGenerator decoyGenerator;
    private final boolean decoyEnabled;
    private final RatioSource ratioSource;
    private final int BATCH_SIZE;
    private long totalIndexingTimeNs = 0L;
    private long totalQueryTimeNs = 0L;
    private int totalInserted = 0;
    private final java.util.concurrent.atomic.AtomicInteger indexedCount = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicLong fileOrdinal = new java.util.concurrent.atomic.AtomicLong(0);

    // artifact/export helpers (aligned with FsPaths)
    private final Path metaDBPath;
    private final Path pointsPath;
    private final Path keyStorePath;
    private final ExecutorService executor;
    private boolean exitOnShutdown = false;
    private final boolean reencEnabled;
    private static final double RATIO_EPS = 1e-24;

    // remember previous FsPaths props to restore on shutdown
    private final String prevBaseProp;
    private final String prevMetaProp;
    private final String prevPointsProp;
    private final String prevKeyStoreProp;

    // Stabilization diagnostics
    private volatile int lastStabilizedRaw = 0;
    private volatile int lastStabilizedFinal = 0;

    /**
     * Global "touched" accumulator of vector IDs encountered during the run.
     */
    private static final int FANOUT_WARN = Integer.getInteger("guard.fanout.warn", 2000);

    /**
     * Re-encryption mode:
     *   - "immediate": (legacy) perform re-encryption checks per query
     *   - "end": (default) touched IDs and re-encrypt once at the end of the run
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

        // ---- basic invariants ----
        Objects.requireNonNull(configPath, "Config path cannot be null");
        Objects.requireNonNull(keysFilePath, "Keys file path cannot be null");
        Objects.requireNonNull(dimensions, "Dimensions cannot be null");
        Objects.requireNonNull(metadataPath, "Metadata path cannot be null");
        if (dimensions.isEmpty()) throw new IllegalArgumentException("Dimensions list cannot be empty");
        if (batchSize <= 0) throw new IllegalArgumentException("Batch size must be positive");

        this.verbose = verbose;
        this.BATCH_SIZE = batchSize;
        int cores = Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(Math.max(2, cores - 2));
        this.configPath = normalizePath(configPath);

        // ---- load config ----
        SystemConfig cfg;
        cfg = new com.fspann.api.ApiSystemConfig(this.configPath).getConfig();
        this.config = cfg;

        this.paper = cfg.getPaper();

        logger.info(
                "FINAL PAPER CONFIG: m={} lambda={} divisions={} tables={} seed={}",
                paper.getM(), paper.getLambda(), paper.getDivisions(), paper.getTables(), paper.getSeed()
        );

        // ==== feature configuration ====

        // K-adaptive (Option-C friendly)
        this.kAdaptive = (config.getKAdaptive() != null)
                ? config.getKAdaptive()
                : new SystemConfig.KAdaptiveConfig();

        // Decoy queries (optional)
        this.decoyEnabled = Boolean.parseBoolean(System.getProperty("decoy.enabled", "false"));
        if (decoyEnabled) {
            double ratio = Double.parseDouble(System.getProperty("decoy.ratio", "0.2"));
            String distStr = System.getProperty("decoy.distribution", "GAUSSIAN");
            DecoyQueryGenerator.DecoyDistribution dist =
                    DecoyQueryGenerator.DecoyDistribution.valueOf(distStr.toUpperCase());

            this.decoyGenerator = new DecoyQueryGenerator(dimensions.get(0), ratio, dist);
            logger.info("Decoy queries enabled: ratio={}, distribution={}", ratio, dist);
        } else {
            this.decoyGenerator = null;
        }

        // Precision controls
        boolean defaultPrecision = (config.getEval() != null) && config.getEval().computePrecision;
        this.computePrecision = propOr(defaultPrecision, "eval.computePrecision", "computePrecision");

        this.writeGlobalPrecisionCsv = propOr(
                config.getEval().writeGlobalPrecisionCsv,
                "eval.writeGlobalPrecisionCsv", "eval.writeGlobalPrecision", "writeGlobalPrecisionCsv"
        );

        this.K_VARIANTS = (config.getEval().kVariants != null && config.getEval().kVariants.length > 0)
                ? config.getEval().kVariants.clone()
                : new int[]{1, 20, 40, 60, 80, 100};

        // selective re-encryption global toggle
        this.reencEnabled = propOr(
                config.isReencryptionGloballyEnabled(),
                "reenc.enabled", "reencryption.enabled", "reencrypt.enabled"
        );

        // ==== output directory + profilers ====

        String outDir = (config.getOutput() != null && config.getOutput().resultsDir != null
                && !config.getOutput().resultsDir.isBlank())
                ? config.getOutput().resultsDir
                : System.getProperty("results.dir", "results");

        this.resultsDir = Paths.get(outDir);
        try {
            Files.createDirectories(resultsDir);
        } catch (IOException ioe) {
            logger.warn("Could not create resultsDir {}; falling back to CWD", resultsDir, ioe);
        }

        this.configuredNoiseScale = (config.getCloak() != null)
                ? Math.max(0.0, config.getCloak().noise)
                : 0.0;

        // per-run re-encryption CSV
        this.reencCsv = resultsDir.resolve("reencrypt_metrics.csv");
        initReencCsvIfNeeded(this.reencCsv);

        // ==== ratio computation mode ====

        String rs = (config.getRatio() != null && config.getRatio().source != null)
                ? config.getRatio().source.toLowerCase(Locale.ROOT)
                : "auto";

        this.ratioSource = switch (rs) {
            case "gt" -> RatioSource.GT;
            case "base" -> RatioSource.BASE;
            default -> RatioSource.AUTO;
        };

        // ==== FsPaths binding ====

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

        // ==== metadata manager + optional sharding ====

        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");

        if (useShardedMetadata) {
            int shardCount = Integer.getInteger("metadata.shards", 16);
            this.shardedMetadata = new ShardedMetadataManager(
                    metadataPath.resolve("sharded_metadata").toString(),
                    shardCount,
                    pointsPath.toString()
            );
            logger.info("Using sharded metadata: {} shards", shardCount);
        } else {
            this.shardedMetadata = null;
        }

        // ==== Crypto + key lifecycle ====

        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService cannot be null");

        Path derivedKeyRoot = resolveKeyStorePath(keysFilePath, metadataPath);

        this.prevKeyStoreProp = System.getProperty(FsPaths.KEYSTORE_PROP);
        System.setProperty(FsPaths.KEYSTORE_PROP, derivedKeyRoot.toString());

        MeterRegistry meterRegistry;
        try {
            meterRegistry = new SimpleMeterRegistry();
        } catch (Throwable ignore) {
            meterRegistry = null;
        }

        KeyLifeCycleService ks = cryptoService.getKeyService();
        if (ks == null) {
            Files.createDirectories(derivedKeyRoot.getParent());
            KeyManager keyManager = new KeyManager(derivedKeyRoot.toString());
            int opsCap = (int) Math.min(Integer.MAX_VALUE, config.getOpsThreshold());
            KeyRotationPolicy policy = new KeyRotationPolicy(opsCap, config.getAgeThresholdMs());

            ks = new KeyRotationServiceImpl(keyManager, policy, metaDBPath.toString(), metadataManager, cryptoService);

            try {
                cryptoService.getClass().getMethod("setKeyService", KeyLifeCycleService.class)
                        .invoke(cryptoService, ks);
            } catch (Exception ignore) {}
        }
        this.keyService = ks;

        // resolve actual keystore path
        Path resolvedKeyStore = derivedKeyRoot;
        if (keyService instanceof KeyRotationServiceImpl kr) {
            try {
                Object km = kr.getClass().getMethod("getKeyManager").invoke(kr);
                String p = (String) km.getClass().getMethod("getStorePath").invoke(km);
                if (p != null && !p.isBlank()) resolvedKeyStore = Paths.get(p);
            } catch (Throwable ignore) {}
            kr.initializeUsageTracking();
        }
        this.keyStorePath = resolvedKeyStore;

        // ==== index service ====

        this.indexService =
                new PartitionedIndexService(
                        metadataManager,
                        config,
                        (KeyRotationServiceImpl) keyService,
                        (AesGcmCryptoService) cryptoService
                );


        // optional background re-encryption
        if (Boolean.parseBoolean(System.getProperty("reenc.background.enabled", "false"))) {
            int intervalMin = Integer.getInteger("reenc.background.intervalMin", 60);

            this.backgroundReencryptor = new BackgroundReencryptionScheduler(
                    (KeyRotationServiceImpl) keyService, cryptoService, indexService, metadataManager
            );
            backgroundReencryptor.start(intervalMin);

            logger.info("Background re-encryption enabled: interval={} min", intervalMin);
        } else {
            this.backgroundReencryptor = null;
        }

        // ==== caches + profiler ====

        this.cache = new StringKeyedCache(config.getNumShards() * 1000);

        Profiler baseProfiler = new Profiler();
        MicrometerProfiler micro = null;
        if (config.isProfilerEnabled()) {
            micro = new MicrometerProfiler(meterRegistry, baseProfiler);
        }
        this.profiler = baseProfiler;
        this.microProfiler = micro;

        // ==== QueryTokenFactories (disables LSH entirely) ====

        for (int dim : dimensions) {

            QueryTokenFactory factory =
                    new QueryTokenFactory(
                            cryptoService,
                            keyService,
                            config
                    );

            tokenFactories.put(dim, factory);

            logger.info(
                    "TokenFactory created: dim={} m={} lambda={} divisions={} tables={}",
                    dim, paper.getM(), paper.getLambda(), paper.getDivisions(), paper.getTables()
            );
        }

        int indexTables = paper.getTables();
        int tokenTables = indexService.numTables();

        if (indexTables != tokenTables) {
            throw new IllegalStateException(
                    "TABLE MISMATCH: index tables=" + indexTables +
                            ", token tables=" + tokenTables
            );
        }

        int primaryDim = dimensions.get(0);
        QueryTokenFactory qtf = tokenFactories.get(primaryDim);

        if (tokenFactories.size() != 1) {
            throw new IllegalStateException(
                    "Multiple dimensions configured but QueryService bound to single TokenFactory. " +
                            "This will corrupt precision."
            );
        }

        // ==== QueryService ====
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService, qtf, cfg);
        // ==== touch accounting / selective re-encryption ====
        this.reencTracker = new ReencryptionTracker();
        if (this.queryService instanceof QueryServiceImpl qs) {
            qs.setReencryptionTracker(reencTracker);
            qs.setStabilizationCallback(this::setStabilizationStats);
        }

        this.reencCoordinator = new SelectiveReencCoordinator(
                indexService,
                cryptoService,
                ks,
                reencTracker,
                meterRegistry,
                resultsDir,
                () -> dirSize(pointsPath)
        );

        // ==== load base vectors (optional distance-ratio) ====

        String baseProp = System.getProperty("base.path", "").trim();
        if (!baseProp.isEmpty()) {
            Path basePath = Paths.get(baseProp);
            boolean isBvecs = basePath.toString().toLowerCase(Locale.ROOT).endsWith(".bvecs");
            int dimExpected = dimensions.get(0);

            try {
                this.baseReader = BaseVectorReader.open(basePath, dimExpected, isBvecs);
                logger.info("BaseVectorReader mapped: (dim={}, type={})",
                        dimExpected, isBvecs ? "bvecs" : "fvecs");
            } catch (IOException ioe) {
                logger.warn("Failed to map base.path={}, ratio metrics disabled", basePath, ioe);
            }
        } else {
            logger.info("No -Dbase.path provided; distance-ratio disabled.");
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

        logger.info(
                "Indexing completed: indexedInThisRun={}, totalIndexed={}",
                totalInserted,
                getIndexedVectorCount()
        );
    }

    /**
     *
     * Uses cryptoService.encryptToPoint() directly instead of duplicating encryption logic.
     * Removes encryptVector() and generateIV() helper methods.
     * Keeps facade clean - no low-level crypto details in main system class.
     */
    public void batchInsert(List<double[]> vectors, int dim) {
        if (!indexService.isFrozen() && indexedCount.get() != fileOrdinal.get()) {
            logger.debug("Index warmup: indexed={} ord={}",
                    indexedCount.get(), fileOrdinal.get());
        }

        Objects.requireNonNull(vectors, "Vectors cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vectors.isEmpty()) return;

        long startNs = System.nanoTime();
        if (profiler != null) profiler.start("batchInsert");

        for (int offset = 0; offset < vectors.size(); offset += BATCH_SIZE) {
            List<double[]> slice =
                    vectors.subList(offset, Math.min(offset + BATCH_SIZE, vectors.size()));

            List<double[]> valid = new ArrayList<>(slice.size());
            List<String> ids = new ArrayList<>(slice.size());

            for (int j = 0; j < slice.size(); j++) {
                double[] v = slice.get(j);
                long ord = fileOrdinal.getAndIncrement();

                if (v == null) {
                    logger.warn("Skipping null vector at ordinal={}", ord);
                    continue;
                }
                if (v.length != dim) {
                    logger.warn(
                            "Skipping vector at ordinal={} with dim={} (expected {})",
                            ord, v.length, dim
                    );
                    continue;
                }

                String pointId = Long.toString(ord);
                if (Long.parseLong(pointId) != ord) {
                    throw new IllegalStateException(
                            "ID drift detected: id=" + pointId + ", ordinal=" + ord
                    );
                }
                ids.add(pointId);
                valid.add(v);
            }

            if (valid.isEmpty()) {
                logger.warn("No valid vectors in batch starting at offset {}", offset);
                continue;
            }

            // ===== Rotate keys ONCE per batch =====
            keyService.rotateIfNeeded();

            // ===== Index plaintext vectors ONLY =====
            try {
                for (int j = 0; j < valid.size(); j++) {
                    indexService.insert(ids.get(j), valid.get(j));
                }

                indexedCount.addAndGet(valid.size());
                totalInserted += valid.size();

                if (verbose) {
                    logger.debug(
                            "Indexed {} vectors from batch at offset {}",
                            valid.size(), offset
                    );
                }
            } catch (Exception e) {
                logger.error("Failed to index batch starting at offset {}", offset, e);
                throw new RuntimeException(
                        "Batch indexing failed at offset " + offset, e
                );
            }
        }

        if (profiler != null) {
            profiler.stop("batchInsert");
            List<Long> timings = profiler.getTimings("batchInsert");
            long durationNs =
                    (!timings.isEmpty())
                            ? timings.get(timings.size() - 1)
                            : (System.nanoTime() - startNs);
            totalIndexingTimeNs += durationNs;
        }

        logger.info(
                "batchInsert complete: {} vectors inserted this call",
                totalInserted
        );
    }

    public int getIndexedVectorCount() {
        return indexedCount.get();
    }

    public void insert(String id, double[] vector, int dim) {
        Objects.requireNonNull(id, "ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vector.length != dim) {
            throw new IllegalArgumentException("Vector length must match dimension");
        }

        if (profiler != null) profiler.start("insert");
        try {
            // Rotate keys if required (actual encryption happens inside index)
            keyService.rotateIfNeeded();

            // ===== Index plaintext vector ONLY =====
            indexService.insert(id, vector);
            indexedCount.incrementAndGet();

        } finally {
            if (profiler != null) {
                profiler.stop("insert");
                List<Long> timings = profiler.getTimings("insert");
                if (!timings.isEmpty()) {
                    long durationNs = timings.get(timings.size() - 1);
                    totalIndexingTimeNs += durationNs;

                    if (verbose) {
                        logger.debug(
                                "Insert complete for id={} in {} ms",
                                id, durationNs / 1_000_000.0
                        );
                    }
                }
            }
        }
    }

    /* ---------------------- Query API ---------------------- */

    QueryTokenFactory factoryForDim(int dim) {
        QueryTokenFactory qtf = tokenFactories.get(dim);
        if (qtf == null)
            throw new IllegalStateException("No QueryTokenFactory for dim=" + dim);
        return qtf;
    }

    public void runQueries(
            List<double[]> queries,
            int dim,
            GroundtruthManager gt,
            boolean trustedGT
    ) {
        Objects.requireNonNull(queries, "queries");
        Objects.requireNonNull(gt, "groundtruth");

        QueryServiceImpl qs = getQueryServiceImpl();

        logger.info("Running explicit query loop: queries={}, dim={}", queries.size(), dim);

        final int MAX_K = Arrays.stream(K_VARIANTS).max().orElseThrow();

        for (int qi = 0; qi < queries.size(); qi++) {
            double[] q = queries.get(qi);

            // ------------------ Groundtruth (diagnostic only) ------------------
            String trueNNId = null;
            if (trustedGT) {
                int[] gtIds = gt.getGroundtruthIds(qi, 1);
                if (gtIds != null && gtIds.length > 0) {
                    trueNNId = String.valueOf(gtIds[0]);
                }
            }
            qs.setTrueNearestId(trueNNId);

            // ------------------ Token (ONCE, MAX_K) ------------------
            QueryToken baseToken = createToken(q, MAX_K, dim);

            long t0 = System.nanoTime();
            List<QueryResult> fullResults = qs.search(baseToken);

            if (fullResults.isEmpty()) {
                int baseProbes = config.getRuntime().getMaxCandidateFactor();
                int fallbackProbes = Math.max(baseProbes * 2, 4);

                indexService.setProbeOverride(fallbackProbes);
                fullResults = qs.search(baseToken);
                indexService.clearProbeOverride();
            }

            long t1 = System.nanoTime();
            addQueryTime(t1 - t0);

            // ------------------ Metrics (truncate only) ------------------
            for (int k : K_VARIANTS) {

                List<QueryResult> atK =
                        fullResults.isEmpty()
                                ? Collections.emptyList()
                                : fullResults.subList(0, Math.min(k, fullResults.size()));

                QueryMetrics m = computeMetricsAtK(
                        k,
                        MAX_K,
                        atK,
                        qi,
                        q,
                        qs,
                        gt
                );

                long serverMs  = (long) boundedServerMs(qs, t0, t1);
                long clientMs  = (t1 - t0) / 1_000_000L;
                long decryptMs = qs.getLastDecryptNs() / 1_000_000L;

                int tokenBytes =
                        baseToken.getEncryptedQuery().length +
                                baseToken.getIv().length;

                profiler.recordQueryRow(
                        "Q" + qi + "_K" + k,
                        serverMs,
                        clientMs,
                        clientMs,
                        decryptMs,
                        lastInsertMs(),

                        m.distanceRatioAtK(),      // ← PAPER RATIO (quality)
                        m.candidateRatioAtK(),     // ← SEARCH EFFICIENCY (cost)
                        m.recallAtK(),

                        qs.getLastCandTotal(),
                        qs.getLastCandKept(),
                        qs.getLastCandDecrypted(),
                        atK.size(),

                        tokenBytes,
                        dim,
                        k,
                        MAX_K,
                        qi,

                        totalFlushed(),
                        flushThreshold(),

                        indexService.getLastTouchedCount(),
                        reencTracker.uniqueCount(),

                        0L,
                        0L,
                        0L,

                        ratioDenomLabelPublic(trustedGT),
                        "partitioned",

                        getLastStabilizedRaw(),
                        getLastStabilizedFinal(),

                        qs.getLastTrueNNRank(),
                        qs.wasLastTrueNNSeen()
                );
          }

        }
    }


    /**
     * Compute evaluation metrics for a single query at a given K.
     *
     * Metrics computed:
     *   1. Distance Ratio: avg(dist_returned_j / dist_groundtruth_j) for j=1..K
     *      - Measures result QUALITY
     *      - Perfect = 1.0 (returned exactly the true k-NNs)
     *      - Higher values indicate returned results are farther than true NNs
     *
     *   2. Precision@K: |true_KNN ∩ returned| / K
     *      - Measures RECALL
     *      - Perfect = 1.0
     *
     *   3. Candidate Ratio: candidates_examined / K
     *      - Measures search EFFICIENCY
     *      - Lower is more efficient (minimum = 1.0)
     *
     **/
    QueryMetrics computeMetricsAtK(
            int k,
            int maxK,
            List<QueryResult> annResults,
            int queryIndex,
            double[] queryVector,
            QueryServiceImpl qs,
            GroundtruthManager gtMgr
    ) {

        // =========================================================
        // Ground Truth
        // =========================================================
        int[] gt = gtMgr.getGroundtruth(queryIndex, k);
        if (gt == null || gt.length < k) {
            return new QueryMetrics(
                    Double.NaN,                 // candidateRatioAtK
                    Double.NaN,                 // distanceRatioAtK
                    Double.NaN                  // recallAtK
            );
        }

        // =========================================================
        // Resolve ANN indices
        // =========================================================
        int upto = Math.min(k, annResults.size());
        int[] annIdx = new int[upto];

        for (int i = 0; i < upto; i++) {
            annIdx[i] = resolveBaseIndex(annResults.get(i));
        }

        // =========================================================
        // Recall@K  (QUALITY)
        // =========================================================
        Set<Integer> gtSet = new HashSet<>(k);
        for (int i = 0; i < k; i++) {
            gtSet.add(gt[i]);
        }

        int hits = 0;
        for (int i = 0; i < upto; i++) {
            if (gtSet.contains(annIdx[i])) hits++;
        }

        double recallAtK = hits / (double) k;

        // =========================================================
        // Distance Ratio@K (DIAGNOSTIC ONLY)
        // =========================================================
        double distanceRatioAtK = Double.NaN;

        if (baseReader != null && k >= 20) {
            double ratioSum = 0.0;
            int used = 0;

            for (int i = 0; i < upto; i++) {
                int annId = annIdx[i];
                int gtId  = gt[i];

                if (annId < 0 || annId >= baseReader.count) continue;
                if (gtId  < 0 || gtId  >= baseReader.count) continue;

                double dGt = baseReader.l2(queryVector, gtId);
                if (dGt <= 0) continue;

                double dAnn = baseReader.l2(queryVector, annId);
                ratioSum += (dAnn / dGt);
                used++;
            }

            if (used > 0) {
                distanceRatioAtK = ratioSum / used;
            }
        }

        // =========================================================
        // Candidate Ratio@K  (PRIMARY — Peng et al.)
        // =========================================================
        int refinedCandidates = qs.getLastCandDecrypted(); // NOT total raw
        double candidateRatioAtK = refinedCandidates / (double) k;

        return new QueryMetrics(
                refinedCandidates / (double) k,  // ← searchEfficiencyRatio (NOT paper ratio)
                distanceRatioAtK,                 // ← THIS IS the paper ratio!
                recallAtK
        );
    }

    private int resolveBaseIndex(QueryResult r) {
        try {
            int idx = Integer.parseInt(r.getId());
            if (idx < 0) {
                throw new IllegalStateException("Negative base index: " + r.getId());
            }
            return idx;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Unresolvable ANN id: " + r.getId(), e
            );
        }
    }

    // --- FOR INTEGRATION TESTS
    private QueryFacade queryFacade;

    public final class QueryFacade {

        private final ForwardSecureANNSystem sys;

        QueryFacade(ForwardSecureANNSystem sys) {
            this.sys = sys;
        }

        /** Simple query path used by ITs */
        public List<QueryResult> evalSimple(
                double[] q,
                int topK,
                int dim,
                boolean cloak
        ) {
            QueryToken tok = cloak
                    ? sys.cloakQuery(q, dim, topK)
                    : sys.createToken(q, topK, dim);

            QueryServiceImpl qs = sys.getQueryServiceImpl();
            return qs.search(tok);
        }

        /** Batch path used by RatioPipelineIT */
        public void evalBatch(
                List<double[]> queries,
                int dim,
                GroundtruthManager gt,
                Path outDir,
                boolean trustedGT
        ) {
            sys.runQueries(queries, dim, gt, trustedGT);
        }
    }

    public QueryFacade getEngine() {
        if (queryFacade == null) {
            queryFacade = new QueryFacade(this);
        }
        return queryFacade;
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

            for (int j = 0; j < batch.size(); j++) {
                indexService.insert(ids.get(j), batch.get(j));
            }
            indexedCount.addAndGet(size);
            remaining -= size;
        }
    }

    public int restoreIndexFromDisk(int version) throws IOException {
        logger.info("Restoring index from metadata for v{} ...", version);

        int restored = 0;
        for (EncryptedPoint ep : metadataManager.getAllEncryptedPoints()) {
            if (ep == null || ep.getVersion() != version) continue;

            double[] vec =
                    cryptoService.decryptFromPoint(
                            ep,
                            keyService.getCurrentVersion().getKey()
                    );

            if (vec != null) {
                indexService.insert(ep.getId(), vec);
                restored++;
            }
        }

        indexService.finalizeForSearch();
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

    public void setQueryOnlyMode(boolean b) {
        this.queryOnlyMode = b;
    }

    public void finalizeForSearch() {
        indexService.finalizeForSearch();
        logger.info("Partitioned index finalized for search");
    }

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

    /** Export profiler CSVs, queries, and storage summary to outDir. */
    public void exportArtifacts(Path outDir) throws IOException {
        Files.createDirectories(outDir);

        /* ====================================================================== */
        /* 1. RAW PROFILER CSV (single source of truth for queries)                */
        /* ====================================================================== */

        Path profilerCsv = outDir.resolve("profiler_metrics.csv");
        if (profiler != null) {
            profiler.exportToCSV(profilerCsv.toString());
        }

        /* ====================================================================== */
        /* 2. AGGREGATES (ART, AvgRatio, counts) — IN MEMORY                       */
        /* ====================================================================== */

        Aggregates agg = Aggregates.fromProfiler(this.profiler);

        double avgArtMs   = agg.avgRunMs;        // canonical ART
        double avgRatio   = agg.avgCandidateRatio;     // canonical ratio

        /* ====================================================================== */
        /* 3. REPRODUCIBLE METRICS SUMMARY                                        */
        /* ====================================================================== */

        String cfgHash = "NA";
        try {
            Path cfgPath = Paths.get(this.configPath);
            if (Files.exists(cfgPath)) {
                cfgHash = toHex(
                        MessageDigest.getInstance("SHA-256")
                                .digest(Files.readAllBytes(cfgPath))
                );
            }
        } catch (Exception ignore) {}

        int keyVer = -1;
        try {
            keyVer = keyService.getCurrentVersion().getVersion();
        } catch (Exception ignore) {}

        Files.writeString(
                outDir.resolve("metrics_summary.txt"),
                String.format(
                        Locale.ROOT,
                        "mode=partitioned%nconfig_sha256=%s%nkey_version=v%d%nART(ms)=%.3f%nAvgRatio=%.6f%n",
                        cfgHash,
                        keyVer,
                        avgArtMs,
                        avgRatio
                ),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        /* ====================================================================== */
        /* 4. PAPER-READY SUMMARY CSV                                             */
        /* ====================================================================== */

        try {
            String dataset;
            String baseProp = System.getProperty("base.path", "");
            if (!baseProp.isBlank()) {
                dataset = Paths.get(baseProp).getFileName().toString();
            } else {
                dataset = this.resultsDir.getFileName().toString();
            }

            String profile =
                    System.getProperty("cli.profile", "ideal-system");

            long totalIndexMs =
                    Math.round(totalIndexingTimeNs / 1_000_000.0);

            agg.spaceMetaBytes   = Math.max(0, dirSize(FsPaths.metadataDb()));
            agg.spacePointsBytes = Math.max(0, dirSize(FsPaths.pointsDir()));

            EvaluationSummaryPrinter.printAndWriteCsv(
                    dataset,
                    profile,
                    paper.getM(),
                    paper.getLambda(),
                    paper.getDivisions(),
                    totalIndexMs,
                    agg,
                    outDir.resolve("summary.csv")
            );
        } catch (Exception e) {
            logger.warn("Failed to write summary.csv", e);
        }
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

        StringBuilder sb = new StringBuilder(256);

        sb.append("v").append(token.getVersion())
                .append(":d").append(token.getDimension())
                .append(":k").append(token.getTopK());

        BitSet[][] codes = token.getBitCodes();
        sb.append(":c");

        for (int t = 0; t < codes.length; t++) {
            sb.append("|t").append(t);
            for (int d = 0; d < codes[t].length; d++) {
                BitSet bs = codes[t][d];
                sb.append(':').append(bs != null ? bs.hashCode() : 0);
            }
        }

        return sb.toString();
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

    private ReencOutcome maybeReencryptTouched(String label, QueryServiceImpl qs) {
        boolean immediate = "immediate".equalsIgnoreCase(System.getProperty("reenc.mode", "end"));

        int touchedUnique = reencTracker.uniqueCount();
        ReencReport rep = ReencReport.empty();

        // Immediate mode = do not re-encrypt here
        if (immediate && !reencRan.get()
                && touchedUnique >= Integer.getInteger("reenc.minTouched", 10_000)) {

            logger.info("[{}] Live-mode selective re-encryption threshold reached ({} touched).",
                    label, touchedUnique);

            // Mark so we don’t trigger again
            reencRan.set(true);
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

    public PartitionedIndexService getIndexService() {
        return this.indexService;
    }

    private static String toHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a) sb.append(String.format("%02x", b));
        return sb.toString();
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

    private String ratioDenomLabel(boolean gtTrusted) {
        return switch (this.ratioSource) {
            case GT -> "gt";
            case BASE -> "base";
            case AUTO -> (gtTrusted ? "gt(auto)" : "base(auto)");
        };
    }

    public static final class ReencReport {
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

        // MUST be public
        public int getReencryptedCount() { return reencryptedCount; }
        public long getTimeMs() { return timeMs; }
        public long getBytesDelta() { return bytesDelta; }
        public long getBytesAfter() { return bytesAfter; }
    }

    static final class ReencOutcome {
        final int cumulativeUnique;
        final ReencReport rep;

        ReencOutcome(int cumulativeUnique, ReencReport rep) {
            this.cumulativeUnique = cumulativeUnique;
            this.rep = rep;
        }
    }

    private void forceRotateOnceAfterQueries(String reason) {
        if (forcedRotationDone) return;

        int before = keyService.getCurrentVersion().getVersion();

        // HARD rotation, independent of policy
        keyService.forceRotateNow();

        int after = keyService.getCurrentVersion().getVersion();

        logger.info(
                "FORCED KEY ROTATION: v{} → v{} ({})",
                before, after, reason
        );

        forcedRotationDone = true;
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

    /**
     * Load queries with validation
     */
    private static List<double[]> loadQueriesWithValidation(Path queryPath, int dimension) throws Exception {
        logger.info("Loading queries from: {}", queryPath);

        if (!Files.exists(queryPath)) {
            throw new IllegalArgumentException("Query file not found: " + queryPath);
        }

        List<double[]> queries = new ArrayList<>();

        try {
            DefaultDataLoader loader = new DefaultDataLoader();
            int batchCount = 0;

            while (true) {
                List<double[]> batch = loader.loadData(queryPath.toString(), dimension);

                if (batch == null || batch.isEmpty()) {
                    break;
                }

                queries.addAll(batch);
                batchCount++;
            }
        } catch (Exception e) {
            logger.warn("DefaultDataLoader failed, trying fallback");
            queries = loadQueriesFallback(queryPath, dimension);
        }

        if (queries.isEmpty()) {
            throw new IllegalStateException("Query loading resulted in empty list");
        }

        logger.info("Loaded {} queries total", queries.size());
        return queries;
    }

    /**
     * Fallback query loading
     */
    private static List<double[]> loadQueriesFallback(Path queryPath, int dimension) throws Exception {
        logger.info("Using fallback FVECS query loading");

        List<double[]> queries = new ArrayList<>();

        if (queryPath.toString().endsWith(".fvecs")) {
            byte[] data = Files.readAllBytes(queryPath);
            int bytesPerVector = 4 + (dimension * 4);
            int numVectors = data.length / bytesPerVector;

            logger.info("FVECS: {} vectors × {} dims", numVectors, dimension);

            for (int i = 0; i < numVectors; i++) {
                double[] vector = new double[dimension];
                int offset = i * bytesPerVector + 4;

                for (int j = 0; j < dimension; j++) {
                    int byteOffset = offset + (j * 4);
                    int bits = (data[byteOffset] & 0xFF) |
                            ((data[byteOffset + 1] & 0xFF) << 8) |
                            ((data[byteOffset + 2] & 0xFF) << 16) |
                            ((data[byteOffset + 3] & 0xFF) << 24);
                    vector[j] = Float.intBitsToFloat(bits);
                }

                queries.add(vector);
            }

            logger.info("✓ Loaded {} vectors via FVECS", queries.size());
        }

        return queries;
    }

    // ==================== K-ADAPTIVE PROBE-ONLY (ABLATION) ====================

    /**
     * Run K-adaptive probe-only widening for a query (ablation study).
     * Increments probe shards WITHOUT actually executing search.
     */
    public void runKAdaptiveProbeOnly(int queryIndex, double[] q, int dim, QueryServiceImpl qs) {
        if (!kAdaptive.enabled || q == null) {
            return;
        }

        try {
            // Adaptive widening: increase probe shards per round
            int currentProbes = Integer.getInteger("probe.shards", 1);
            int newProbes = Math.min(
                    (int) (currentProbes * kAdaptive.probeFactor),
                    (int) kAdaptive.maxFanout
            );
            System.setProperty("probe.shards", String.valueOf(newProbes));

            logger.debug("K-adaptive probe-only: query {} increased probes {} → {}",
                    queryIndex, currentProbes, newProbes);
        } catch (Exception e) {
            logger.warn("K-adaptive probe-only failed for query {}", queryIndex, e);
        }
    }

    // ==================== INSERTION & FLUSHING METRICS ====================

    /**
     * Get last insertion time in milliseconds.
     * Returns the most recent insert operation duration.
     */
    public long lastInsertMs() {
        // Fallback from profiler if available
        if (profiler != null) {
            List<Long> timings = profiler.getTimings("insert");
            if (!timings.isEmpty()) {
                return timings.get(timings.size() - 1) / 1_000_000L;  // convert ns to ms
            }
        }
        return 0L;
    }

    /**
     * Get total number of vectors flushed to disk.
     * This is a facade over the indexService buffer state.
     */
    public int totalFlushed() {
        try {
            com.fspann.common.EncryptedPointBuffer buf = indexService.getPointBuffer();
            if (buf != null) {
                // Get flushed count from buffer (if available)
                try {
                    java.lang.reflect.Method getFlushed =
                            buf.getClass().getMethod("getFlushedCount");
                    Object result = getFlushed.invoke(buf);
                    if (result instanceof Number n) {
                        return n.intValue();
                    }
                } catch (Exception ignore) {}
            }
        } catch (Exception ignore) {}

        // Fallback: return indexed count (conservative estimate)
        return indexedCount.get();
    }

    /**
     * Get the buffer flush threshold (batch size before flushing).
     * This is typically BATCH_SIZE or a configured value.
     */
    public int flushThreshold() {
        return BATCH_SIZE;
    }

    /* ====================================================================== */
    /*                     FORWARD SECURE ANN — PUBLIC FAÇADE                */
    /* ====================================================================== */

    /** Façade: produce a token for (q, k, dim) */
    public QueryToken createToken(double[] q, int k, int dim) {

        if (!indexService.isFrozen()) {
            throw new IllegalStateException(
                    "Query attempted before indexService.finalizeForSearch()");
        }


        Objects.requireNonNull(q, "Query vector cannot be null");

        if (q.length != dim) {
            throw new IllegalArgumentException(
                    "Query dimension mismatch: expected=" + dim +
                            " actual=" + q.length
            );
        }

        QueryTokenFactory factory = tokenFactories.get(dim);
        if (factory == null) {
            throw new IllegalStateException(
                    "No QueryTokenFactory registered for dimension=" + dim
            );
        }

        return factory.create(q, k);
    }

    /** Façade: produce a cloaked token (noise only if enabled) */
    public QueryToken cloakQuery(double[] q, int dim, int k) {
        return createToken(q, k, dim);
    }

    /** Façade: expose QueryTokenFactory */
    public QueryTokenFactory getFactoryForDim(int dim) {
        return factoryForDim(dim);
    }

    /** Façade: expose QueryServiceImpl */
    public QueryServiceImpl getQueryServiceImpl() {
        return (QueryServiceImpl) this.queryService;
    }

    /** Façade: compute server-ms bounded to client window */
    public double boundedServerMs(QueryServiceImpl qs, long startNs, long endNs) {
        final long clientWin = Math.max(0L, endNs - startNs);
        long server = Math.max(0L, qs.getLastQueryDurationNs());
        if (clientWin > 0 && server > clientWin * 1.10) server = clientWin;
        return server / 1_000_000.0;
    }

    /** Façade: expose the logical-result cache */
    public Map<String, List<QueryResult>> getQueryCache() {
        return this.queryCache;
    }

    /** Façade: global query-time accumulator */
    public void addQueryTime(long ns) {
        if (!queryOnlyMode) {
            totalQueryTimeNs += Math.max(0L, ns);
        }
    }
    /** Façade: expose selective re-encryption hook */
    public ReencOutcome doReencrypt(String label, QueryServiceImpl qs) {
        // SECURITY: re-encrypt ALL ANN-touched candidates (not only returned)
        return maybeReencryptTouched(label, qs);
    }
    private void runSelectiveReencryptionIfNeeded() {
        if (!reencEnabled) {
            logger.info("Selective re-encryption disabled");
            return;
        }

        if (!"end".equalsIgnoreCase(System.getProperty("reenc.mode", "end"))) {
            return;
        }

        // ------------------------------------------------------------
        // 1. Force EXACTLY ONE rotation (post-query, per profile)
        // ------------------------------------------------------------
        if (!forcedRotationDone) {
            forceRotateOnceAfterQueries("postQueryProfile");
        }

        // ------------------------------------------------------------
        // 2. Drain touched IDs ONCE
        // ------------------------------------------------------------
        Set<String> touchedIds = reencTracker.drainTouchedIds();
        int touched = touchedIds.size();

        logger.info(
                "Selective re-encryption START | touchedPoints={}",
                touched
        );

        if (touched == 0) {
            logger.warn("No touched vectors; skipping selective re-encryption");
            return;
        }

        // ------------------------------------------------------------
        // 3. Selective re-encryption ONLY on touched points
        // ------------------------------------------------------------
        int targetVer = keyService.getCurrentVersion().getVersion();

        long t0 = System.currentTimeMillis();
        ReencryptReport report =
                reencCoordinator.runOnceWithVersion(targetVer, touchedIds);
        long t1 = System.currentTimeMillis();

        logger.info(
                "Selective re-encryption DONE | touched={} reencrypted={} timeMs={}",
                touched,
                report.reencryptedCount(),
                (t1 - t0)
        );

        // ------------------------------------------------------------
        // 4. DO NOT finalize / delete old keys (paper-safe)
        // ------------------------------------------------------------

        int oldVer = targetVer - 1;
        int remaining = -1;
        if (keyService instanceof KeyRotationServiceImpl kr) {
            remaining = kr.migrationRemaining(oldVer);
        }

        logger.info(
                "Migration status | oldVersion={} remainingVectors={}",
                oldVer, remaining
        );

    }

    /** Façade: max-K across auditK and K-variants */
    public void resetProbeShards() {
        System.clearProperty("probe.shards");
    }
    public boolean kAdaptiveProbeEnabled() {
        return kAdaptive != null && kAdaptive.enabled;
    }
    public void setStabilizationStats(int raw, int fin) {
        this.lastStabilizedRaw = raw;
        this.lastStabilizedFinal = fin;
    }
    public int getLastStabilizedRaw() { return lastStabilizedRaw; }
    public int getLastStabilizedFinal() { return lastStabilizedFinal; }
    public String ratioDenomLabelPublic(boolean trusted) {
        return ratioDenomLabel(trusted);
    }
    static List<double[]> sampleBaseVectors(Path basePath, int dim, int max) throws IOException {
        List<double[]> out = new ArrayList<>(max);
        DefaultDataLoader loader = new DefaultDataLoader();
        FormatLoader fl = loader.lookup(basePath);

        Iterator<double[]> it = fl.openVectorIterator(basePath);
        while (out.size() < max && it.hasNext()) {
            double[] v = it.next();
            if (v != null && v.length == dim) {
                out.add(v);
            }
        }
        return out;
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

            if (backgroundReencryptor != null) {
                backgroundReencryptor.shutdown();
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

    public void shutdownForTests() {
        try {
            // Never allow System.exit in tests
            setExitOnShutdown(false);

            // 1. Kill background re-encryption FIRST
            if (backgroundReencryptor != null) {
                try {
                    backgroundReencryptor.shutdown();
                } catch (Exception ignored) {}
            }

            // 2. Kill main executor HARD
            if (executor != null) {
                executor.shutdownNow();
            }

            // 3. Close RocksDB LAST
            if (metadataManager != null) {
                metadataManager.close();
            }

            // 4. Close base reader if present
            try {
                if (baseReader != null) baseReader.close();
            } catch (Exception ignored) {}

        } catch (Exception ignored) {
            // absolutely no rethrow in tests
        }
    }

    /**
     * Lifecycle:
     *   SETUP
     *   → INDEX
     *   → FINALIZE INDEX
     *   → QUERY + EVALUATE
     *   → SELECTIVE RE-ENCRYPT
     *   → EXPORT
     *   → SHUTDOWN
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            System.err.println(
                    "Usage: <configPath> <dataPath> <queryPath> <keysFilePath> <dimensions> <metadataPath> <groundtruthPath> [batchSize]"
            );
            System.exit(1);
        }

        // ===================== ARGUMENTS =====================
        final String configFile  = args[0];
        final String dataPath    = args[1];
        final String queryPath   = args[2];
        final String keysFile    = args[3];
        final List<Integer> dims =
                Arrays.stream(args[4].split(",")).map(Integer::parseInt).toList();
        final int dimension      = dims.get(0);
        final Path metadataPath  = Paths.get(args[5]);
        String groundtruth       = args[6];
        final int batchSize      = (args.length >= 8)
                ? Integer.parseInt(args[7])
                : 100_000;

        // ===================== QUERY LIMIT =====================
        final int queryLimit =
                Integer.getInteger(
                        "queryLimit",
                        Integer.getInteger("cli.queryLimit", -1)
                );

        if (queryLimit > 0) {
            logger.warn("Query limit ENABLED: {}", queryLimit);
        }

        logger.info("FSP-ANN start | dim={} | batchSize={}", dimension, batchSize);

        // ===================== FILESYSTEM =====================
        Files.createDirectories(metadataPath);
        Path pointsRoot = metadataPath.resolve("points");
        Path metaDBRoot = metadataPath.resolve("metadata");
        Files.createDirectories(pointsRoot);
        Files.createDirectories(metaDBRoot);

        // ===================== MODE =====================
        boolean queryOnlyMode =
                "POINTS_ONLY".equalsIgnoreCase(dataPath)
                        || Boolean.getBoolean("query.only");

        int restoreVer = Integer.getInteger("restore.version", -1);
        if (queryOnlyMode && restoreVer <= 0) {
            restoreVer = detectLatestVersion(pointsRoot);
        }

        Path baseVecs  = Paths.get(dataPath);
        Path queryVecs = Paths.get(queryPath);

        List<double[]> allQueries =
                loadQueriesWithValidation(Paths.get(queryPath), dimension);

        if (allQueries.isEmpty()) {
            throw new IllegalStateException("No queries loaded from " + queryPath);
        }

        final List<double[]> queries;
        if (queryLimit > 0 && queryLimit < allQueries.size()) {
            queries = allQueries.subList(0, queryLimit);
            logger.warn(
                    "Using {} / {} queries (queryLimit active)",
                    queries.size(),
                    allQueries.size()
            );
        } else {
            queries = allQueries;
            logger.info("Using all {} queries", queries.size());
        }

        if (Files.exists(baseVecs)) {
            System.setProperty("base.path", baseVecs.toString());
            logger.debug("Set base.path={} for ratio computation (mode={})",
                    baseVecs, queryOnlyMode ? "query-only" : "full");
        }

        // ===================== METADATA + CRYPTO =====================
        RocksDBMetadataManager metadataManager =
                RocksDBMetadataManager.create(
                        metaDBRoot.toString(),
                        pointsRoot.toString()
                );

        Path resolvedKS = resolveKeyStorePath(keysFile, metadataPath);
        Files.createDirectories(resolvedKS.getParent());
        KeyManager keyManager = new KeyManager(resolvedKS.toString());

        // NOTE: ops / age policy is finalized AFTER config load
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(
                        keyManager,
                        new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                        metaDBRoot.toString(),
                        metadataManager,
                        null
                );

        CryptoService crypto =
                new AesGcmCryptoService(
                        new SimpleMeterRegistry(),
                        keyService,
                        metadataManager
                );
        keyService.setCryptoService(crypto);

        if (queryOnlyMode && restoreVer > 0) {
            keyService.activateVersion(restoreVer);
        }

        // ===================== SYSTEM (SINGLE CONFIG LOAD) =====================
        ForwardSecureANNSystem sys =
                new ForwardSecureANNSystem(
                        configFile,
                        dataPath,
                        keysFile,
                        dims,
                        metadataPath,
                        false,
                        metadataManager,
                        crypto,
                        batchSize
                );

        // >>> SINGLE SOURCE OF TRUTH <<<
        SystemConfig cfg = sys.config;

        // =====================  KEY ROTATION POLICY =====================
        int opsCap = (int) Math.min(Integer.MAX_VALUE, cfg.getOpsThreshold());
        long ageMs = cfg.getAgeThresholdMs();

        keyService.setPolicy(
                new KeyRotationPolicy(
                        queryOnlyMode ? Integer.MAX_VALUE : opsCap,
                        queryOnlyMode ? Long.MAX_VALUE : ageMs
                )
        );

        // ===================== GROUND TRUTH =====================
        boolean needAutoGT =
                "AUTO".equalsIgnoreCase(groundtruth)
                        || !Files.exists(Paths.get(groundtruth));

        if (needAutoGT) {
            if (queryOnlyMode) {
                throw new IllegalStateException(
                        "AUTO GT not allowed in QUERY-ONLY mode"
                );
            }

            int kMax = Arrays.stream(
                    (cfg.getEval() != null && cfg.getEval().kVariants != null)
                            ? cfg.getEval().kVariants
                            : new int[]{100}
            ).max().orElse(100);

            int threads =
                    Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

            groundtruth =
                    GroundtruthPrecompute.run(
                            baseVecs,
                            queryVecs,
                            GroundtruthPrecompute.defaultOutputForQuery(queryVecs),
                            kMax,
                            threads
                    ).toString();
        }

        // =====================================================================
        // QUERY-ONLY MODE
        // =====================================================================
        if (queryOnlyMode) {

            sys.setQueryOnlyMode(true);

            if (restoreVer > 0) {
                int restored = sys.restoreIndexFromDisk(restoreVer);
                logger.info(
                        "Restored index | version={} | points={}",
                        restoreVer,
                        restored
                );
            }

            sys.finalizeForSearch();

            GroundtruthManager gt = new GroundtruthManager();
            gt.load(groundtruth);

            if ("gt".equalsIgnoreCase(cfg.getRatio().source) || "auto".equalsIgnoreCase(cfg.getRatio().source)) {

                int gtSampleSize = cfg.getRatio().gtSample;       // e.g., 100
                double gtTolerance = cfg.getRatio().gtMismatchTolerance;  // e.g., 0.05

                if (gtSampleSize <= 0) gtSampleSize = 100;
                if (gtTolerance <= 0) gtTolerance = 0.05;

                logger.info("Validating groundtruth: samples={}, tolerance={}%",
                        gtSampleSize, gtTolerance * 100);

                GroundtruthValidator.ValidationResult validation =
                        GroundtruthValidator.validate(
                                baseVecs,           // Path to base vectors
                                queries,            // Query vectors
                                gt,                 // Groundtruth manager
                                dimension,          // Vector dimension
                                gtSampleSize,       // Number of queries to validate
                                gtTolerance         // Allowed mismatch rate
                        );

                if (!validation.valid) {
                    logger.error("GT Validation FAILED: {}", validation.message);
                    logger.error("Mismatched queries (first 10): {}", validation.mismatchedQueries);
                    throw new IllegalStateException(validation.message);
                }
                logger.info(
                        "GT validation confirms ID == base offset invariant for dim={}",
                        dimension
                );
                logger.info("GT Validation PASSED: {}", validation.message);
            }

            sys.runQueries(queries, dimension, gt, true);

            sys.runSelectiveReencryptionIfNeeded();

            sys.shutdown();

            if (cfg.getOutput() != null && cfg.getOutput().exportArtifacts) {
                sys.exportArtifacts(sys.resultsDir);
            }

            logger.info("FSP-ANN complete | QUERY-ONLY");
            return;
        }

        // =====================================================================
        // FULL MODE
        // =====================================================================

        // ---- INDEX ----
        long t0 = System.currentTimeMillis();
        sys.indexStream(dataPath, dimension);
        metadataManager.flush();
        logger.info(
                "Index complete | time={}s",
                (System.currentTimeMillis() - t0) / 1000.0
        );

        // ---- FINALIZE INDEX ----
        sys.finalizeForSearch();

        // ---- QUERY + EVALUATE ----
        GroundtruthManager gt = new GroundtruthManager();
        gt.load(groundtruth);

        long q0 = System.currentTimeMillis();
        sys.runQueries(
                queries,
                dimension,
                gt,
                true
        );
        logger.info(
                "Query complete | time={}s",
                (System.currentTimeMillis() - q0) / 1000.0
        );

        // ---- SELECTIVE RE-ENCRYPT ----
        sys.runSelectiveReencryptionIfNeeded();

        // AFTER selective re-encryption (appendix only)
        if (Boolean.getBoolean("reenc.fullMigration")) {
            logger.warn("APPENDIX MODE: Running FULL re-encryption migration");
            keyService.finalizeRotation();   // migrate all remaining old-version points
        }

        // ---- SHUTDOWN ----
        sys.shutdown();

        // ---- EXPORT ----
        if (cfg.getOutput() != null && cfg.getOutput().exportArtifacts) {
            sys.exportArtifacts(sys.resultsDir);
        }

        logger.info("FSP-ANN complete");
    }
}