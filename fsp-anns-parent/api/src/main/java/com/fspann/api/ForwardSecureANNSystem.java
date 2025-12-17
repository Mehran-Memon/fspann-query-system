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
import com.fspann.common.ReencryptReport;
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
import java.util.stream.Collectors;


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
    private final TopKProfiler topKProfiler;
    private volatile boolean queryOnlyMode = false;
    private BaseVectorReader baseReader = null;
    private final ReencryptionTracker reencTracker;
    private final SelectiveReencCoordinator reencCoordinator;
    private QueryExecutionEngine engine;
    private final MicrometerProfiler microProfiler;

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
    private final BackgroundReencryptionScheduler backgroundReencryptor;

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

    // Stabilization diagnostics
    private volatile int lastStabilizedRaw = 0;
    private volatile int lastStabilizedFinal = 0;

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

        // ---- basic invariants ----
        Objects.requireNonNull(configPath, "Config path cannot be null");
        Objects.requireNonNull(keysFilePath, "Keys file path cannot be null");
        Objects.requireNonNull(dimensions, "Dimensions cannot be null");
        Objects.requireNonNull(metadataPath, "Metadata path cannot be null");
        if (dimensions.isEmpty()) throw new IllegalArgumentException("Dimensions list cannot be empty");
        if (batchSize <= 0) throw new IllegalArgumentException("Batch size must be positive");

        this.verbose = verbose;
        this.BATCH_SIZE = batchSize;
        this.executor = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
        this.configPath = normalizePath(configPath);

        // ---- load config ----
        SystemConfig cfg;
        try {
            cfg = new com.fspann.api.ApiSystemConfig(this.configPath).getConfig();
        } catch (IOException e) {
            logger.error("Failed to initialize configuration: {}", configPath, e);
            throw e;
        }
        this.config = cfg;

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
                : new int[]{1, 5, 10, 20, 40, 60, 80, 100};

        // selective re-encryption global toggle
        this.reencEnabled = propOr(
                config.isReencryptionGloballyEnabled(),
                "reenc.enabled", "reencryption.enabled", "reencrypt.enabled"
        );

        // ==== audit subsystem ====

        boolean enableAudit = false;
        int aK = 100, aEvery = 100, aWorst = 25;

        try {
            var ac = config.getAudit();
            if (ac != null) {
                enableAudit = ac.enable;
                if (ac.k > 0) aK = ac.k;
                if (ac.sampleEvery > 0) aEvery = ac.sampleEvery;
                if (ac.worstKeep > 0) aWorst = ac.worstKeep;
            }
        } catch (Throwable ignore) {}

        enableAudit = enableAudit || propOr(true, "output.audit", "audit");

        this.auditEnable = enableAudit;
        this.auditK = aK;
        this.auditSampleEvery = aEvery;
        this.auditWorstKeep = aWorst;

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

        this.topKProfiler = new TopKProfiler(resultsDir.toString());

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

        // ==== audit files ====
        RetrievedAudit ra = null;
        if (auditEnable) {
            try {
                ra = new RetrievedAudit(this.resultsDir);
            } catch (IOException ioe) {
                logger.warn("Audit writer init failed; audit disabled", ioe);
            }
        }
        this.retrievedAudit = ra;

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
            int lambda = Math.max(1, config.getPaper().lambda);
            int m = Math.max(1, config.getPaper().m);
            long seed = config.getPaper().seed;

            int divisions = Math.max(1, config.getPaper().divisions);
            int expected = config.getPaper().divisions;
            if (divisions != expected) {
                throw new IllegalStateException(
                        "TokenFactory divisions mismatch: factory=" + divisions +
                                " config=" + expected +
                                " (configPath=" + this.configPath + ")"
                );
            }

                tokenFactories.put(
                        dim,
                        new QueryTokenFactory(
                                cryptoService,
                                keyService,
                                indexService,   // REQUIRED
                                config,
                                divisions
                        )
                );

                logger.info("TokenFactory created (MSANNP): dim={} m={} lambda={} divisions={} seed={}", dim, m, lambda, divisions, seed);
            }

        int primaryDim = dimensions.get(0);
        QueryTokenFactory qtf = tokenFactories.get(primaryDim);

        // ==== QueryService ====
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService, qtf, cfg);
        // ==== touch accounting / selective re-encryption ====
        this.reencTracker = new ReencryptionTracker();
        if (this.queryService instanceof QueryServiceImpl qs) {
            qs.setReencryptionTracker(reencTracker);
            qs.setStabilizationCallback(this::setStabilizationStats);
        }
        this.engine = new QueryExecutionEngine(this, profiler, K_VARIANTS);

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
        Objects.requireNonNull(vectors, "Vectors cannot be null");
        if (dim <= 0) throw new IllegalArgumentException("Dimension must be positive");
        if (vectors.isEmpty()) return;

        long startNs = System.nanoTime();
        if (profiler != null) profiler.start("batchInsert");

        for (int offset = 0; offset < vectors.size(); offset += BATCH_SIZE) {
            List<double[]> slice = vectors.subList(offset, Math.min(offset + BATCH_SIZE, vectors.size()));

            List<double[]> valid = new ArrayList<>(slice.size());
            List<String> ids = new ArrayList<>(slice.size());

            // Create a NEW list for THIS batch only
            List<EncryptedPoint> batchEncrypted = new ArrayList<>();

            // Get current key version ONCE per batch
            KeyVersion currentKV = keyService.getCurrentVersion();

            for (int j = 0; j < slice.size(); j++) {
                double[] v = slice.get(j);
                long ord = fileOrdinal.getAndIncrement();

                if (v == null) {
                    logger.warn("Skipping null vector at ordinal={}", ord);
                    continue;
                }
                if (v.length != dim) {
                    logger.warn("Skipping vector at ordinal={} with dim={} (expected {})", ord, v.length, dim);
                    continue;
                }

                String pointId = Long.toString(ord);
                ids.add(pointId);
                valid.add(v);

                // ===== USE CRYPTOSERVICE DIRECTLY - No duplicate encryption =====
                try {
                    EncryptedPoint ep = cryptoService.encryptToPoint(
                            pointId,                    // id
                            v,                          // plaintext vector
                            currentKV.getKey()          // secret key
                    );

                    if (ep == null) {
                        logger.error("cryptoService.encryptToPoint returned null for ordinal {}", ord);
                        ids.remove(ids.size() - 1);
                        valid.remove(valid.size() - 1);
                        continue;
                    }

                    if (ep.getCiphertext() == null || ep.getCiphertext().length == 0) {
                        logger.error("Encryption produced empty ciphertext for ordinal {}", ord);
                        ids.remove(ids.size() - 1);
                        valid.remove(valid.size() - 1);
                        continue;
                    }

                    // Add to batch
                    batchEncrypted.add(ep);
                    if (verbose) {
                        logger.debug("Encrypted vector {} (key v{})", pointId, ep.getKeyVersion());
                    }

                } catch (Exception e) {
                    logger.error("Failed to encrypt vector at ordinal {}", ord, e);
                    ids.remove(ids.size() - 1);
                    valid.remove(valid.size() - 1);
                    continue;
                }
            }

            if (valid.isEmpty()) {
                logger.warn("No valid vectors in batch starting at offset {}", offset);
                continue;
            }

            // ===== CRITICAL: Persist THIS batch BEFORE indexing =====
            try {
                for (EncryptedPoint ep : batchEncrypted) {
                    metadataManager.saveEncryptedPoint(ep);
                }
                // Flush to ensure durability
                metadataManager.flush();

                if (verbose) {
                    logger.debug("Persisted and flushed {} encrypted points to metadata", batchEncrypted.size());
                }
            } catch (Exception e) {
                logger.error("Failed to persist {} encrypted points in batch starting at {}",
                        batchEncrypted.size(), offset, e);
                throw new RuntimeException("Batch metadata persistence failed at offset " + offset, e);
            }

            // ===== Rotate keys BEFORE indexing =====
            keyService.rotateIfNeeded();

            // ===== Index all valid vectors =====
            try {
                for (int j = 0; j < valid.size(); j++) {
                    indexService.insert(ids.get(j), valid.get(j));
                }
                indexedCount.addAndGet(valid.size());
                totalInserted += valid.size();

                if (verbose) {
                    logger.debug("Indexed {} vectors from batch at offset {}", valid.size(), offset);
                }
            } catch (Exception e) {
                logger.error("Failed to index batch starting at offset {}", offset, e);
                throw new RuntimeException("Batch indexing failed at offset " + offset, e);
            }
        }

        if (profiler != null) {
            profiler.stop("batchInsert");
            List<Long> timings = profiler.getTimings("batchInsert");
            long durationNs = (!timings.isEmpty() ? timings.get(timings.size() - 1) : (System.nanoTime() - startNs));
            totalIndexingTimeNs += durationNs;
        }

        logger.info("batchInsert complete: {} vectors inserted this call", totalInserted);
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

            // ===== Get current key version =====
            KeyVersion currentKV = keyService.getCurrentVersion();

            // ===== USE CRYPTOSERVICE DIRECTLY - single encryption operation =====
            EncryptedPoint encryptedPoint;
            try {
                encryptedPoint = cryptoService.encryptToPoint(
                        id,                         // id
                        vector,                     // plaintext vector
                        currentKV.getKey()          // secret key
                );

                if (encryptedPoint == null) {
                    throw new RuntimeException("cryptoService.encryptToPoint returned null");
                }

                if (encryptedPoint.getCiphertext() == null || encryptedPoint.getCiphertext().length == 0) {
                    throw new RuntimeException("Encryption produced empty ciphertext");
                }

            } catch (Exception e) {
                logger.error("Vector encryption failed for id={}", id, e);
                throw new RuntimeException("Vector encryption failed for " + id, e);
            }

            // ===== CRITICAL: Persist to metadata BEFORE indexing =====
            try {
                metadataManager.saveEncryptedPoint(encryptedPoint);
                if (verbose) {
                    logger.debug("Persisted encrypted point {} to metadata (key v{})",
                            id, encryptedPoint.getKeyVersion());
                }
            } catch (Exception e) {
                logger.error("Failed to persist encrypted point {} to metadata", id, e);
                throw new RuntimeException("Metadata persistence failed for " + id, e);
            }

            // ===== Index the plaintext vector =====
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

    QueryTokenFactory factoryForDim(int dim) {
        QueryTokenFactory qtf = tokenFactories.get(dim);
        if (qtf == null)
            throw new IllegalStateException("No QueryTokenFactory for dim=" + dim);
        return qtf;
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
        /*                      1. RAW PROFILER CSV EXPORT                         */
        /* ====================================================================== */

        Path profilerCsv = outDir.resolve("profiler_metrics.csv");

        if (profiler != null) {
            profiler.exportToCSV(profilerCsv.toString());
        }

        /* ====================================================================== */
        /*                 2. COMPUTE ART + AvgRatio BY READING CSV               */
        /* ====================================================================== */

        double avgArt = 0.0;
        double avgRatio = 0.0;

        if (Files.exists(profilerCsv)) {

            List<String> lines = Files.readAllLines(profilerCsv)
                    .stream()
                    .filter(s -> !s.trim().isEmpty())
                    .collect(Collectors.toList());

            if (lines.size() > 1) { // skip header
                double sumArt = 0.0;
                double sumRatio = 0.0;
                int count = 0;

                for (int i = 1; i < lines.size(); i++) {
                    String[] cols = lines.get(i).split(",");

                    if (cols.length < 7) continue;

                    try {
                        double serverMs = Double.parseDouble(cols[1]);
                        double clientMs = Double.parseDouble(cols[2]);
                        double ratio = Double.parseDouble(cols[6]);

                        sumArt += (serverMs + clientMs);
                        sumRatio += ratio;
                        count++;

                    } catch (Exception ignore) {
                    }
                }

                if (count > 0) {
                    avgArt = sumArt / count;
                    avgRatio = sumRatio / count;
                }
            }
        }

        /* ====================================================================== */
        /*                   3. WRITE METRICS SUMMARY (REPRODUCIBLE)              */
        /* ====================================================================== */

        String mode = "partitioned";

        String cfgHash = "NA";
        try {
            Path cfgPath = Paths.get(this.configPath);
            if (Files.exists(cfgPath)) {
                byte[] bytes = Files.readAllBytes(cfgPath);
                cfgHash = toHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            }
        } catch (Exception ignore) {
        }

        int keyVer = -1;
        try {
            keyVer = keyService.getCurrentVersion().getVersion();
        } catch (Exception ignore) {
        }

        Files.writeString(
                outDir.resolve("metrics_summary.txt"),
                String.format(Locale.ROOT,
                        "mode=%s  config_sha256=%s  key_version=v%d%nART(ms)=%.3f%nAvgRatio=%.6f%n",
                        mode, cfgHash, keyVer, avgArt, avgRatio),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
        );


        /* ====================================================================== */
        /*                         4. EXPORT TOP-K EVALUATION                     */
        /* ====================================================================== */

        try {
            topKProfiler.export(outDir.resolve("topk_evaluation.csv").toString());
        } catch (Exception ignore) {
        }


        /* ====================================================================== */
        /*                         5. WRITE QUERIES SNAPSHOT                      */
        /* ====================================================================== */

        if (!recentQueries.isEmpty()) {
            Path qcsv = outDir.resolve("queries.csv");
            try (var w = Files.newBufferedWriter(qcsv)) {
                for (double[] q : recentQueries) {
                    for (int i = 0; i < q.length; i++) {
                        if (i > 0) w.write(",");
                        w.write(Double.toString(q[i]));
                    }
                    w.write("\n");
                }
            }
        }


        /* ====================================================================== */
        /*                      6. STORAGE SUMMARY + PRINTER                      */
        /* ====================================================================== */

        try {
            Aggregates agg = Aggregates.fromProfiler(this.profiler);

            // -----------------------
            // DATASET NAME (fallback)
            // -----------------------
            String dataset;
            String cliDataset = System.getProperty("cli.dataset", "");
            if (!cliDataset.isBlank()) {
                dataset = cliDataset;
            } else {
                String baseProp = System.getProperty("base.path", "");
                if (!baseProp.isBlank()) {
                    dataset = Paths.get(baseProp).getFileName().toString();
                } else {
                    dataset = this.resultsDir.getFileName().toString();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        /* ====================================================================== */
        /*                      7. EvaluationSummaryPrinter CSVs                  */
        /* ====================================================================== */

        try {
            Aggregates agg = Aggregates.fromProfiler(this.profiler);

            // dataset identification fallback
            String dataset;
            String baseProp = System.getProperty("base.path", "");
            if (!baseProp.isBlank()) {
                dataset = Paths.get(baseProp).getFileName().toString();
            } else {
                dataset = this.resultsDir.getFileName().toString();
            }

            String profile;
            String cliProfile = System.getProperty("cli.profile", "");
            if (!cliProfile.isBlank()) {
                profile = cliProfile;
            } else {
                profile = "ideal-system";
            }
            int m = config.getPaper().m;
            int lambda = config.getPaper().lambda;
            int divisions = config.getPaper().divisions;

            long totalIndexMs = Math.round(totalIndexingTimeNs / 1_000_000.0);

            Path summaryCsv = outDir.resolve("summary.csv");

            EvaluationSummaryPrinter.printAndWriteCsv(
                    dataset,
                    profile,
                    m,
                    lambda,
                    divisions,
                    totalIndexMs,
                    agg,
                    summaryCsv
            );
        } catch (Exception e) {
            logger.warn("Failed to run EvaluationSummaryPrinter", e);
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

        StringBuilder sb = new StringBuilder(128);

        // Version + dimension + topK
        sb.append("v").append(token.getVersion())
                .append(":d").append(token.getDimension())
                .append(":k").append(token.getTopK());

        // Partitioned-mode codes (BitSet array)
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
            // No codes available
            sb.append(":nocodes");
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

    // Per-query hook: accumulate touched IDs; optionally do "immediate" re-encryption if requested.
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
                (keyService instanceof KeyRotationServiceImpl)
                        ? (KeyRotationServiceImpl) keyService
                        : null;

        if (!enabled || kr == null) {
            appendReencCsv(
                    reencCsv, "SUMMARY",
                    (keyService != null && keyService.getCurrentVersion() != null)
                            ? keyService.getCurrentVersion().getVersion()
                            : -1,
                    reencMode,
                    uniqueCount, 0, uniqueCount,
                    new ReencryptReport(0, 0, 0L, 0L, 0L)
            );
            return;
        }

        if (uniqueCount == 0 && totalQueryTimeNs > 0) {
            throw new IllegalStateException(
                    "Queries executed but no candidates tracked for reencryption"
            );
        }

        if (uniqueCount == 0) {
            logger.error(
                    "finalizeReencryptionAtEnd: touchedGlobal is EMPTY! " +
                            "This indicates candidate tracking failed. " +
                            "Check QueryServiceImpl.getLastCandidateIds() and ensure " +
                            "lastCandIds is populated in search()."
            );

            appendReencCsv(
                    reencCsv, "SUMMARY",
                    keyService.getCurrentVersion().getVersion(),
                    reencMode,
                    0, 0, 0,
                    new ReencryptReport(0, 0, 0L, 0L, 0L)
            );
            return;
        }

        // ----------------------------
        // BEGIN RE-ENCRYPTION PIPELINE
        // ----------------------------

        kr.freezeRotation(true);

        final ReencryptReport rep;
        final int targetVer;

        try {
            // 1. New version (NO pruning)
            kr.forceRotateNow();
            targetVer = keyService.getCurrentVersion().getVersion();

            // 2. Selective re-encryption
            StorageSizer sizer = () -> dirSize(pointsPath);
            rep = kr.reencryptTouched(allTouchedUnique, targetVer, sizer);

            // 3. Log summary
            appendReencCsv(
                    reencCsv, "SUMMARY",
                    targetVer, reencMode,
                    uniqueCount, 0, uniqueCount,
                    rep
            );

            // 4. Finalize deletion of old keys
            finalizeRotation(targetVer);

        } finally {
            // 5. Always unfreeze
            kr.freezeRotation(false);
        }

        // ----------------------------
        // BEGIN VERIFICATION BLOCK
        // ----------------------------

        boolean ok = true;
        long measuredBytesAfter = dirSize(pointsPath);

        if (rep.reencryptedCount() < 0) ok = false;
        if (rep.bytesAfter() != measuredBytesAfter) ok = false;
        if (rep.touchedCount() != uniqueCount) ok = false;

        if (!ok) {
            try {
                synchronized (REENC_CSV_LOCK) {
                    String line = String.format(
                            Locale.ROOT,
                            "SUMMARY_CHECK,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                            targetVer,
                            reencMode,
                            uniqueCount, 0, uniqueCount,
                            rep.reencryptedCount(),
                            tryGetLong(rep, "alreadyCurrentCount"),
                            tryGetLong(rep, "retriedCount"),
                            rep.timeMs(),
                            rep.bytesDelta(),
                            measuredBytesAfter
                    );
                    Files.writeString(
                            reencCsv,
                            line,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND
                    );
                }
            } catch (IOException e) {
                logger.warn("Failed to append SUMMARY_CHECK", e);
            }
        }
    }

    /**
     * Final phase of key rotation: delete all keys older than the target version.
     * This must be called ONLY after complete re-encryption.
     */
    private void finalizeRotation(int targetVersion) {
        if (!(keyService instanceof KeyRotationServiceImpl kr)) return;

        try {
            var kmGetter = kr.getClass().getDeclaredMethod("getKeyManager");
            kmGetter.setAccessible(true);
            Object kmObj = kmGetter.invoke(kr);
            if (!(kmObj instanceof com.fspann.key.KeyManager km)) {
                logger.error("finalizeRotation: keyManager unavailable");
                return;
            }

            // SAFELY delete all keys older than the target version
            km.deleteKeysOlderThan(targetVersion);
            logger.info("Key rotation finalized: deleted all keys older than v{}", targetVersion);

        } catch (Exception e) {
            logger.error("finalizeRotation(v{}) failed", targetVersion, e);
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

    private static boolean containsInt(int[] a, int v) {
        for (int x : a) if (x == v) return true;
        return false;
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

    public void accumulateTouchedIds(Set<String> ids) {
        if (ids != null && !ids.isEmpty()) {
            touchedGlobal.addAll(ids);
            logger.debug("Accumulated {} touched IDs (total now: {})", ids.size(), touchedGlobal.size());
        }
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
                logger.info("  Loaded batch {}: {} queries (total: {})",
                        batchCount, batch.size(), queries.size());
            }
        } catch (Exception e) {
            logger.warn("DefaultDataLoader failed, trying fallback");
            queries = loadQueriesFallback(queryPath, dimension);
        }

        if (queries.isEmpty()) {
            throw new IllegalStateException("Query loading resulted in empty list");
        }

        logger.info("✓ Loaded {} queries total", queries.size());
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
        QueryToken token = tokenFactories.get(dim).create(q, k);
        return token;
    }

    /** Façade: produce a cloaked token (noise only if enabled) */
    public QueryToken cloakQuery(double[] q, int dim, int k) {
        if (configuredNoiseScale <= 0) {
            return createToken(q, k, dim);
        }
        return factoryForDim(dim).create(q, k); // QueryExecutionEngine adds cloak noise separately
    }

    /** Façade: expose QueryTokenFactory */
    public QueryTokenFactory getFactoryForDim(int dim) {
        return factoryForDim(dim);
    }

    /** Façade: expose QueryServiceImpl */
    public QueryServiceImpl getQueryServiceImpl() {
        return (QueryServiceImpl) this.queryService;
    }

    private double ratioAtKMetric(double[] q,
                                  List<QueryResult> prefix,
                                  int k,
                                  int qIndex,
                                  GroundtruthManager gt,
                                  BaseVectorReader base,
                                  boolean trusted) {

        if (prefix == null || prefix.isEmpty() || k <= 0) return 0.0;

        int[] truth;
        if (ratioSource == RatioSource.GT || (ratioSource == RatioSource.AUTO && trusted)) {
            truth = gt.getGroundtruth(qIndex, k);
        } else {
            truth = topKFromBase(base, q, k);
        }

        if (truth == null || truth.length == 0) return 0.0;

        int hits = 0;
        int upto = Math.min(k, prefix.size());
        for (int i = 0; i < upto; i++) {
            try {
                int id = Integer.parseInt(prefix.get(i).getId());
                if (containsInt(truth, id)) hits++;
            } catch (Exception ignore) {}
        }
        return (double) hits / (double) truth.length;
    }

    /** Façade: compute server-ms bounded to client window */
    public double boundedServerMs(QueryServiceImpl qs, long startNs, long endNs) {
        final long clientWin = Math.max(0L, endNs - startNs);
        long server = Math.max(0L, qs.getLastQueryDurationNs());
        if (clientWin > 0 && server > clientWin * 1.10) server = clientWin;
        return server / 1_000_000.0;
    }

    /** Façade: cache-key for QueryToken */
    public String getCacheKeyOf(QueryToken tok) {
        return cacheKeyOf(tok);
    }

    /** Façade: expose the logical-result cache */
    public Map<String, List<QueryResult>> getQueryCache() {
        return this.queryCache;
    }

    /** Façade: record most recent queries for artifact export */
    public void recordRecentVector(double[] q) {
        recordRecent(q);
    }

    /** Façade: global query-time accumulator */
    public void addQueryTime(long ns) {
        if (!queryOnlyMode) {
            totalQueryTimeNs += Math.max(0L, ns);
        }
    }
    /** Façade: compute ratio@K */
    public double computeRatio(double[] q,
                               List<QueryResult> prefix,
                               int k,
                               int qIndex,
                               GroundtruthManager gt,
                               boolean trusted) {
        if (baseReader == null) return Double.NaN;
        return ratioAtKMetric(q, prefix, k, qIndex, gt, baseReader, trusted);
    }

    /** Façade: compute precision@K */
    public double computePrecision(List<QueryResult> prefix, int[] truth) {
        if (truth == null || truth.length == 0) return 0.0;
        int hits = 0;
        for (int i = 0; i < prefix.size() && i < truth.length; i++) {
            try {
                int id = Integer.parseInt(prefix.get(i).getId());
                if (containsInt(truth, id)) hits++;
            } catch (Exception ignore) {}
        }
        return (double) hits / truth.length;
    }

    /** Façade: GT / BASE label exposure */
    public String ratioDenomLabelPublic(boolean trusted) {
        return ratioDenomLabel(trusted);
    }

    /** Façade: expose selective re-encryption hook */
    public ReencOutcome doReencrypt(String label, QueryServiceImpl qs) {
        Set<String> ids = qs.getLastCandidateIds(); // must exist
        if (ids != null && !ids.isEmpty()) {
            touchedGlobal.addAll(ids);
        }
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

        int touched = touchedGlobal.size();
        if (touched == 0) {
            logger.warn("No candidates touched; skipping selective re-encryption");
            return;
        }

        logger.info("Running selective re-encryption on {} touched points", touched);
        finalizeReencryptionAtEnd();
    }

    /** Façade: max-K across auditK and K-variants */
    public int baseKForToken() {
        return Math.max(
                Math.max(auditK, 100),
                Arrays.stream(K_VARIANTS).max().orElse(100)
        );
    }

    public void resetProbeShards() {
        System.clearProperty("probe.shards");
    }

    public boolean kAdaptiveProbeEnabled() {
        return kAdaptive != null && kAdaptive.enabled;
    }

    /** Test-only: expose evaluation engine */
    public QueryExecutionEngine getEngine() {
        return this.engine;
    }
    public int[] getKVariants() {
        return K_VARIANTS.clone();
    }

    public void setStabilizationStats(int raw, int fin) {
        this.lastStabilizedRaw = raw;
        this.lastStabilizedFinal = fin;
    }
    public int getLastStabilizedRaw() { return lastStabilizedRaw; }
    public int getLastStabilizedFinal() { return lastStabilizedFinal; }

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

        List<double[]> queries = loadQueriesWithValidation(Paths.get(queryPath), dimension);

        if (queries.isEmpty()) {
            throw new IllegalStateException("No queries loaded from " + queryPath);
        }

        if (!queryOnlyMode && Files.exists(baseVecs)) {
            System.setProperty("base.path", baseVecs.toString());
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

            sys.engine.evalBatch(
                    queries,
                    dimension,
                    gt,
                    sys.resultsDir,
                    true
            );

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


        logger.info(
                "Running evalBatch: queries={}, kVariants={}",
                queries.size(),
                Arrays.toString(sys.getKVariants())
        );


        long q0 = System.currentTimeMillis();
        sys.engine.evalBatch(
                queries,
                dimension,
                gt,
                sys.resultsDir,
                true
        );
        logger.info(
                "Query complete | time={}s",
                (System.currentTimeMillis() - q0) / 1000.0
        );

        // ---- SELECTIVE RE-ENCRYPT ----
        sys.runSelectiveReencryptionIfNeeded();

        // ---- SHUTDOWN ----
        sys.shutdown();

        // ---- EXPORT ----
        if (cfg.getOutput() != null && cfg.getOutput().exportArtifacts) {
            sys.exportArtifacts(sys.resultsDir);
        }

        logger.info("FSP-ANN complete");
    }


}