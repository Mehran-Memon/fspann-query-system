package com.fspann.index.paper;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.crypto.AesGcmCryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PartitionedIndexService - MSANNP Implementation with Data-Adaptive GFunctions
 * ==============================================================================
 *
 * KEY CHANGE (v2.0):
 * ------------------
 * Now uses GFunctionRegistry for data-adaptive omega computation.
 *
 * The registry is initialized automatically during the first batch insert
 * using a sample of actual vectors. This ensures:
 *   1. omega_j values are computed from actual projection ranges
 *   2. Index and query use IDENTICAL GFunction parameters
 *   3. Works correctly for ANY dataset (SIFT, Glove, Deep1B, etc.)
 *
 * INITIALIZATION FLOW:
 * --------------------
 * 1. First call to insert() triggers initializeRegistry() if not done
 * 2. initializeRegistry() samples staged vectors and calls GFunctionRegistry.initialize()
 * 3. All subsequent insert() and code() calls use cached GFunctions
 *
 * QUERY FLOW:
 * -----------
 * 1. QueryTokenFactory calls code(vec, seedOverride) or uses GFunctionRegistry directly
 * 2. GFunctionRegistry returns the SAME GFunction used during indexing
 * 3. Codes match -> correct candidates returned
 */
public final class PartitionedIndexService implements IndexService {

    private static final Logger logger =
            LoggerFactory.getLogger(PartitionedIndexService.class);

//    private static final int DEFAULT_BUILD_THRESHOLD =
//            Math.max(20_000, Runtime.getRuntime().availableProcessors() * 20_000);
private static final int DEFAULT_BUILD_THRESHOLD = 20_000;

    // Minimum sample size for GFunction initialization
    private static final int MIN_SAMPLE_SIZE = 1000;
    private static final int MAX_SAMPLE_SIZE = 10000;

    private final RocksDBMetadataManager metadata;
    private final SystemConfig cfg;
    private final StorageMetrics storageMetrics;
    private final KeyRotationServiceImpl keyService;
    private final AesGcmCryptoService cryptoService;

    // dim -> per-table states (tables = ℓ)
    private final Map<Integer, DimensionState[]> dims = new ConcurrentHashMap<>();

    private volatile boolean frozen = false;
    private volatile int lastRawVisited = 0;

    // Buffer for collecting sample vectors before initialization
    private final List<double[]> initSampleBuffer = Collections.synchronizedList(new ArrayList<>());

    private final ThreadLocal<Integer> lastTouched =
            ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Set<String>> lastTouchedIds =
            ThreadLocal.withInitial(() -> new HashSet<>(2048));
    private final ThreadLocal<Integer> probeOverride =
            ThreadLocal.withInitial(() -> -1);

    private final List<PendingVector> pendingVectors =
            Collections.synchronizedList(new ArrayList<>());

    private static final class DimensionState {
        final int dim;
        final int table; // 0..L-1

        final List<DivisionState> divisions = new ArrayList<>();

        final List<EncryptedPoint> staged = new ArrayList<>();
        final List<BitSet[]> stagedCodes = new ArrayList<>();

        DimensionState(int dim, int table) {
            this.dim = dim;
            this.table = table;
        }
    }

    private static final class DivisionState {
        List<PrefixPartitioner.Partition> prefixPartitions;
    }

    private static class PendingVector {
        final String id;
        final double[] vector;

        PendingVector(String id, double[] vector) {
            this.id = id;
            this.vector = vector.clone();
        }
    }

    public PartitionedIndexService(
            RocksDBMetadataManager metadata,
            SystemConfig cfg,
            KeyRotationServiceImpl keyService,
            AesGcmCryptoService cryptoService) {

        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.cryptoService = Objects.requireNonNull(cryptoService, "cryptoService");

        this.storageMetrics = metadata.getStorageMetrics();
        if (this.storageMetrics == null) {
            throw new IllegalStateException(
                    "StorageMetrics not available from RocksDBMetadataManager"
            );
        }

        logger.info("PartitionedIndexService initialized | buildThreshold={} | registry=data-adaptive",
                DEFAULT_BUILD_THRESHOLD);
        SystemConfig.PaperConfig pc = cfg.getPaper();
        logger.info(
                "CONFIG ASSERT: m={} lambda={} tables={} divisions={} seed={}",
                pc.getM(), pc.getLambda(), pc.getTables(), pc.getDivisions(), pc.getSeed()
        );

    }

    // =====================================================
    // GFUNCTION REGISTRY INITIALIZATION
    // =====================================================

    /**
     * Force initialization with current sample buffer.
     * Called when we have enough samples or at build time.
     */
    private synchronized void initializeRegistry() {
        if (GFunctionRegistry.isInitialized()) {
            logger.debug("GFunctionRegistry already initialized (concurrent call)");
            return;
        }

        logger.info("Initializing GFunctionRegistry");
        logger.info("Sample buffer size: {}", initSampleBuffer.size());

        if (initSampleBuffer.size() < MIN_SAMPLE_SIZE) {
            throw new IllegalStateException(
                    "Refusing to initialize GFunctionRegistry with sampleSize=" +
                            initSampleBuffer.size() +
                            " (< MIN_SAMPLE_SIZE=" + MIN_SAMPLE_SIZE + ")"
            );
        }

        if (initSampleBuffer.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot initialize GFunctionRegistry: no sample vectors available"
            );
        }

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int dimension = initSampleBuffer.get(0).length;

        logger.info("Parameters: dim={} m={} λ={} tables={} divisions={} sampleSize={}",
                dimension, pc.getM(), pc.getLambda(), pc.getTables(), pc.getDivisions(), initSampleBuffer.size()
        );

        try {
            GFunctionRegistry.initialize(
                    initSampleBuffer,
                    dimension,
                    pc.getM(),
                    pc.getLambda(),
                    pc.getSeed(),
                    pc.getTables(),
                    pc.getDivisions()
            );

            logger.info("GFunctionRegistry initialized successfully");
            logger.info("Stats: {}", GFunctionRegistry.getStats());

            // ← ADD: Sample omega values for verification
            try {
                Coding.GFunction g0 = GFunctionRegistry.get(dimension, 0, 0);

                // Log first 5 omega values for first GFunction
                double[] omega = g0.omega;
                if (omega != null && omega.length > 0) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < Math.min(5, omega.length); i++) {
                        sb.append(String.format("%.2f", omega[i]));
                        if (i < Math.min(4, omega.length - 1)) sb.append(", ");
                    }
                    logger.info("Sample omega[table=0,div=0]: [{}...] (showing first 5 of {})",
                            sb.toString(), omega.length);

                    // Check if hardcoded (all values near 1.0)
                    boolean hardcoded = true;
                    for (double w : omega) {
                        if (Math.abs(w - 1.0) > 0.1) {
                            hardcoded = false;
                            break;
                        }
                    }

                    if (hardcoded) {
                        logger.error("⚠️ OMEGA VALUES ARE HARDCODED! All values near 1.0 - this will cause zero recall!");
                    } else {
                        logger.info("✓ Omega values are data-adaptive (not hardcoded)");
                    }
                }
            } catch (Exception e) {
                logger.warn("Could not inspect GFunction omega values", e);
            }

        } catch (Exception e) {
            logger.error("FATAL: GFunctionRegistry initialization failed!", e);
            throw new RuntimeException("Failed to initialize GFunctionRegistry", e);
        }

        initSampleBuffer.clear();
    }

    /**
     * Ensure registry is initialized before any coding operation.
     */
    private void ensureRegistryInitialized() {
        if (GFunctionRegistry.isInitialized()) return;

        throw new IllegalStateException(
                "GFunctionRegistry not initialized. " +
                        "Index must ingest at least " + MIN_SAMPLE_SIZE +
                        " vectors before search or coding."
        );
    }


    // =====================================================
    // INSERT
    // =====================================================

    @Override
    public void insert(String id, double[] vector) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(vector, "vector cannot be null");

        if (GFunctionRegistry.isInitialized()) {
            int regDim = (int) GFunctionRegistry.getStats().get("dimension");
            if (vector.length != regDim) {
                throw new IllegalArgumentException(
                        "Mixed dimensions not supported in single index: got "
                                + vector.length + ", expected " + regDim
                );
            }
        }

        if (!GFunctionRegistry.isInitialized()) {
            synchronized (initSampleBuffer) {
                if (!GFunctionRegistry.isInitialized()
                        && initSampleBuffer.size() < MAX_SAMPLE_SIZE) {
                    initSampleBuffer.add(vector.clone());
                }
                if (initSampleBuffer.size() >= MIN_SAMPLE_SIZE) {
                    initializeRegistry();
                }
            }
        }

        if (!GFunctionRegistry.isInitialized()) {
            // Stage plaintext for later indexing
            PendingVector pv = new PendingVector(id, vector);
            synchronized (pendingVectors) {
                pendingVectors.add(pv);
            }
            return;
        }


        // -------- INDEXING PHASE --------
        EncryptedPoint ep;
        try {
            ep = cryptoService.encrypt(id, vector, keyService.getCurrentVersion());
            if (ep == null) throw new RuntimeException("encrypt() returned null");
        } catch (Exception e) {
            throw new RuntimeException("Encryption failed for vector " + id, e);
        }

        insert(ep, vector);
    }

    public void insert(EncryptedPoint pt, double[] vec) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        Objects.requireNonNull(vec, "vector cannot be null");

        int dim = vec.length;

        // Persist once (global metadata)
        try {
            metadata.saveEncryptedPoint(pt);
        } catch (IOException e) {
            logger.error("Failed to persist encrypted point {}: {}", pt.getId(), e.getMessage());
            throw new RuntimeException("Persistence failed for point " + pt.getId(), e);
        }

        DimensionState[] tables = dims.computeIfAbsent(dim, d -> newTableStates(d));

        // Stage into ALL tables (ℓ)
        for (DimensionState S : tables) {

            BitSet[] codes = new BitSet[cfg.getPaper().getDivisions()];

            for (int d = 0; d < codes.length; d++) {
                Coding.GFunction G =
                        GFunctionRegistry.get(dim, S.table, d);
                codes[d] = Coding.C(vec, G);
            }

            synchronized (S) {
                S.staged.add(pt);
                S.stagedCodes.add(codes);

            }
        }
    }

    private DimensionState[] newTableStates(int dim) {
        int L = cfg.getPaper().getTables();
        DimensionState[] arr = new DimensionState[L];
        for (int t = 0; t < L; t++) arr[t] = new DimensionState(dim, t);
        return arr;
    }

    private long tableSeed(int table) {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return pc.getSeed() + (table * 1_000_003L);
    }

    /**
     * Returns the number of tables (L) configured for the index.
     */
    public int numTables() {
        return cfg.getPaper().getTables();
    }

    // =====================================================
    // BUILD (Algorithm-2) per table
    // =====================================================

    private void build(DimensionState S) {
        ensureRegistryInitialized();

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int divisions = pc.getDivisions();
        int fullBits = pc.getM() * pc.getLambda();

        if (!S.divisions.isEmpty()) {
            logger.warn("build() called on non-empty divisions - skipping");
            return;
        }

        //Log GFunction used for first build
        if (S.table == 0) {
            Coding.GFunction g0 = GFunctionRegistry.get(S.dim, 0, 0);
            logger.info("BUILD GFunction[table=0,div=0]: seed={}, m={}, lambda={}, omega[0]={}, omega[5]={}",
                    g0.seed, g0.m, g0.lambda,
                    g0.omega[0],
                    g0.omega.length > 5 ? g0.omega[5] : -1.0
            );

            // Log first 5 omega values
            StringBuilder omegaStr = new StringBuilder();
            for (int i = 0; i < Math.min(5, g0.omega.length); i++) {
                omegaStr.append(String.format("%.2f", g0.omega[i]));
                if (i < Math.min(4, g0.omega.length - 1)) omegaStr.append(", ");
            }
            logger.info("BUILD omega sample[table=0,div=0]: [{}...]", omegaStr.toString());

            // ← ADD: Log a sample vector's BitSet code
            if (!S.staged.isEmpty() && S.stagedCodes.size() > 0) {
                BitSet sampleCode = S.stagedCodes.get(0)[0]; // First vector, first division
                logger.info("BUILD BitSet sample[table=0,div=0]: length={}, cardinality={}, first10bits={}",
                        sampleCode.length(),
                        sampleCode.cardinality(),
                        getBitString(sampleCode, 10)
                );
            }
        }

        for (int d = 0; d < divisions; d++) {
            Map<String, BitSet> idToCode = new HashMap<>(S.staged.size());

            for (int i = 0; i < S.staged.size(); i++) {
                idToCode.put(
                        S.staged.get(i).getId(),
                        S.stagedCodes.get(i)[d]
                );
            }

            DivisionState div = new DivisionState();
            div.prefixPartitions = PrefixPartitioner.build(idToCode, fullBits);

            S.divisions.add(div);
        }

        S.staged.clear();
        S.stagedCodes.clear();

        logger.debug(
                "Built MSANNP prefix partitions: dim={} table={} divisions={}",
                S.dim, S.table, S.divisions.size()
        );
    }

    // ← ADD: Helper method
    private String getBitString(BitSet bs, int maxBits) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(maxBits, bs.length()); i++) {
            sb.append(bs.get(i) ? '1' : '0');
        }
        return sb.toString();
    }


    // =====================================================
    // REAL RUN path: candidate IDs only (unions across tables)
    // =====================================================

    /**
     * Lookup candidate IDs from partitioned index.
     *
     * Key behavior:
     * 1. Validates that token has codes for all tables
     * 2. Iterates through relaxation levels (0 to lambda)
     * 3. Unions candidates across all tables
     * 4. Stops when HARD_CAP (global safety limit) is reached
     */
    public List<String> lookupCandidateIds(QueryToken token) {
        Objects.requireNonNull(token, "token");
        if (!frozen) throw new IllegalStateException("Index not finalized");

        int dim = token.getDimension();
        DimensionState[] tables = dims.get(dim);
        if (tables == null) return List.of();

        BitSet[][] qCodes = token.getBitCodes();
        if (qCodes == null) {
            throw new IllegalStateException(
                    "MSANNP violation: QueryToken missing BitSet codes"
            );
        }

        int K = token.getTopK();
        int requiredK = Math.max(K, cfg.getEval().getMaxK());
        int HARD_CAP = Math.max(
                cfg.getRuntime().getMaxGlobalCandidates(),
                50 * K
        );

        Map<String, Integer> score = new HashMap<>(HARD_CAP);
        Set<String> deleted = new HashSet<>(2048);

        int rawSeen = 0;
        int fullBits = cfg.getPaper().getM() * cfg.getPaper().getLambda();
        int maxRelax = fullBits - 1;

        // Track candidates at each relaxation level
        int[] candidatesAtRelax = new int[Math.min(5, maxRelax + 1)];

        // Track first query diagnostics
        boolean isFirstRelax = true;

        for (int relax = 0; relax <= maxRelax && score.size() < HARD_CAP; relax++) {
            int prefixLen = fullBits - relax;
            int partitionIndex = fullBits - prefixLen;  // This is the list index

            for (int t = 0; t < tables.length && score.size() < HARD_CAP; t++) {
                DimensionState S = tables[t];

                for (int d = 0; d < S.divisions.size() && score.size() < HARD_CAP; d++) {
                    DivisionState div = S.divisions.get(d);

                    // CRITICAL: Verify partition list size
                    if (div.prefixPartitions == null || div.prefixPartitions.isEmpty()) {
                        logger.error("FATAL: Division [t={}, d={}] has no partitions!", t, d);
                        continue;
                    }

                    if (partitionIndex >= div.prefixPartitions.size()) {
                        logger.error("FATAL: partitionIndex={} >= partitions.size()={} for relax={}, prefixLen={}",
                                partitionIndex, div.prefixPartitions.size(), relax, prefixLen);
                        continue;
                    }

                    // Get the partition
                    PrefixPartitioner.Partition part = div.prefixPartitions.get(partitionIndex);

                    // Verify partition prefixLen matches
                    if (part.prefixLen != prefixLen) {
                        logger.error("PARTITION MISMATCH: Expected prefixLen={}, got partition.prefixLen={} at index={}",
                                prefixLen, part.prefixLen, partitionIndex);
                    }

                    BitSet q = qCodes[t][d];
                    PrefixPartitioner.PrefixKey key = new PrefixPartitioner.PrefixKey(q, prefixLen);

                    // LOG: First query detailed diagnostics
                    if (isFirstRelax && t == 0 && d == 0) {
                        logger.info("QUERY PARTITION LOOKUP [relax={}, t={}, d={}]:", relax, t, d);
                        logger.info("  prefixLen={}, partitionIndex={}", prefixLen, partitionIndex);
                        logger.info("  queryKey={}", key);
                        logger.info("  partition.prefixLen={}, partition.buckets.size={}",
                                part.prefixLen, part.buckets.size());
                        logger.info("  queryKey IN partition: {}", part.buckets.containsKey(key));

                        if (!part.buckets.containsKey(key)) {
                            logger.warn("QUERY KEY NOT FOUND! Showing first 3 partition keys:");
                            int count = 0;
                            for (PrefixPartitioner.PrefixKey partKey : part.buckets.keySet()) {
                                logger.warn("    Partition key {}: {}", count, partKey);
                                if (++count >= 3) break;
                            }
                        }
                    }

                    List<String> ids = part.buckets.get(key);
                    if (ids == null) continue;

                    if (isFirstRelax && t == 0 && d == 0) {
                        logger.info("Found {} candidates in this partition", ids.size());
                    }

                    for (String id : ids) {
                        if (deleted.contains(id)) continue;
                        if (metadata.isDeleted(id)) {
                            deleted.add(id);
                            continue;
                        }

                        int weight = (relax << 12) + (t << 6) + d;
                        score.merge(id, weight, Integer::sum);
                        rawSeen++;

                        if (score.size() >= HARD_CAP) break;
                    }
                }
            }

            // Capture candidates at this relaxation level
            if (relax < candidatesAtRelax.length) {
                candidatesAtRelax[relax] = score.size();
            }

            isFirstRelax = false;
        }

        List<Map.Entry<String, Integer>> ordered = new ArrayList<>(score.entrySet());
        ordered.sort(Comparator.comparingInt(Map.Entry::getValue));

        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(ordered.size(), HARD_CAP); i++) {
            out.add(ordered.get(i).getKey());
        }

        lastTouched.set(out.size());
        lastTouchedIds.get().clear();
        lastTouchedIds.get().addAll(out);
        lastRawVisited = rawSeen;

        // Summary log
        StringBuilder relaxBreakdown = new StringBuilder();
        for (int r = 0; r < candidatesAtRelax.length; r++) {
            relaxBreakdown.append("r").append(r).append("=").append(candidatesAtRelax[r]);
            if (r < candidatesAtRelax.length - 1) relaxBreakdown.append(", ");
        }

        logger.info("LSH Lookup: K={} | rawSeen={} | uniqueCands={} | returned={} | deleted={} | [{}]",
                K, rawSeen, score.size(), out.size(), deleted.size(), relaxBreakdown.toString());

        if (out.size() < requiredK) {
            logger.warn("Recall floor violated: returned={} required={}", out.size(), requiredK);
        }

        return out;
    }
    public EncryptedPoint loadPointIfActive(String id) {
        if (metadata.isDeleted(id)) return null;
        try {
            return metadata.loadEncryptedPoint(id);
        } catch (Exception e) {
            return null;
        }
    }


    // =====================================================
    // FINALIZATION
    // =====================================================

    private void directInsert(EncryptedPoint pt, double[] vec) {
        int dim = vec.length;

        try {
            metadata.saveEncryptedPoint(pt);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        DimensionState[] tables = dims.computeIfAbsent(dim, d -> newTableStates(d));

        int divisions = cfg.getPaper().getDivisions();

        for (DimensionState S : tables) {

            BitSet[] codes = new BitSet[divisions];

            for (int d = 0; d < divisions; d++) {
                Coding.GFunction G =
                        GFunctionRegistry.get(dim, S.table, d);
                codes[d] = Coding.C(vec, G);
            }

            synchronized (S) {
                S.staged.add(pt);
                S.stagedCodes.add(codes);
            }
        }
    }

    public void finalizeForSearch() {
        if (frozen) {
            logger.info("Index already finalized");
            return;
        }

        logger.info("Finalizing index for search...");

        if (!GFunctionRegistry.isInitialized()) {
            if (initSampleBuffer.size() >= MIN_SAMPLE_SIZE) {
                initializeRegistry();
            } else {
                throw new IllegalStateException(
                        "Cannot finalize index: only " + initSampleBuffer.size() +
                                " samples collected (< MIN_SAMPLE_SIZE)"
                );
            }
        }

        // HARD registry consistency check
        Map<String, Object> stats = GFunctionRegistry.getStats();
        SystemConfig.PaperConfig pc = cfg.getPaper();
        if ((int) stats.get("m") != pc.getM()
                || (int) stats.get("lambda") != pc.getLambda()
                || (int) stats.get("tables") != pc.getTables()
                || (int) stats.get("divisions") != pc.getDivisions()) {
            throw new IllegalStateException(
                    "GFunctionRegistry mismatch at finalize: " + stats
            );
        }

        // Flush pending plaintext vectors (NO recursive insert)
        if (!pendingVectors.isEmpty()) {
            logger.info("Flushing {} pending vectors after registry init",
                    pendingVectors.size());

            for (PendingVector pv : pendingVectors) {
                EncryptedPoint ep =
                        cryptoService.encrypt(pv.id, pv.vector, keyService.getCurrentVersion());
                directInsert(ep, pv.vector);
            }
            pendingVectors.clear();
        }

        // Build all tables
        for (DimensionState[] arr : dims.values()) {
            for (DimensionState S : arr) {
                synchronized (S) {
                    if (!S.staged.isEmpty()) build(S);
                }
            }
        }

        frozen = true;

        logger.info("Index finalization complete");
    }

    public boolean isFrozen() {
        return frozen;
    }

    // =====================================================
    // REQUIRED IndexService METHODS
    // =====================================================

    @Override
    public EncryptedPointBuffer getPointBuffer() {
        return null;
    }

    public Set<String> getLastTouchedIds() {
        return Collections.unmodifiableSet(lastTouchedIds.get());
    }

    public int getLastTouchedCount() {
        return lastTouched.get();
    }

    public void setProbeOverride(int probes) {
        probeOverride.set(probes);
    }

    public void clearProbeOverride() {
        probeOverride.remove();
    }

    public int getLastRawCandidateCount() {
        return lastRawVisited;
    }


}