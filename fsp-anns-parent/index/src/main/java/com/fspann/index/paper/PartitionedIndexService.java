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
import java.util.concurrent.atomic.AtomicBoolean;

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
        List<GreedyPartitioner.SubsetBounds> I = List.of();
        Map<String, List<String>> tagToIds = new HashMap<>();
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
                pc.m, pc.lambda, pc.getTables(), pc.divisions, pc.seed
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

        logger.info("=".repeat(60));
        logger.info(">>> INITIALIZING GFunctionRegistry <<<");
        logger.info("=".repeat(60));
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
                dimension, pc.m, pc.lambda, pc.getTables(), pc.divisions, initSampleBuffer.size()
        );

        try {
            GFunctionRegistry.initialize(
                    initSampleBuffer,
                    dimension,
                    pc.m,
                    pc.lambda,
                    pc.seed,
                    pc.getTables(),
                    pc.divisions
            );

            logger.info("=".repeat(60));
            logger.info(">>> GFunctionRegistry INITIALIZED SUCCESSFULLY <<<");
            logger.info("=".repeat(60));
            logger.info("Stats: {}", GFunctionRegistry.getStats());

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

        // Persist once (global metadata), not per-table
        try {
            metadata.saveEncryptedPoint(pt);
        } catch (IOException e) {
            logger.error("Failed to persist encrypted point {}: {}", pt.getId(), e.getMessage());
            throw new RuntimeException("Persistence failed for point " + pt.getId(), e);
        }

        DimensionState[] tables = dims.computeIfAbsent(dim, d -> newTableStates(d));

        // Stage into ALL tables (ℓ)
        for (DimensionState S : tables) {
            BitSet[] codes = codeForTable(vec, S.table);
            synchronized (S) {
                S.staged.add(pt);
                S.stagedCodes.add(codes);

                if (S.staged.size() >= DEFAULT_BUILD_THRESHOLD) {
                    build(S);
                    try {
                        storageMetrics.updateDimensionStorage(dim);
                    } catch (Exception ignore) {}
                }
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
        return pc.seed + (table * 1_000_003L);
    }

    /**
     * Returns the number of tables (L) configured for the index.
     */
    public int numTables() {
        return cfg.getPaper().getTables();
    }

    // =====================================================
    // CODING (using GFunctionRegistry)
    // =====================================================

    /**
     * Compute codes for a vector for a specific table.
     * Uses GFunctionRegistry for data-adaptive GFunctions.
     */
    private BitSet[] codeForTable(double[] vec, int table) {
        ensureRegistryInitialized();
        return GFunctionRegistry.codeForTable(vec, table);
    }

    /**
     * Legacy method: code() for backward compatibility.
     * Now delegates to GFunctionRegistry.
     */
    public BitSet[] code(double[] vec) {
        ensureRegistryInitialized();
        // Return codes for table 0 (legacy single-table behavior)
        return GFunctionRegistry.codeForTable(vec, 0);
    }

    /**
     * Seed override for table-independence.
     * Used by QueryTokenFactory.
     *
     * NOTE: The seed is now only used to identify which table's GFunction to use.
     * The actual GFunction parameters (alpha, r, omega) come from the registry.
     */
    public BitSet[] code(double[] vec, long seedOverride) {
        ensureRegistryInitialized();

        // Compute which table this seed corresponds to
        SystemConfig.PaperConfig pc = cfg.getPaper();
        long baseSeed = pc.seed;

        // Reverse the seed computation: seed = baseSeed + (table * 1_000_003L)
        int table = (int) ((seedOverride - baseSeed) / 1_000_003L);

        if (table < 0 || table >= pc.getTables()) {
            logger.warn("Seed {} doesn't match any table (baseSeed={}, tables={}). Using table 0.",
                    seedOverride, baseSeed, pc.getTables());
            table = 0;
        }

        return GFunctionRegistry.codeForTable(vec, table);
    }

    // =====================================================
    // BUILD (Algorithm-2) per table
    // =====================================================

    private void build(DimensionState S) {
        // Ensure registry is initialized before building
        ensureRegistryInitialized();

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int divisions = pc.divisions;

        S.divisions.clear();

        for (int d = 0; d < divisions; d++) {
            List<GreedyPartitioner.Item> items = new ArrayList<>(S.staged.size());
            for (int i = 0; i < S.staged.size(); i++) {
                items.add(new GreedyPartitioner.Item(
                        S.staged.get(i).getId(),
                        S.stagedCodes.get(i)[d]
                ));
            }

            int codeBits = pc.m * pc.lambda;

            long seedTD = tableSeed(S.table) + d;
            var br = GreedyPartitioner.build(items, codeBits, seedTD);

            DivisionState div = new DivisionState();
            div.I = br.indexI;
            div.tagToIds = br.tagToIds;
            S.divisions.add(div);
        }

        S.staged.clear();
        S.stagedCodes.clear();
        logger.debug("Built partitions: dim={} table={} divisions={}", S.dim, S.table, S.divisions.size());
    }

    // =====================================================
    // LOOKUP (paper baseline) - unions across tables
    // =====================================================

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "token cannot be null");
        Set<String> deletedCache = new HashSet<>(1024);

        if (!frozen) throw new IllegalStateException("Index not finalized before lookup");

        if (!cfg.isPaperMode()) {
            throw new IllegalStateException(
                    "lookup() is PAPER BASELINE ONLY. Use lookupCandidateIds() for production runs."
            );
        }

        if (cfg.getSearchMode() != com.fspann.config.SearchMode.PAPER_BASELINE) {
            throw new IllegalStateException("lookup() is restricted to PAPER_BASELINE. Use lookupCandidateIds() for real runs.");
        }

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int K = token.getTopK();

        int runtimeCap = cfg.getRuntime().getMaxCandidateFactor() * K;
        int paperCap = cfg.getPaper().getSafetyMaxCandidates();
        final int HARD_CAP = (paperCap > 0) ? Math.min(runtimeCap, paperCap) : runtimeCap;

        logger.info("lookup BASELINE START: K={}, HARD_CAP={}", K, HARD_CAP);

        Set<String> touchedIds = lastTouchedIds.get();
        touchedIds.clear();

        int dim = token.getDimension();
        DimensionState[] tables = dims.get(dim);
        if (tables == null) {
            lastTouched.set(0);
            return List.of();
        }

        BitSet[][] codesByTable = token.getCodesByTable();
        if (codesByTable == null || codesByTable.length == 0) {
            lastTouched.set(0);
            return List.of();
        }

        LinkedHashMap<String, EncryptedPoint> out = new LinkedHashMap<>(HARD_CAP);
        final int perDivBits = perDivisionBits();

        int maxRelax = cfg.getRuntime().getMaxRelaxationDepth();
        int L = Math.min(tables.length, codesByTable.length);

        boolean hitLimit = false;

        for (int relax = 0; relax <= Math.min(pc.lambda, maxRelax) && !hitLimit; relax++) {
            int earlyStop = cfg.getRuntime().getEarlyStopCandidates();
            if (earlyStop > 0 && out.size() >= earlyStop) break;

            int relaxedBits = perDivBits - (relax * pc.m);
            if (relaxedBits <= 0) continue;

            for (int t = 0; t < L && !hitLimit; t++) {
                if (out.size() >= HARD_CAP) {
                    logger.info("HARD_CAP reached before table {}: size={}", t, out.size());
                    hitLimit = true;
                    break;
                }

                DimensionState S = tables[t];
                if (S == null || S.divisions.isEmpty()) continue;

                BitSet[] qcodes = codesByTable[t];
                if (qcodes == null || qcodes.length == 0) continue;

                int safeDivs = Math.min(S.divisions.size(), qcodes.length);

                for (int offset = 0; offset < safeDivs; offset++) {
                    int d = (offset + relax) % safeDivs;
                    DivisionState div = S.divisions.get(d);
                    BitSet qc = qcodes[d];

                    for (GreedyPartitioner.SubsetBounds sb : div.I) {
                        boolean match = (relax == 0)
                                ? covers(sb, qc, relaxedBits)
                                : coversRelaxed(sb, qc, relaxedBits);

                        if (!match) continue;

                        List<String> ids = div.tagToIds.get(sb.tag);
                        if (ids == null) continue;

                        for (String id : ids) {
                            touchedIds.add(id);

                            if (out.containsKey(id)) continue;
                            if (deletedCache.contains(id)) continue;
                            if (metadata.isDeleted(id)) {
                                deletedCache.add(id);
                                continue;
                            }

                            try {
                                EncryptedPoint ep = metadata.loadEncryptedPoint(id);
                                if (ep != null) out.put(id, ep);
                            } catch (Exception ignore) {}

                            if (out.size() >= HARD_CAP) {
                                logger.info("HARD_CAP reached: size={}, touched={}",
                                        out.size(), touchedIds.size());
                                hitLimit = true;
                                break;
                            }
                        }

                        if (hitLimit) break;
                    }

                    if (hitLimit) break;
                }

                if (hitLimit) break;
            }

            if (hitLimit) break;
        }

        logger.info("lookup BASELINE END: returned={}, touched={}", out.size(), touchedIds.size());

        lastTouched.set(touchedIds.size());
        return new ArrayList<>(out.values());
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
     * 4. Stops when MAX_IDS limit is reached
     */
    public List<String> lookupCandidateIds(QueryToken token) {
        Objects.requireNonNull(token, "token");
        if (!frozen) throw new IllegalStateException("Index not finalized");

        BitSet[][] codesByTable = token.getCodesByTable();
        if (codesByTable == null || codesByTable.length == 0)
            throw new IllegalStateException("Token has no codes");

        int dim = token.getDimension();
        DimensionState[] tables = dims.get(dim);
        if (tables == null) return List.of();

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int K = token.getTopK();

        boolean precisionMode = cfg.getRuntime().isPrecisionMode();

        final int MIN_IDS = precisionMode
                ? cfg.getRuntime().getMinPrecisionCandidates()
                : cfg.getRuntime().getMaxCandidateFactor() * K;

        final int MAX_IDS = precisionMode
                ? cfg.getRuntime().getMaxPrecisionCandidates()
                : cfg.getRuntime().getMaxCandidateFactor() * K;

        final int perDivBits = perDivisionBits();
        final int maxRelax = precisionMode
                ? cfg.getRuntime().getMaxRelaxationDepth()
                : Math.min(cfg.getRuntime().getMaxRelaxationDepth(), pc.lambda);

        final int L = Math.min(tables.length, codesByTable.length);

        Map<String, Integer> score = new HashMap<>(MAX_IDS);
        Set<String> deletedCache = new HashSet<>(1024);

        for (int relax = 0; relax <= maxRelax; relax++) {
            int bits = perDivBits - relax * pc.m;
            if (bits <= 0) break;

            for (int offsetT = 0; offsetT < L; offsetT++) {
                int t = (offsetT + relax) % L;
                DimensionState S = tables[t];
                if (S == null || S.divisions.isEmpty()) continue;

                BitSet[] qcodes = codesByTable[t];
                if (qcodes == null) continue;

                int safeDivs = Math.min(S.divisions.size(), qcodes.length);

                for (int offsetD = 0; offsetD < safeDivs; offsetD++) {
                    int d = (offsetD + relax) % safeDivs;
                    DivisionState div = S.divisions.get(d);
                    BitSet qc = qcodes[d];

                    for (GreedyPartitioner.SubsetBounds sb : div.I) {
                        boolean match = (relax == 0)
                                ? covers(sb, qc, bits)
                                : coversRelaxed(sb, qc, bits);

                        if (!match) continue;

                        List<String> ids = div.tagToIds.get(sb.tag);
                        if (ids == null) continue;

                        for (String id : ids) {
                            if (deletedCache.contains(id)) continue;
                            if (metadata.isDeleted(id)) {
                                if (deletedCache.size() < 100_000) {
                                    deletedCache.add(id);
                                }
                                continue;
                            }

                            int s = (relax << 24) | (t << 16) | (d << 8);
                            score.merge(id, s, Math::min);

                            if (score.size() >= MAX_IDS) break;
                        }
                        if (score.size() >= MAX_IDS) break;
                    }
                    if (score.size() >= MAX_IDS) break;
                }
                if (score.size() >= MAX_IDS) break;
            }

            if (precisionMode && relax > 0 && score.size() >= MIN_IDS) {
                break;
            }

        }

        List<Map.Entry<String, Integer>> ordered =
                new ArrayList<>(score.entrySet());

        ordered.sort(Comparator.comparingInt(Map.Entry::getValue));

        List<String> out = new ArrayList<>(Math.min(MAX_IDS, ordered.size()));
        for (Map.Entry<String, Integer> e : ordered) {
            out.add(e.getKey());
            if (out.size() >= MAX_IDS) break;
        }

        lastTouched.set(out.size());
        lastTouchedIds.get().clear();
        lastTouchedIds.get().addAll(out);

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

    private boolean covers(GreedyPartitioner.SubsetBounds sb, BitSet c, int bits) {
        var cmp = new GreedyPartitioner.CodeComparator(bits);
        return cmp.compare(sb.lower, c) <= 0 && cmp.compare(c, sb.upper) <= 0;
    }

    private boolean coversRelaxed(
            GreedyPartitioner.SubsetBounds sb,
            BitSet q,
            int bits
    ) {
        int violations = 0;

        for (int i = 0; i < bits; i++) {
            boolean qi = q.get(i);
            boolean lo = sb.lower.get(i);
            boolean hi = sb.upper.get(i);

            if ((lo && !qi) || (!hi && qi)) {
                violations++;
                // Allow limited violations
                if (violations > 1) return false;
            }
        }
        return true;
    }

    private int perDivisionBits() {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return pc.m * pc.lambda;
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

        for (DimensionState S : tables) {
            BitSet[] codes = codeForTable(vec, S.table);
            S.staged.add(pt);
            S.stagedCodes.add(codes);
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
        if ((int) stats.get("m") != pc.m
                || (int) stats.get("lambda") != pc.lambda
                || (int) stats.get("tables") != pc.getTables()
                || (int) stats.get("divisions") != pc.divisions) {
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

    private void maybeCollectSample(double[] vec) {
        if (GFunctionRegistry.isInitialized()) return;

        synchronized (initSampleBuffer) {
            if (GFunctionRegistry.isInitialized()) return;

            if (initSampleBuffer.size() < MAX_SAMPLE_SIZE) {
                initSampleBuffer.add(vec.clone());
            }

            if (initSampleBuffer.size() >= MIN_SAMPLE_SIZE) {
                initializeRegistry();
            }
        }
    }


}