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

    /**
     * Candidate with pre-computed Hamming distance to query.
     * Used for pre-decrypt filtering.
     */
    public record CandidateWithScore(String id, long hammingDist)
            implements Comparable<CandidateWithScore> {

        @Override
        public int compareTo(CandidateWithScore other) {
            return Long.compare(this.hammingDist, other.hammingDist);
        }
    }

    // Greedy partition parameters (local defaults, no config dependency)
    private static final int DEFAULT_GREEDY_BLOCK_SIZE = 64;
    private static final int DEFAULT_MAX_PROBES = 5;


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
        List<GreedyPartitioner.Partition> partitions;
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
        int blockSize = DEFAULT_GREEDY_BLOCK_SIZE;

        if (!S.divisions.isEmpty()) {
            logger.warn("build() called on non-empty divisions - skipping");
            return;
        }

        // Log GFunction used for first build
        if (S.table == 0) {
            Coding.GFunction g0 = GFunctionRegistry.get(S.dim, 0, 0);
            logger.info(
                    "BUILD GFunction[table=0,div=0]: seed={}, m={}, lambda={}, omega[0]={}, omega[5]={}",
                    g0.seed, g0.m, g0.lambda,
                    g0.omega[0],
                    g0.omega.length > 5 ? g0.omega[5] : -1.0
            );

            StringBuilder omegaStr = new StringBuilder();
            for (int i = 0; i < Math.min(5, g0.omega.length); i++) {
                omegaStr.append(String.format("%.2f", g0.omega[i]));
                if (i < Math.min(4, g0.omega.length - 1)) omegaStr.append(", ");
            }
            logger.info("BUILD omega sample[table=0,div=0]: [{}...]", omegaStr.toString());

            if (!S.staged.isEmpty() && !S.stagedCodes.isEmpty()) {
                BitSet sampleCode = S.stagedCodes.get(0)[0];
                logger.info(
                        "BUILD BitSet sample[table=0,div=0]: length={}, cardinality={}, first10bits={}",
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
            div.partitions = GreedyPartitioner.build(idToCode, blockSize);
            S.divisions.add(div);
        }

        S.staged.clear();
        S.stagedCodes.clear();

        logger.debug(
                "Built MSANNP greedy partitions: dim={} table={} divisions={} blockSize={}",
                S.dim, S.table, S.divisions.size(), blockSize
        );
    }

    // ← ADD: Helper method
    private String getBitString(BitSet bs, int maxBits) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(63, bs.size()); i++) {
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
            throw new IllegalStateException("MSANNP violation: QueryToken missing BitSet codes");
        }
        if (qCodes.length != tables.length) {
            throw new IllegalStateException(
                    "Token tables mismatch: token=" + qCodes.length + " index=" + tables.length
            );
        }

        final int K = token.getTopK();
        final int requiredK = Math.max(K, cfg.getEval().getMaxK());
        final int HARD_CAP = Math.max(
                cfg.getRuntime().getMaxGlobalCandidates(),
                cfg.getRuntime().getRefinementLimit()
        );
        final int perDivisionMaxProbes = effectiveMaxProbes();

        // bestScore[id] = minimum distance-to-partition-range seen so far
        Map<String, Long> bestScore = new HashMap<>(Math.min(HARD_CAP, 1 << 16));
        Set<String> deleted = new HashSet<>(2048);
        int rawSeen = 0;

        for (int t = 0; t < tables.length && bestScore.size() < HARD_CAP; t++) {
            DimensionState S = tables[t];
            if (S.divisions == null || S.divisions.isEmpty()) continue;

            for (int d = 0; d < S.divisions.size() && bestScore.size() < HARD_CAP; d++) {
                if (d >= qCodes[t].length) {
                    throw new IllegalStateException(
                            "Token divisions mismatch at table=" + t +
                                    " expectedDivisions>=" + (d + 1)
                    );
                }

                DivisionState div = S.divisions.get(d);
                List<GreedyPartitioner.Partition> parts = div.partitions;
                if (parts == null || parts.isEmpty()) continue;

                long qKey = GreedyPartitioner.computeKey(qCodes[t][d]);
                int center = GreedyPartitioner.findNearestPartition(parts, qKey);

                // PriorityQueue entries: [partitionIndex, distanceToRange]
                PriorityQueue<long[]> probeQueue =
                        new PriorityQueue<>(Comparator.comparingLong(a -> a[1]));

                boolean[] visited = new boolean[parts.size()];

                BitSet qBits = qCodes[t][d];

                GreedyPartitioner.Partition centerPart = parts.get(center);
                long centerDist = GreedyPartitioner.hamming(qBits, centerPart.repCode);

                probeQueue.add(new long[]{center, centerDist});
                visited[center] = true;

                int probesUsed = 0;

                while (!probeQueue.isEmpty()
                        && probesUsed < perDivisionMaxProbes
                        && bestScore.size() < HARD_CAP) {

                    long[] cur = probeQueue.poll();
                    int idx = (int) cur[0];
                    probesUsed++;

                    rawSeen += collectPartitionOrdered(
                            parts.get(idx),
                            qBits,
                            bestScore,
                            deleted
                    );

                    int left = idx - 1;
                    if (left >= 0 && !visited[left]) {
                        visited[left] = true;
                        long dist = GreedyPartitioner.hamming(qBits, parts.get(left).repCode);
                        probeQueue.add(new long[]{left, dist});
                    }

                    int right = idx + 1;
                    if (right < parts.size() && !visited[right]) {
                        visited[right] = true;
                        long dist = GreedyPartitioner.hamming(qBits, parts.get(right).repCode);
                        probeQueue.add(new long[]{right, dist});
                    }
                }
            }
        }

        // ---- FINAL ORDERING (AFTER ALL TABLES & DIVISIONS) ----
        List<Map.Entry<String, Long>> ordered =
                new ArrayList<>(bestScore.entrySet());
        ordered.sort(Comparator.comparingLong(Map.Entry::getValue));

        List<String> out = new ArrayList<>(Math.min(ordered.size(), HARD_CAP));
        for (int i = 0; i < ordered.size() && out.size() < HARD_CAP; i++) {
            out.add(ordered.get(i).getKey());
        }

        lastTouched.set(out.size());
        lastTouchedIds.get().clear();
        lastTouchedIds.get().addAll(out);
        lastRawVisited = rawSeen;

        logger.info(
                "LSH Lookup: K={} | perDivProbes={} | rawSeen={} | uniqueCands={} | returned={} | deleted={}",
                K, perDivisionMaxProbes, rawSeen, bestScore.size(), out.size(), deleted.size()
        );

        if (out.size() < requiredK) {
            logger.warn("Recall floor violated: returned={} required={}", out.size(), requiredK);
        }

        return out;
    }

    /**
     * Lookup candidates WITH Hamming scores for pre-decrypt filtering.
     * This method returns the Hamming distances that are already computed
     * during candidate retrieval, enabling cheap pre-filtering before decryption.
     *
     * @param token Query token with bitsets
     * @return List of candidates with Hamming distances, sorted by distance
     */
    public List<CandidateWithScore> lookupCandidatesWithScores(QueryToken token) {
        Objects.requireNonNull(token, "token");
        if (!frozen) throw new IllegalStateException("Index not finalized");

        int dim = token.getDimension();
        DimensionState[] tables = dims.get(dim);
        if (tables == null) return List.of();

        BitSet[][] qCodes = token.getBitCodes();
        if (qCodes == null) {
            throw new IllegalStateException("MSANNP violation: QueryToken missing BitSet codes");
        }
        if (qCodes.length != tables.length) {
            throw new IllegalStateException(
                    "Token tables mismatch: token=" + qCodes.length + " index=" + tables.length
            );
        }

        final int K = token.getTopK();
        final int requiredK = Math.max(K, cfg.getEval().getMaxK());
        final int HARD_CAP = Math.max(
                cfg.getRuntime().getMaxGlobalCandidates(),
                cfg.getRuntime().getRefinementLimit()
        );
        final int perDivisionMaxProbes = effectiveMaxProbes();

        // bestScore[id] = minimum Hamming distance seen across all tables/divisions
        Map<String, Long> bestScore = new HashMap<>(Math.min(HARD_CAP, 1 << 16));
        Set<String> deleted = new HashSet<>(2048);
        int rawSeen = 0;

        // Same candidate retrieval logic as lookupCandidateIds()
        for (int t = 0; t < tables.length && bestScore.size() < HARD_CAP; t++) {
            DimensionState S = tables[t];
            if (S.divisions == null || S.divisions.isEmpty()) continue;

            for (int d = 0; d < S.divisions.size() && bestScore.size() < HARD_CAP; d++) {
                if (d >= qCodes[t].length) {
                    throw new IllegalStateException(
                            "Token divisions mismatch at table=" + t +
                                    " expectedDivisions>=" + (d + 1)
                    );
                }

                DivisionState div = S.divisions.get(d);
                List<GreedyPartitioner.Partition> parts = div.partitions;
                if (parts == null || parts.isEmpty()) continue;

                long qKey = GreedyPartitioner.computeKey(qCodes[t][d]);
                int center = GreedyPartitioner.findNearestPartition(parts, qKey);

                PriorityQueue<long[]> probeQueue =
                        new PriorityQueue<>(Comparator.comparingLong(a -> a[1]));

                boolean[] visited = new boolean[parts.size()];
                BitSet qBits = qCodes[t][d];

                GreedyPartitioner.Partition centerPart = parts.get(center);
                long centerDist = GreedyPartitioner.hamming(qBits, centerPart.repCode);

                probeQueue.add(new long[]{center, centerDist});
                visited[center] = true;

                int probesUsed = 0;

                while (!probeQueue.isEmpty()
                        && probesUsed < perDivisionMaxProbes
                        && bestScore.size() < HARD_CAP) {

                    long[] cur = probeQueue.poll();
                    int idx = (int) cur[0];
                    probesUsed++;

                    rawSeen += collectPartitionOrdered(
                            parts.get(idx),
                            qBits,
                            bestScore,
                            deleted
                    );

                    int left = idx - 1;
                    if (left >= 0 && !visited[left]) {
                        visited[left] = true;
                        long dist = GreedyPartitioner.hamming(qBits, parts.get(left).repCode);
                        probeQueue.add(new long[]{left, dist});
                    }

                    int right = idx + 1;
                    if (right < parts.size() && !visited[right]) {
                        visited[right] = true;
                        long dist = GreedyPartitioner.hamming(qBits, parts.get(right).repCode);
                        probeQueue.add(new long[]{right, dist});
                    }
                }
            }
        }

        // ---- RETURN WITH SCORES (key change!) ----
        List<CandidateWithScore> result = new ArrayList<>(bestScore.size());
        for (Map.Entry<String, Long> entry : bestScore.entrySet()) {
            result.add(new CandidateWithScore(entry.getKey(), entry.getValue()));
        }

        // Sort by Hamming distance (ascending)
        result.sort(Comparator.comparingLong(CandidateWithScore::hammingDist));

        lastTouched.set(result.size());
        lastTouchedIds.get().clear();
        for (CandidateWithScore c : result) {
            lastTouchedIds.get().add(c.id());
        }
        lastRawVisited = rawSeen;

        logger.info(
                "LSH Lookup (with scores): K={} | perDivProbes={} | rawSeen={} | uniqueCands={} | returned={} | deleted={}",
                K, perDivisionMaxProbes, rawSeen, bestScore.size(), result.size(), deleted.size()
        );

        if (result.size() < requiredK) {
            logger.warn("Recall floor violated: returned={} required={}", result.size(), requiredK);
        }

        return result;
    }

    public EncryptedPoint loadPointIfActive(String id) {
        if (metadata.isDeleted(id)) return null;
        try {
            return metadata.loadEncryptedPoint(id);
        } catch (Exception e) {
            return null;
        }
    }

    private int collectPartitionOrdered(
            GreedyPartitioner.Partition p,
            BitSet qBits,
            Map<String, Long> bestScore,
            Set<String> deleted
    ) {
        int newlySeen = 0;

        // Partition score = Hamming distance to representative
        long partDist = GreedyPartitioner.hamming(qBits, p.repCode);

        for (String id : p.ids) {
            if (deleted.contains(id)) continue;

            if (metadata.isDeleted(id)) {
                deleted.add(id);
                continue;
            }

            Long prev = bestScore.get(id);
            if (prev == null || partDist < prev) {
                bestScore.put(id, partDist);
                newlySeen++;
            }
        }
        return newlySeen;
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

    private int effectiveMaxProbes() {
        Integer o = probeOverride.get();
        if (o != null && o > 0) return o;

        int cfgOverride = cfg.getRuntime().getProbeOverride();
        if (cfgOverride > 0) return cfgOverride;

        return DEFAULT_MAX_PROBES;
    }

    /**
     * Default per-division probe budget (paper baseline).
     * Does NOT include runtime overrides.
     */
    public int getDefaultMaxProbes() {
        return DEFAULT_MAX_PROBES;
    }



}