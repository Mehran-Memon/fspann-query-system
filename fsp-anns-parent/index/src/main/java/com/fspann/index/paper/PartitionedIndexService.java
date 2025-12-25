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

public final class PartitionedIndexService implements IndexService {

    private static final Logger logger =
            LoggerFactory.getLogger(PartitionedIndexService.class);

    private static final int DEFAULT_BUILD_THRESHOLD =
            Math.max(20_000, Runtime.getRuntime().availableProcessors() * 20_000);

    private final RocksDBMetadataManager metadata;
    private final SystemConfig cfg;
    private final StorageMetrics storageMetrics;
    private final KeyRotationServiceImpl keyService;
    private final AesGcmCryptoService cryptoService;

    // dim -> per-table states (tables = ℓ)
    private final Map<Integer, DimensionState[]> dims = new ConcurrentHashMap<>();

    private volatile boolean frozen = false;

    private final ThreadLocal<Integer> lastTouched =
            ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Set<String>> lastTouchedIds =
            ThreadLocal.withInitial(() -> new HashSet<>(2048));
    private final ThreadLocal<Integer> probeOverride =
            ThreadLocal.withInitial(() -> -1);

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

        logger.info("PartitionedIndexService initialized | buildThreshold={}", DEFAULT_BUILD_THRESHOLD);
    }

    // =====================================================
    // INSERT
    // =====================================================

    @Override
    public void insert(String id, double[] vector) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(vector, "vector cannot be null");

        int dim = vector.length;

        EncryptedPoint ep;
        try {
            ep = cryptoService.encrypt(id, vector, keyService.getCurrentVersion());
            if (ep == null) throw new RuntimeException("encrypt() returned null");

            // DIAGNOSTIC: Verify tracking happened
            if (keyService instanceof com.fspann.key.KeyRotationServiceImpl kr) {
                int tracked = kr.getKeyManager().getUsageTracker().getVectorCount(ep.getKeyVersion());
                logger.debug("After encrypt: id={}, keyVersion={}, trackedCount={}",
                        id, ep.getKeyVersion(), tracked);
            }
        } catch (Exception e) {
            logger.error("Failed to encrypt vector {}: {}", id, e.getMessage());
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
            BitSet[] codes = code(vec, tableSeed(S.table));
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
        int L = cfg.getPaper().getTables(); // get the number of tables
        DimensionState[] arr = new DimensionState[L];
        for (int t = 0; t < L; t++) arr[t] = new DimensionState(dim, t);
        return arr;
    }

    private long tableSeed(int table) {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return pc.seed + (table * 1_000_003L);
    }

    // =====================================================
    // BUILD (Algorithm-2) per table
    // =====================================================

    private void build(DimensionState S) {
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

/**
 *
 * PASTE THIS to replace lines 220-310 in:
 * query/src/main/java/com/fspann/query/service/PartitionedIndexService.java
 *
 * Same fixes as lookupCandidateIds but for PAPER_BASELINE mode
 */
    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "token cannot be null");
        Set<String> deletedCache = new HashSet<>();

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

        // CRITICAL FIX: Track if we hit limit
        boolean hitLimit = false;

        for (int relax = 0; relax <= Math.min(pc.lambda, maxRelax) && !hitLimit; relax++) {
            int earlyStop = cfg.getRuntime().getEarlyStopCandidates();
            if (earlyStop > 0 && out.size() >= earlyStop) break;

            int relaxedBits = perDivBits - (relax * pc.m);
            if (relaxedBits <= 0) continue;

            for (int t = 0; t < L && !hitLimit; t++) {
                // Check limit before each table
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

                for (int d = 0; d < safeDivs && !hitLimit; d++) {
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
     *
     * PASTE THIS to replace lines 333-450 in:
     * query/src/main/java/com/fspann/query/service/PartitionedIndexService.java
     *
     * Key fixes:
     * 1. Breaks ALL nested loops when MAX_IDS hit (not just innermost)
     * 2. Checks limit BEFORE processing each table/division/subset
     * 3. INFO-level logging (not debug) to see what's happening
     * 4. Clear diagnostics showing where limit is hit
     */
    public List<String> lookupCandidateIds(QueryToken token) {
        Objects.requireNonNull(token, "token");
        if (!frozen) throw new IllegalStateException("Index not finalized");

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int K = token.getTopK();

        // Calculate hard limit
        int runtimeCap = cfg.getRuntime().getMaxCandidateFactor() * K;
        int paperCap = cfg.getPaper().getSafetyMaxCandidates();
        final int MAX_IDS = (paperCap > 0) ? Math.min(runtimeCap, paperCap) : runtimeCap;

        // DIAGNOSTIC: Always log (INFO level so we can see it!)
        logger.info("lookupCandidateIds START: K={}, maxCandFactor={}, MAX_IDS={}, lambda={}",
                K, cfg.getRuntime().getMaxCandidateFactor(), MAX_IDS, pc.lambda);

        int dim = token.getDimension();
        DimensionState[] tables = dims.get(dim);
        if (tables == null) {
            lastTouched.set(0);
            lastTouchedIds.get().clear();
            logger.info("lookupCandidateIds: No tables for dim={}, returning empty", dim);
            return List.of();
        }

        BitSet[][] codesByTable = token.getCodesByTable();
        if (codesByTable == null || codesByTable.length == 0) {
            lastTouched.set(0);
            lastTouchedIds.get().clear();
            logger.info("lookupCandidateIds: No codes, returning empty");
            return List.of();
        }

        Set<String> seen = new LinkedHashSet<>(MAX_IDS);
        Set<String> deletedCache = new HashSet<>();
        int visitedCount = 0;   // every ID examined
        int uniqueAdded = 0;    // unique IDs kept

        final int perDivBits = perDivisionBits();
        int maxRelax = cfg.getRuntime().getMaxRelaxationDepth();
        int L = Math.min(tables.length, codesByTable.length);

        // Track if we hit limit to break ALL loops
        boolean hitLimit = false;

        for (int relax = 0; relax <= Math.min(pc.lambda, maxRelax) && !hitLimit; relax++) {
            int bits = perDivBits - relax * pc.m;
            if (bits <= 0) break;

            logger.debug("Relaxation round {}: bits={}, currentCandidates={}", relax, bits, seen.size());

            for (int t = 0; t < L && !hitLimit; t++) {
                // CRITICAL: Check limit BEFORE processing each table
                if (seen.size() >= MAX_IDS) {
                    logger.info("Candidate limit reached BEFORE table {}/{}: seen={}, MAX_IDS={}",
                            t, L, seen.size(), MAX_IDS);
                    hitLimit = true;
                    break;
                }

                DimensionState S = tables[t];
                if (S == null || S.divisions.isEmpty()) continue;

                BitSet[] qcodes = codesByTable[t];
                if (qcodes == null || qcodes.length == 0) continue;

                int safeDivs = Math.min(S.divisions.size(), qcodes.length);

                for (int d = 0; d < safeDivs && !hitLimit; d++) {
                    DivisionState div = S.divisions.get(d);
                    BitSet qc = qcodes[d];

                    for (GreedyPartitioner.SubsetBounds sb : div.I) {
                        if (!covers(sb, qc, bits)) continue;

                        List<String> ids = div.tagToIds.get(sb.tag);
                        if (ids == null) continue;

                        // Check limit BEFORE adding IDs from this subset
                        if (seen.size() >= MAX_IDS) {
                            logger.info("Hit MAX_IDS mid-division: table={}, div={}, subset={}",
                                    t, d, sb.tag);
                            hitLimit = true;
                            break;
                        }

                        for (String id : ids) {
                            // Skip deleted
                            if (deletedCache.contains(id)) continue;
                            if (metadata.isDeleted(id)) {
                                deletedCache.add(id);
                                continue;
                            }

                            // Add if new
                            visitedCount++;

                            if (seen.add(id)) {
                                uniqueAdded++;

                                if (seen.size() >= MAX_IDS) {
                                    logger.info(
                                            "Reached MAX_IDS: seen={}, visited={}",
                                            seen.size(), visitedCount
                                    );
                                    hitLimit = true;
                                    break;
                                }
                            }
                        }

                            if (hitLimit) break; // Exit subset loop
                        }

                        if (hitLimit) break; // Exit division loop
                    }

                    if (hitLimit) break; // Exit table loop
                }

                if (hitLimit) break; // Exit relaxation loop

                logger.debug("After relaxation {}: candidates={}/{}", relax, seen.size(), MAX_IDS);
            }

            // Final logging
        double ratio = (K > 0) ? ((double) seen.size() / K) : 0.0;

        logger.info(
                "lookupCandidateIds END: K={}, candidates={}, visited={}, unique={}, ratio={}",
                K, seen.size(), visitedCount, uniqueAdded,
                String.format("%.3f", ratio)
        );

            lastTouched.set(visitedCount);
            lastTouchedIds.get().clear();
            lastTouchedIds.get().addAll(seen);

            return new ArrayList<>(seen);
        }



    public EncryptedPoint loadPointIfActive(String id) {
        if (metadata.isDeleted(id)) return null;
        try { return metadata.loadEncryptedPoint(id); }
        catch (Exception e) { return null; }
    }

    private boolean covers(GreedyPartitioner.SubsetBounds sb, BitSet c, int bits) {
        var cmp = new GreedyPartitioner.CodeComparator(bits);
        return cmp.compare(sb.lower, c) <= 0 && cmp.compare(c, sb.upper) <= 0;
    }

    private int perDivisionBits() {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return pc.m * pc.lambda;
    }

    // =====================================================
    // FINALIZATION
    // =====================================================
    public void finalizeForSearch() {
        if (frozen) {
            logger.info("Index already finalized");
            return;
        }

        logger.info("Finalizing index for search...");

        for (DimensionState[] arr : dims.values()) {
            for (DimensionState S : arr) {
                synchronized (S) {
                    if (!S.staged.isEmpty()) build(S);
                }
            }
        }

        frozen = true;

        for (Integer dim : dims.keySet()) {
            try {
                storageMetrics.updateDimensionStorage(dim);
                StorageMetrics.StorageSnapshot snap = storageMetrics.getSnapshot();
                logger.info("Finalized dim {}: {}", dim, snap.summary());
            } catch (Exception e) {
                logger.warn("Failed to update storage metrics for dimension {}", dim, e);
            }
        }

        logger.info("Index finalization complete");
    }

    public boolean isFrozen() { return frozen; }

    // =====================================================
    // CODING (Algorithm-1)
    // =====================================================
    public BitSet[] code(double[] vec) {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return Coding.code(vec, pc.divisions, pc.m, pc.lambda, pc.seed);
    }

    /** NEW: seed override for table-independence (used by QueryTokenFactory + insert staging) */
    public BitSet[] code(double[] vec, long seedOverride) {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return Coding.code(vec, pc.divisions, pc.m, pc.lambda, seedOverride);
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

    private boolean coversRelaxed(GreedyPartitioner.SubsetBounds sb, BitSet c, int bits) {
        var cmp = new GreedyPartitioner.CodeComparator(bits);
        return cmp.compare(sb.lower, c) <= 0 && cmp.compare(c, sb.upper) <= 0;
    }
}
