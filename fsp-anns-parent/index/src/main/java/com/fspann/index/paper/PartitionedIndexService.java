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
    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "token cannot be null");
        Set<String> deletedCache = new HashSet<>();

        if (!frozen) throw new IllegalStateException("Index not finalized before lookup");

        if (cfg.getSearchMode() != com.fspann.config.SearchMode.PAPER_BASELINE) {
            throw new IllegalStateException("lookup() is restricted to PAPER_BASELINE. Use lookupCandidateIds() for real runs.");
        }

        SystemConfig.PaperConfig pc = cfg.getPaper();

        int runtimeCap = cfg.getRuntime().getMaxCandidateFactor() * token.getTopK();
        int paperCap = cfg.getPaper().getSafetyMaxCandidates();
        final int HARD_CAP = (paperCap > 0) ? Math.min(runtimeCap, paperCap) : runtimeCap;

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

        for (int relax = 0; relax <= Math.min(pc.lambda, maxRelax); relax++) {
            int earlyStop = cfg.getRuntime().getEarlyStopCandidates();
            if (earlyStop > 0 && out.size() >= earlyStop) break;

            int relaxedBits = perDivBits - (relax * pc.m);
            if (relaxedBits <= 0) continue;

            for (int t = 0; t < L; t++) {
                DimensionState S = tables[t];
                if (S == null || S.divisions.isEmpty()) continue;

                BitSet[] qcodes = codesByTable[t];
                if (qcodes == null || qcodes.length == 0) continue;

                int safeDivs = Math.min(S.divisions.size(), qcodes.length);

                for (int d = 0; d < safeDivs; d++) {
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
                                lastTouched.set(touchedIds.size());
                                return new ArrayList<>(out.values());
                            }
                        }
                    }
                }
            }
        }

        lastTouched.set(touchedIds.size());
        return new ArrayList<>(out.values());
    }

    // =====================================================
    // REAL RUN path: candidate IDs only (unions across tables)
    // =====================================================
    public List<String> lookupCandidateIds(QueryToken token) {
        Objects.requireNonNull(token, "token");
        if (!frozen) throw new IllegalStateException("Index not finalized");

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int K = token.getTopK();

        int runtimeCap = cfg.getRuntime().getMaxCandidateFactor() * K;
        int paperCap = cfg.getPaper().getSafetyMaxCandidates();
        final int MAX_IDS = (paperCap > 0) ? Math.min(runtimeCap, paperCap) : runtimeCap;

        int dim = token.getDimension();
        DimensionState[] tables = dims.get(dim);
        if (tables == null) {
            lastTouched.set(0);
            lastTouchedIds.get().clear();
            return List.of();
        }

        BitSet[][] codesByTable = token.getCodesByTable();
        if (codesByTable == null || codesByTable.length == 0) {
            lastTouched.set(0);
            lastTouchedIds.get().clear();
            return List.of();
        }

        Set<String> seen = new LinkedHashSet<>(MAX_IDS);
        Set<String> deletedCache = new HashSet<>();
        int touched = 0;

        final int perDivBits = perDivisionBits();
        int maxRelax = cfg.getRuntime().getMaxRelaxationDepth();
        int L = Math.min(tables.length, codesByTable.length);

        for (int relax = 0; relax <= Math.min(pc.lambda, maxRelax); relax++) {
            int bits = perDivBits - relax * pc.m;
            if (bits <= 0) break;

            for (int t = 0; t < L; t++) {
                DimensionState S = tables[t];
                if (S == null || S.divisions.isEmpty()) continue;

                BitSet[] qcodes = codesByTable[t];
                if (qcodes == null || qcodes.length == 0) continue;

                int safeDivs = Math.min(S.divisions.size(), qcodes.length);

                for (int d = 0; d < safeDivs; d++) {
                    DivisionState div = S.divisions.get(d);
                    BitSet qc = qcodes[d];

                    for (GreedyPartitioner.SubsetBounds sb : div.I) {
                        if (!covers(sb, qc, bits)) continue;

                        List<String> ids = div.tagToIds.get(sb.tag);
                        if (ids == null) continue;

                        for (String id : ids) {
                            if (deletedCache.contains(id)) continue;
                            if (metadata.isDeleted(id)) {
                                deletedCache.add(id);
                                continue;
                            }

                            if (seen.add(id)) {
                                touched++;
                                if (seen.size() >= MAX_IDS) {
                                    lastTouched.set(touched);
                                    lastTouchedIds.get().clear();
                                    lastTouchedIds.get().addAll(seen);
                                    return new ArrayList<>(seen);
                                }
                            }
                        }
                    }
                }
            }
        }

        lastTouched.set(touched);
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
