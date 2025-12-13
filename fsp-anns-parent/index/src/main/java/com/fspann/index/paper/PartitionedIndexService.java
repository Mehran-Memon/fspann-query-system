package com.fspann.index.paper;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PartitionedIndexService (mSANNP)
 *
 * - ID-only partitions
 * - No LSH
 * - No intersection
 * - Stable arrival order
 * - Compatible with D1 limiter
 */
public final class PartitionedIndexService implements IndexService {

    private static final Logger log =
            LoggerFactory.getLogger(PartitionedIndexService.class);

    private final RocksDBMetadataManager metadata;
    private final SystemConfig cfg;

    // dim -> state
    private final Map<Integer, DimensionState> dims = new ConcurrentHashMap<>();

    private static final class DimensionState {
        final int dim;
        final List<DivisionState> divisions = new ArrayList<>();
        final List<EncryptedPoint> staged = new ArrayList<>();
        final List<BitSet[]> stagedCodes = new ArrayList<>();

        DimensionState(int dim) {
            this.dim = dim;
        }
    }

    private static final class DivisionState {
        List<GreedyPartitioner.SubsetBounds> I = List.of();
        Map<String, List<String>> tagToIds = new HashMap<>();
    }

    public PartitionedIndexService(
            RocksDBMetadataManager metadata,
            SystemConfig cfg) {

        this.metadata = Objects.requireNonNull(metadata);
        this.cfg = Objects.requireNonNull(cfg);
    }

    // ======================================================
    // INSERT
    // ======================================================
    @Override
    public void insert(String id, double[] vector) {
        EncryptedPoint ep = metadata.encrypt(id, vector); // or injected CryptoService
        insert(ep, vector);
    }

    public void insert(EncryptedPoint pt, double[] vec) {
        int dim = vec.length;
        DimensionState S = dims.computeIfAbsent(dim, DimensionState::new);

        BitSet[] codes = code(vec);

        synchronized (S) {
            S.staged.add(pt);
            S.stagedCodes.add(codes);

            if (S.staged.size() >= cfg.getPaper().buildThreshold) {
                build(S);
            }
        }
    }

    @Override
    public void insert(EncryptedPoint pt) {
        throw new UnsupportedOperationException();
    }

    // ======================================================
    // BUILD
    // ======================================================
    private void build(DimensionState S) {

        var pc = cfg.getPaper();
        int divisions = pc.divisions;
        int m = pc.m;

        S.divisions.clear();

        for (int d = 0; d < divisions; d++) {
            List<GreedyPartitioner.Item> items = new ArrayList<>();

            for (int i = 0; i < S.staged.size(); i++) {
                items.add(new GreedyPartitioner.Item(
                        S.staged.get(i).getId(),
                        S.stagedCodes.get(i)[d]
                ));
            }

            var br = GreedyPartitioner.build(
                    items,
                    m,
                    pc.seed + d
            );

            DivisionState div = new DivisionState();
            div.I = br.indexI;
            div.tagToIds = br.tagToIds;

            S.divisions.add(div);
        }

        S.staged.clear();
        S.stagedCodes.clear();
    }

    // ======================================================
    // LOOKUP (Algorithm-3 prefix order)
    // ======================================================
    @Override
    public List<String> lookup(QueryToken token) {
        int dim = token.getDimension();
        DimensionState S = dims.get(dim);
        if (S == null) return List.of();

        BitSet[] qcodes = token.getCodes();
        LinkedHashSet<String> out = new LinkedHashSet<>();

        for (int d = 0; d < S.divisions.size(); d++) {
            DivisionState div = S.divisions.get(d);
            BitSet qc = qcodes[d];

            for (GreedyPartitioner.SubsetBounds sb : div.I) {
                if (covers(sb, qc)) {
                    List<String> ids = div.tagToIds.get(sb.tag);
                    if (ids != null) out.addAll(ids);
                }
            }
        }
        return new ArrayList<>(out);
    }

    private boolean covers(GreedyPartitioner.SubsetBounds sb, BitSet c) {
        var cmp = new GreedyPartitioner.CodeComparator(sb.codeBits);
        return cmp.compare(sb.lower, c) <= 0 &&
                cmp.compare(c, sb.upper) <= 0;
    }

    // ======================================================
    // REQUIRED IndexService METHODS
    // ======================================================
    @Override
    public EncryptedPoint getEncryptedPoint(String id) {
        try {
            return metadata.loadEncryptedPoint(id);
        } catch (Exception e) {
            return null;
        }
    }

    @Override public void updateCachedPoint(EncryptedPoint pt) {}
    @Override public void delete(String id) {}
    @Override public void markDirty(int shardId) {}
    @Override public int getShardIdForVector(double[] v) { return -1; }
    @Override public EncryptedPointBuffer getPointBuffer() { return null; }

    @Override
    public int getIndexedVectorCount() {
        return dims.values().stream()
                .mapToInt(d -> d.divisions.isEmpty() ? 0 :
                        d.divisions.get(0).tagToIds.values().stream()
                                .mapToInt(List::size).sum())
                .sum();
    }

    @Override
    public Set<Integer> getRegisteredDimensions() {
        return dims.keySet();
    }

    @Override
    public int getVectorCountForDimension(int dim) {
        DimensionState S = dims.get(dim);
        if (S == null || S.divisions.isEmpty()) return 0;
        return S.divisions.get(0).tagToIds.values().stream()
                .mapToInt(List::size).sum();
    }

    public void finalizeForSearch() {
        if (frozen) return;
        frozen = true;

        for (var e : buffers.entrySet()) {
            int dim = e.getKey();
            List<CodedPoint> cps = e.getValue();

            if (cps.isEmpty()) continue;

            BuildResult br = buildForDim(dim, cps);
            built.put(dim, br);

            log.info(
                    "Partitioned index built: dim={} divisions={} w={} subsets={}",
                    dim,
                    br.divisions,
                    br.w,
                    br.indexI.get(0).size()
            );
        }

        buffers.clear();
    }

    // ======================================================
    // CODING (Option-C deterministic)
    // ======================================================
    private BitSet[] code(double[] vec) {
        return Coding.code(
                vec,
                cfg.getPaper().divisions,
                cfg.getPaper().m,
                cfg.getPaper().seed
        );
    }
}
