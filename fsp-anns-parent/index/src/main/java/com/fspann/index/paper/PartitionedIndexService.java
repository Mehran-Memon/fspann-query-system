package com.fspann.index.paper;

import com.fspann.common.EncryptedPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.BitSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * PartitionedIndexService
 * ---------------------------------------
 * Core properties:
 *   • Deterministic multi-division partition index
 *   • Uses only client BitSet codes (no server LSH)
 *   • Fully dimension-bound (no cross-dimension)
 *   • Deterministic union semantics
 *   • Compatible with forward security (EncryptedPoint opaque)
 *   • K-adaptive widening via probe.shards
 *   • No fallback modes, no dimension mismatch fallback
 *
 * Partition model (per dimension d):
 *   • ℓ divisions
 *   • each division has:
 *        G-function family descriptor (m × lambda)
 *        intervals SubsetBounds[] = I
 *        tag → subset mapping
 *
 * Insert:
 *   • Uses CodedPoint (EncryptedPoint + BitSet[])
 *
 * Lookup:
 *   • Uses token.codes[] only (privacy)
 */
public final class PartitionedIndexService{

    private static final Logger log = LoggerFactory.getLogger(PartitionedIndexService.class);

    // ---------------------------------------------------------------------
    // Paper parameters
    // ---------------------------------------------------------------------
    private final int m;          // projections per division
    private final int lambda;     // bits per projection
    private final int divisions;  // ℓ
    private final long seedBase;  // global seed

    private final int buildThreshold;
    // α used in paper for partition fanout
    private static final double PAPER_ALPHA = clampAlpha(
            Double.parseDouble(System.getProperty("paper.alpha", "0.02"))
    );

    // Global cap across divisions (if > 0)
    private static final int MAX_ALL_CAND =
            Integer.getInteger("paper.maxCandidatesAllDivs", -1);

    // per-div expansion cap
    private static final int MAX_PER_DIV =
            Integer.getInteger("paper.maxPerDiv", 65_536);

    // dimension → state
    private final ConcurrentMap<Integer, DimensionState> dims = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> idToDim = new ConcurrentHashMap<>();

    public PartitionedIndexService(int m, int lambda, int divisions, long seedBase) {
        this(m, lambda, divisions, seedBase,
                Integer.getInteger("paper.buildThreshold", 2_000),
                Integer.getInteger("paper.maxCandidates", -1));
    }

    public PartitionedIndexService(int m, int lambda, int divisions,
                                   long seedBase, int buildThreshold, int maxCandidates) {
        if (m <= 0 || lambda <= 0 || divisions <= 0)
            throw new IllegalArgumentException("m, lambda, divisions must all be > 0");

        this.m = m;
        this.lambda = lambda;
        this.divisions = divisions;
        this.seedBase = seedBase;

        this.buildThreshold = buildThreshold;
    }

    private static double clampAlpha(double x) {
        if (Double.isNaN(x)) return 0.03;
        if (x < 0.005) return 0.005;
        if (x > 0.10) return 0.10;
        return x;
    }

    // =====================================================================
    //  Core Structures
    // =====================================================================

    private static final class DivisionState {
        final Coding.CodeFamily G;
        List<GreedyPartitioner.SubsetBounds> I = Collections.emptyList();
        Map<String, List<EncryptedPoint>> tagToSubset = new ConcurrentHashMap<>();
        int w = 0;  // baseTarget per division

        DivisionState(Coding.CodeFamily g) { this.G = g; }
    }

    private static final class DimensionState {
        final int dim;
        final List<EncryptedPoint> staged = new ArrayList<>();
        final List<BitSet[]> stagedCodes = new ArrayList<>();
        final List<DivisionState> divs = new ArrayList<>();

        DimensionState(int dim) { this.dim = dim; }
    }

    // =====================================================================
    //  PaperSearchEngine API: INSERT
    // =====================================================================

    public void insert(EncryptedPoint pt) {
        throw new UnsupportedOperationException(
                "Option-C: server requires codes; call insert(ep, vector) or insertWithCodes().");
    }

    public void insert(EncryptedPoint pt, double[] vec) {
        Objects.requireNonNull(vec);
        BitSet[] codes = code(vec);
        insertWithCodes(new CodedPoint(pt, codes, divisions));
    }

    public void insertWithCodes(CodedPoint cp) {
        if (cp == null || cp.pt == null)
            throw new IllegalArgumentException("CodedPoint cannot be null");
        if (cp.codes == null || cp.codes.length != divisions)
            throw new IllegalArgumentException("CodedPoint codes[] must size == divisions");

        int dim = cp.pt.getVectorLength();
        idToDim.put(cp.pt.getId(), dim);

        DimensionState S = dims.computeIfAbsent(dim, DimensionState::new);

        synchronized (S) {
            if (!isBuilt(S)) {
                S.staged.add(cp.pt);
                S.stagedCodes.add(cp.codes);
                if (S.staged.size() >= buildThreshold)
                    buildAllDivisions(S);
                return;
            }

            // Already built: direct bucket insert
            for (int i = 0; i < divisions; i++) {
                DivisionState div = S.divs.get(i);
                BitSet code = cp.codes[i];

                for (int idx : covering(code, div.I)) {
                    GreedyPartitioner.SubsetBounds sb = div.I.get(idx);
                    div.tagToSubset
                            .computeIfAbsent(sb.tag, k -> new CopyOnWriteArrayList<>())
                            .add(cp.pt);
                }
            }
        }
    }

    // =====================================================================
    //  PaperSearchEngine API: LOOKUP
    // =====================================================================

    public List<EncryptedPoint> lookup(com.fspann.common.QueryToken tok) {

        Objects.requireNonNull(tok);

        // SAFETY SHORT-CIRCUIT
        BitSet[] qcodes = tok.getCodes();
        if (qcodes == null || qcodes.length == 0) {
            return Collections.emptyList();
        }

        int dim = tok.getDimension();
        if (dim <= 0) return Collections.emptyList();
        DimensionState S = dims.get(dim);
        if (S == null) return Collections.emptyList();

        ensureBuilt(S);

        final Set<String> seen = new LinkedHashSet<>();
        final List<EncryptedPoint> out = new ArrayList<>();

        final int globalCap = resolveGlobalCap();
        final int N = getVectorCountForDimension(dim);

        for (int divIdx = 0; divIdx < divisions; divIdx++) {
            if (out.size() >= globalCap) break;

            DivisionState div = S.divs.get(divIdx);
            if (div.I.isEmpty()) continue;

            BitSet qc = qcodes[divIdx];
            if (qc == null) continue;

            List<Integer> anchors = covering(qc, div.I);
            if (anchors.isEmpty()) {
                int near = nearest(qc, div.I);
                if (near >= 0) anchors = List.of(near);
            }
            if (anchors.isEmpty()) continue;

            int baseTarget = Math.max(1,
                    (int) Math.ceil(PAPER_ALPHA * N / (double) divisions));

            int target = applyProbeScaling(baseTarget);
            int collected = 0;

            boolean[] visited = new boolean[div.I.size()];

            // 1) anchor intervals
            for (int a : anchors) {
                if (a < 0 || a >= div.I.size() || visited[a]) continue;
                collected += collectInterval(div, a, seen, out, target, globalCap, visited);
                if (collected >= target || out.size() >= globalCap)
                    break;
            }
            if (collected >= target || out.size() >= globalCap)
                continue;

            // 2) symmetric widening
            for (int r = 1; r < div.I.size() && collected < target && out.size() < globalCap; r++) {
                for (int a : anchors) {
                    if (out.size() >= globalCap) break;

                    int L = a - r;
                    if (L >= 0 && !visited[L]) {
                        collected += collectInterval(div, L, seen, out, target, globalCap, visited);
                        if (collected >= target || out.size() >= globalCap) break;
                    }

                    int R = a + r;
                    if (R < div.I.size() && !visited[R]) {
                        collected += collectInterval(div, R, seen, out, target, globalCap, visited);
                        if (collected >= target || out.size() >= globalCap) break;
                    }
                }
            }
        }

        return out;
    }

    private int collectInterval(
            DivisionState div,
            int idx,
            Set<String> seen,
            List<EncryptedPoint> out,
            int target,
            int globalCap,
            boolean[] visited)
    {
        visited[idx] = true;
        GreedyPartitioner.SubsetBounds sb = div.I.get(idx);
        List<EncryptedPoint> subset = div.tagToSubset.get(sb.tag);
        if (subset == null || subset.isEmpty()) return 0;

        int count = 0;
        for (EncryptedPoint ep : subset) {
            if (out.size() >= globalCap) break;
            if (seen.add(ep.getId())) {
                out.add(ep);
                count++;
                if (count >= target) break;
            }
        }
        return count;
    }

    // =====================================================================
    //  PaperSearchEngine: DELETE + METRICS
    // =====================================================================

    public void delete(String id) {
        Integer dim = idToDim.remove(id);
        if (dim == null) return;

        DimensionState S = dims.get(dim);
        if (S == null) return;

        synchronized (S) {
            S.staged.removeIf(p -> id.equals(p.getId()));
            S.stagedCodes.removeIf(codeArr ->
                    Arrays.stream(codeArr).anyMatch(Objects::isNull)); // safe alignment

            for (DivisionState div : S.divs) {
                for (List<EncryptedPoint> subset : div.tagToSubset.values())
                    subset.removeIf(p -> id.equals(p.getId()));
            }
        }
    }

    public int getVectorCountForDimension(int dim) {
        DimensionState S = dims.get(dim);
        if (S == null) return 0;

        synchronized (S) {
            Set<String> ids = new HashSet<>();
            for (EncryptedPoint ep : S.staged) ids.add(ep.getId());
            for (DivisionState div : S.divs) {
                for (List<EncryptedPoint> subset : div.tagToSubset.values())
                    for (EncryptedPoint ep : subset) ids.add(ep.getId());
            }
            return ids.size();
        }
    }

    public int getVectorCountForDimension(int d, boolean includeAll) {
        return getVectorCountForDimension(d);
    }

    // =====================================================================
    //  BUILD
    // =====================================================================

    private boolean isBuilt(DimensionState S) {
        return !S.divs.isEmpty() && !S.divs.get(0).I.isEmpty();
    }

    private void ensureBuilt(DimensionState S) {
        if (isBuilt(S)) return;
        synchronized (S) {
            if (!isBuilt(S)) buildAllDivisions(S);
        }
    }

    private void buildAllDivisions(DimensionState S) {
        int N = S.staged.size();
        if (N == 0) return;

        List<EncryptedPoint> pts = new ArrayList<>(N);
        List<BitSet[]> codes = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            pts.add(S.staged.get(i));
            codes.add(S.stagedCodes.get(i));
        }

        S.divs.clear();

        for (int divIdx = 0; divIdx < divisions; divIdx++) {
            long seed = deriveSeed(divIdx, S.dim);

            Coding.GMeta G = Coding.fromSeedOnly(m, lambda, seed);

            List<GreedyPartitioner.Item> items = new ArrayList<>(N);
            for (int i = 0; i < N; i++)
                items.add(new GreedyPartitioner.Item(pts.get(i).getId(),
                        pts.get(i), codes.get(i)[divIdx]));

            GreedyPartitioner.BuildResult br =
                    GreedyPartitioner.build(items, G.codeBits(),
                            seed ^ 0x9E3779B97F4A7C15L);

            DivisionState div = new DivisionState(G);
            div.I = br.indexI;

            Map<String, List<EncryptedPoint>> map = new ConcurrentHashMap<>();
            for (var e : br.tagToSubset.entrySet())
                map.put(e.getKey(), new CopyOnWriteArrayList<>(e.getValue()));
            div.tagToSubset = map;

            div.w = Math.max(1,
                    (int) Math.ceil(PAPER_ALPHA * N / (double) divisions));

            log.info("Built division {}/{} for dim={} with w={}",
                    (divIdx + 1), divisions, S.dim, div.w);

            S.divs.add(div);
        }

        S.staged.clear();
        S.stagedCodes.clear();
    }

    private long deriveSeed(int divIdx, int dim) {
        long x = seedBase;
        x ^= ((long) divIdx * 0xBF58476D1CE4E5B9L);
        x ^= ((long) dim     * 0x94D049BB133111EBL);
        x ^= ((long) m       * 0x2545F4914F6CDD1DL);
        x ^= ((long) lambda  * 0xD1342543DE82EF95L);
        return mix64(x);
    }

    // =====================================================================
    //  Covering / Nearest
    // =====================================================================

    private List<Integer> covering(BitSet code,
                                   List<GreedyPartitioner.SubsetBounds> I)
    {
        if (I.isEmpty() || code == null) return Collections.emptyList();

        GreedyPartitioner.CodeComparator cmp =
                new GreedyPartitioner.CodeComparator(I.get(0).codeBits);

        int lo = 0, hi = I.size() - 1, best = -1;

        // rightmost interval with lower <= code
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (cmp.compare(I.get(mid).lower, code) <= 0) {
                best = mid;
                lo = mid + 1;
            } else hi = mid - 1;
        }

        if (best < 0) return Collections.emptyList();

        List<Integer> hits = new ArrayList<>(3);
        for (int i = Math.max(0, best - 1);
             i <= Math.min(I.size() - 1, best + 1); i++)
        {
            if (cmp.compare(I.get(i).lower, code) <= 0 &&
                    cmp.compare(code, I.get(i).upper) <= 0)
            {
                hits.add(i);
            }
        }

        return hits.isEmpty() ?
                List.of(Math.max(0, Math.min(best, I.size() - 1))) :
                hits;
    }

    private int nearest(BitSet code, List<GreedyPartitioner.SubsetBounds> I) {
        if (I.isEmpty()) return -1;
        int best = -1, bestGap = Integer.MAX_VALUE;
        for (int i = 0; i < I.size(); i++) {
            BitSet rep = (I.get(i).lower != null) ? I.get(i).lower : I.get(i).upper;
            int gap = hamming(code, rep);
            if (gap < bestGap) { bestGap = gap; best = i; }
        }
        return best;
    }

    private int hamming(BitSet a, BitSet b) {
        if (a == null || b == null) return Integer.MAX_VALUE;
        BitSet x = (BitSet) a.clone();
        x.xor(b);
        return x.cardinality();
    }

    // =====================================================================
    //  Helpers
    // =====================================================================

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    /**
     * Server-side coding function (Option-C).
     * Deterministic m × λ random hyperplanes per division.
     */
    public BitSet[] code(double[] vec) {
        Objects.requireNonNull(vec, "vec");
        BitSet[] out = new BitSet[divisions];

        for (int div = 0; div < divisions; div++) {
            BitSet bits = new BitSet(m);

            for (int proj = 0; proj < m; proj++) {
                long seed = mix64(
                        seedBase
                                ^ ((long) div * 0x9E3779B97F4A7C15L)
                                ^ ((long) proj * 0xBF58476D1CE4E5B9L)
                );

                double dot = 0.0;
                long s = seed;

                for (double v : vec) {
                    s ^= (s << 21);
                    s ^= (s >>> 35);
                    s ^= (s << 4);

                    double r = ((s & 0x3fffffffL) / (double) 0x3fffffffL) * 2.0 - 1.0;
                    dot += v * r;
                }

                if (dot >= 0) bits.set(proj);
            }
            out[div] = bits;
        }
        return out;
    }

    private int resolveGlobalCap() {
        if (MAX_ALL_CAND > 0) return MAX_ALL_CAND;
        String prop = System.getProperty("probe.shards");
        if (prop != null) {
            try {
                int v = Integer.parseInt(prop.trim());
                if (v > 0) return Math.min(v, 1_000_000_000);
            } catch (Throwable ignore) {}
        }
        return Integer.MAX_VALUE;
    }

    private int applyProbeScaling(int base) {
        String prop = System.getProperty("probe.shards");
        if (prop == null) return base;

        int v;
        try {
            v = Integer.parseInt(prop.trim());
        } catch (Throwable t) {
            return base;
        }
        if (v <= 0) return base;

        int factor = (int) Math.round(Math.log(v) / Math.log(2.0));
        if (factor < 1) factor = 1;
        if (factor > 64) factor = 64;

        long scaled = (long) base * factor;
        int out = (scaled > MAX_PER_DIV) ? MAX_PER_DIV : (int) scaled;
        return out;
    }

    // public getters
    public int getTotalVectorCount() {
        int sum = 0;
        for (int dim : dims.keySet())
            sum += getVectorCountForDimension(dim);
        return sum;
    }

    /**
     * Get shard ID for vector (compatibility method).
     * Option-C partitioned mode doesn't use traditional shards.
     * Returns -1 to indicate no specific shard.
     *
     * @param vector the vector (unused in partitioned mode)
     * @return -1 (no shard assignment in Option-C)
     */
    public int getShardIdForVector(double[] vector) {
        // Option-C doesn't use shards; partitions are division-based
        // Return -1 to indicate N/A
        return -1;
    }

    public Set<Integer> getRegisteredDimensions() {
        return Collections.unmodifiableSet(dims.keySet());
    }

    public void updateCachedPoint(EncryptedPoint ep) {
        Objects.requireNonNull(ep);
        Integer dim = idToDim.get(ep.getId());
        if (dim == null) return;

        DimensionState S = dims.get(dim);
        if (S == null) return;

        synchronized (S) {
            for (int i = 0; i < S.staged.size(); i++) {
                if (ep.getId().equals(S.staged.get(i).getId()))
                    S.staged.set(i, ep);
            }

            for (DivisionState div : S.divs) {
                for (List<EncryptedPoint> subset : div.tagToSubset.values()) {
                    for (int i = 0; i < subset.size(); i++)
                        if (ep.getId().equals(subset.get(i).getId()))
                            subset.set(i, ep);
                }
            }
        }
    }
}
