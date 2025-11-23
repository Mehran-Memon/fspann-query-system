package com.fspann.index.paper;

import com.fspann.common.EncryptedPoint;
import com.fspann.index.service.SecureLSHIndexService.PaperSearchEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentMap;

/**
 * PartitionedIndexService
 * -----------------------
 * Implements the partitioning scheme with multi-division codes and forward-secure indexing.
 *
 * Core ideas:
 *  - Index is per-dimension (d) -> DimensionState.
 *  - Each dimension has ℓ divisions. Each division:
 *      * Has a CodeFamily (G) and SubsetBounds index I (ordered in code-space).
 *      * Maps subset tags -> lists of EncryptedPoint.
 *  - Insert path uses precomputed codes from the client (CodedPoint).
 *  - Lookup path uses ONLY client codes (token.getCodes()) for privacy.
 *
 * K-Adaptive:
 *  - PAPER_W_ALPHA controls the base per-division fanout:
 *        w_div ≈ α * |D| / ℓ
 *  - System property `probe.shards` rescales w_div via applyProbeScaling().
 *  - Global candidate cap is derived from:
 *        paper.maxCandidatesAllDivs  OR  probe.shards  OR  Integer.MAX_VALUE
 */
public class PartitionedIndexService implements PaperSearchEngine {

    // ----------------------------- Tunables -----------------------------
    private final int m;                // projections per division
    private final int lambda;           // bits per projection
    private final int divisions;        // ℓ (number of divisions)
    private final long seedBase;        // base seed to derive per-division seeds
    private final int buildThreshold;   // min items per dimension to build partitions
    private final int maxCandidates;    // optional upper bound (currently unused directly)

    private static final Logger logger = LoggerFactory.getLogger(PartitionedIndexService.class);

    // paper.alpha (0.5%–10% clamp)
    private static final double PAPER_W_ALPHA = clampAlpha(
            Double.parseDouble(System.getProperty("paper.alpha", "0.02"))
    );

    /**
     * Hard global cap across all divisions if > 0.
     * Otherwise, we fall back to probe.shards or Integer.MAX_VALUE.
     */
    private static final int MAX_ALL_CANDIDATES =
            Integer.getInteger("paper.maxCandidatesAllDivs", -1);

    /**
     * Upper bound for per-division fanout after probe.shards scaling.
     */
    private static final int MAX_PER_DIV =
            Integer.getInteger("paper.maxPerDiv", 65_536);

    // dimension -> state
    private final ConcurrentMap<Integer, DimensionState> dims = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> idToDim = new ConcurrentHashMap<>();

    public PartitionedIndexService(int m, int lambda, int divisions, long seedBase) {
        this(m, lambda, divisions, seedBase,
                Integer.getInteger("paper.buildThreshold", 2_000),
                Integer.getInteger("paper.maxCandidates", -1)
        );
    }

    public PartitionedIndexService(int m, int lambda, int divisions, long seedBase,
                                   int buildThreshold, int maxCandidates) {
        if (m <= 0 || lambda <= 0 || divisions <= 0) {
            throw new IllegalArgumentException("m, lambda, divisions must be > 0");
        }
        this.m = m;
        this.lambda = lambda;
        this.divisions = divisions;
        this.seedBase = seedBase;
        this.buildThreshold = buildThreshold;
        this.maxCandidates = maxCandidates;
    }

    private static double clampAlpha(double x) {
        if (Double.isNaN(x)) return 0.03;
        if (x < 0.005) return 0.005;
        if (x > 0.10) return 0.10;
        return x;
    }

    // --------------------------------------------------------------------
    // Coding: must match QueryTokenFactory.code(...)
    // --------------------------------------------------------------------

    public BitSet[] code(double[] vec) {
        Objects.requireNonNull(vec, "vec");
        final BitSet[] out = new BitSet[divisions];
        for (int div = 0; div < divisions; div++) {
            final BitSet bits = new BitSet(m);
            for (int proj = 0; proj < m; proj++) {
                long seed = mix64(seedBase
                        ^ ((long) div * 0x9E3779B97F4A7C15L)
                        ^ ((long) proj * 0xBF58476D1CE4E5B9L));
                double dot = 0.0;
                long s = seed;
                for (double v : vec) {
                    // xorshift-ish update
                    s ^= (s << 21);
                    s ^= (s >>> 35);
                    s ^= (s << 4);
                    // map to [-1, 1]
                    double r = ((s & 0x3fffffffL) / (double) 0x3fffffffL) * 2.0 - 1.0;
                    dot += v * r;
                }
                if (dot >= 0) bits.set(proj);
            }
            out[div] = bits;
        }
        return out;
    }

    /** Only returns a GFunction if already present; we don’t rebuild from GMeta here. */
    @SuppressWarnings("unused")
    private static Coding.GFunction toLegacyGFunction(Coding.CodeFamily g) {
        if (g instanceof Coding.GFunction gf) {
            return gf;
        }
        return null;
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdl;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53l;
        return z ^ (z >>> 33);
    }

    private static long mix(long seedBase, int d, int m, int lambda, int division) {
        long x = seedBase;
        x ^= ((long) d * 0xBF58476D1CE4E5B9L);
        x ^= ((long) m * 0x94D049BB133111EBL);
        x ^= ((long) lambda * 0x2545F4914F6CDD1DL);
        x ^= ((long) division * 0xD1342543DE82EF95L);
        x ^= (x >>> 33); x *= 0xff51afd7ed558ccdl;
        x ^= (x >>> 33); x *= 0xc4ceb9fe1a85ec53l;
        x ^= (x >>> 33);
        return x;
    }

    // ------------------------- In-memory state --------------------------

    private static final class DivisionState {
        final Coding.CodeFamily G;  // LSH function family descriptor
        List<GreedyPartitioner.SubsetBounds> I = Collections.emptyList(); // sorted
        Map<String, List<EncryptedPoint>> tagToSubset = new ConcurrentHashMap<>();
        int w = 0; // base per-division paper-w target

        DivisionState(Coding.CodeFamily g) { this.G = g; }
    }

    private static final class DimensionState {
        final int d;
        final List<EncryptedPoint> staged = new ArrayList<>();
        final List<BitSet[]> stagedCodes = new ArrayList<>();
        final List<DivisionState> divisions = new ArrayList<>();
        DimensionState(int d) { this.d = d; }
    }

    // --------------------------------------------------------------------
    // PaperSearchEngine API
    // --------------------------------------------------------------------

    @Override
    @Deprecated
    public void insert(EncryptedPoint pt) {
        throw new UnsupportedOperationException("Server requires precomputed codes; use insertWithCodes().");
    }

    @Override
    @Deprecated
    public void insert(EncryptedPoint pt, double[] plaintextVector) {
        if (plaintextVector == null) throw new IllegalArgumentException("plaintextVector cannot be null");
        final BitSet[] codes = code(plaintextVector);
        insertWithCodes(new CodedPoint(pt, codes, this.divisions));
    }

    /**
     * Main insert path: server receives EncryptedPoint + codes (for all ℓ divisions).
     */
    public void insertWithCodes(CodedPoint cp) {
        if (cp == null || cp.pt == null || cp.codes == null || cp.codes.length != divisions) {
            throw new IllegalArgumentException("CodedPoint must provide codes for all divisions.");
        }
        final int d = cp.pt.getVectorLength();
        idToDim.put(cp.pt.getId(), d);
        final DimensionState S = dims.computeIfAbsent(d, DimensionState::new);
        synchronized (S) {
            if (isBuilt(S)) {
                // Already partitioned: insert into appropriate subsets immediately
                for (int divIdx = 0; divIdx < divisions; divIdx++) {
                    final DivisionState div = S.divisions.get(divIdx);
                    final BitSet code = cp.codes[divIdx];
                    for (int idx : findCoveringIntervals(code, div.I)) {
                        final GreedyPartitioner.SubsetBounds sb = div.I.get(idx);
                        div.tagToSubset
                                .computeIfAbsent(sb.tag, k -> new CopyOnWriteArrayList<>())
                                .add(cp.pt);
                    }
                }
            } else {
                // Still staging: buffer for later build
                S.staged.add(cp.pt);
                S.stagedCodes.add(cp.codes);
            }
            maybeBuild(S);
        }
    }

    @Override
    public List<EncryptedPoint> lookup(com.fspann.common.QueryToken token) {
        Objects.requireNonNull(token, "QueryToken must not be null");
        final int d = token.getDimension();
        final DimensionState S = dims.get(d);
        if (S == null) return Collections.emptyList();

        ensureBuilt(S);

        final Set<String> seen = new LinkedHashSet<>();
        final List<EncryptedPoint> cands = new ArrayList<>();

        // We rely only on client-supplied codes in partitioned mode.
        final BitSet[] codesFromClient = token.getCodes();
        final int globalCap = resolveGlobalCap();

        for (int divIdx = 0; divIdx < S.divisions.size(); divIdx++) {
            if (cands.size() >= globalCap) break;

            final DivisionState div = S.divisions.get(divIdx);
            if (div.I.isEmpty()) continue;

            // Use code from token for this division (if provided)
            BitSet Cq = null;
            if (codesFromClient != null && divIdx < codesFromClient.length) {
                BitSet src = codesFromClient[divIdx];
                if (src != null) {
                    Cq = (BitSet) src.clone();
                }
            }

            // If no code is supplied for this division, skip (privacy-preserving).
            if (Cq == null) continue;

            // Seed intervals that cover Cq; fallback to nearest interval
            List<Integer> hits = findCoveringIntervals(Cq, div.I);
            if (hits.isEmpty()) {
                int seed = nearestIdx(Cq, div.I);
                if (seed >= 0) hits = List.of(seed);
            }
            if (hits.isEmpty()) continue;

            final int currentSize = getVectorCountForDimension(S.d);

            // Base target per division (paper α rule)
            int baseTargetPerDiv = Math.max(
                    1,
                    (int) Math.ceil(PAPER_W_ALPHA * currentSize / (double) this.divisions)
            );

            // Apply probe.shards-based scaling (K-adaptive widening)
            int targetPerDiv = applyProbeScaling(baseTargetPerDiv);

            int gatheredThisDiv = 0;
            final boolean[] visited = new boolean[div.I.size()];

            // 1) Anchors
            for (int idx : hits) {
                if (idx < 0 || idx >= div.I.size() || visited[idx]) continue;
                GreedyPartitioner.SubsetBounds sb = div.I.get(idx);
                List<EncryptedPoint> subset = div.tagToSubset.get(sb.tag);
                if (subset != null && !subset.isEmpty()) {
                    for (EncryptedPoint p : subset) {
                        if (cands.size() >= globalCap) break;
                        if (seen.add(p.getId())) {
                            cands.add(p);
                            if (++gatheredThisDiv >= targetPerDiv) break;
                        }
                    }
                }
                visited[idx] = true;
                if (gatheredThisDiv >= targetPerDiv || cands.size() >= globalCap) break;
            }
            if (gatheredThisDiv >= targetPerDiv || cands.size() >= globalCap) continue;

            // 2) Symmetric widening around anchors
            for (int radius = 1; gatheredThisDiv < targetPerDiv && radius < div.I.size(); radius++) {
                for (int anchor : hits) {
                    if (cands.size() >= globalCap) break;

                    int left = anchor - radius;
                    if (left >= 0 && !visited[left]) {
                        GreedyPartitioner.SubsetBounds sbL = div.I.get(left);
                        List<EncryptedPoint> subsetL = div.tagToSubset.get(sbL.tag);
                        if (subsetL != null && !subsetL.isEmpty()) {
                            for (EncryptedPoint p : subsetL) {
                                if (cands.size() >= globalCap) break;
                                if (seen.add(p.getId())) {
                                    cands.add(p);
                                    if (++gatheredThisDiv >= targetPerDiv) break;
                                }
                            }
                        }
                        visited[left] = true;
                    }
                    if (gatheredThisDiv >= targetPerDiv || cands.size() >= globalCap) break;

                    int right = anchor + radius;
                    if (right < div.I.size() && !visited[right]) {
                        GreedyPartitioner.SubsetBounds sbR = div.I.get(right);
                        List<EncryptedPoint> subsetR = div.tagToSubset.get(sbR.tag);
                        if (subsetR != null && !subsetR.isEmpty()) {
                            for (EncryptedPoint p : subsetR) {
                                if (cands.size() >= globalCap) break;
                                if (seen.add(p.getId())) {
                                    cands.add(p);
                                    if (++gatheredThisDiv >= targetPerDiv) break;
                                }
                            }
                        }
                        visited[right] = true;
                    }
                    if (gatheredThisDiv >= targetPerDiv || cands.size() >= globalCap) break;
                }
            }
        }

        return cands;
    }

    @Override
    public void delete(String id) {
        Integer d = idToDim.remove(id);
        if (d == null) return;
        DimensionState S = dims.get(d);
        if (S == null) return;

        synchronized (S) {
            // Remove from staged
            for (int i = S.staged.size() - 1; i >= 0; i--) {
                if (id.equals(S.staged.get(i).getId())) {
                    S.staged.remove(i);
                    break;
                }
            }
            // Remove from built subsets
            for (DivisionState div : S.divisions) {
                for (List<EncryptedPoint> subset : div.tagToSubset.values()) {
                    subset.removeIf(p -> id.equals(p.getId()));
                }
            }
        }
    }

    @Override
    public int getVectorCountForDimension(int dimension) {
        DimensionState S = dims.get(dimension);
        if (S == null) return 0;
        synchronized (S) {
            // Collect unique ids from staged points
            Set<String> ids = new HashSet<>();
            for (EncryptedPoint p : S.staged) {
                ids.add(p.getId());
            }

            // Collect unique ids from all built divisions / subsets
            for (DivisionState div : S.divisions) {
                for (List<EncryptedPoint> subset : div.tagToSubset.values()) {
                    for (EncryptedPoint p : subset) {
                        ids.add(p.getId());
                    }
                }
            }

            return ids.size();
        }
    }

    private boolean isBuilt(DimensionState S) {
        return !S.divisions.isEmpty() && !S.divisions.get(0).I.isEmpty();
    }

    private void maybeBuild(DimensionState S) {
        if (isBuilt(S)) return;
        if (S.staged.size() >= buildThreshold) {
            buildAllDivisions(S);
        }
    }

    private void ensureBuilt(DimensionState S) {
        if (isBuilt(S)) return;
        synchronized (S) {
            if (!isBuilt(S)) buildAllDivisions(S);
        }
    }

    private void buildAllDivisions(DimensionState S) {
        List<EncryptedPoint> pts = new ArrayList<>();
        List<BitSet[]> codes = new ArrayList<>();

        for (int i = 0; i < S.staged.size(); i++) {
            BitSet[] cs = S.stagedCodes.get(i);
            if (cs != null && cs.length == divisions) {
                pts.add(S.staged.get(i));
                codes.add(cs);
            }
        }

        if (pts.isEmpty()) return;

        final int d = S.d;
        S.divisions.clear();

        for (int divIdx = 0; divIdx < divisions; divIdx++) {
            long seed = mix(seedBase, d, m, lambda, divIdx);
            Coding.GMeta G = Coding.fromSeedOnly(m, lambda, seed);

            List<GreedyPartitioner.Item> items = new ArrayList<>(pts.size());
            for (int i = 0; i < pts.size(); i++) {
                items.add(new GreedyPartitioner.Item(pts.get(i).getId(), pts.get(i), codes.get(i)[divIdx]));
            }

            GreedyPartitioner.BuildResult br = GreedyPartitioner.build(
                    items, G.codeBits(), seed ^ 0x9E3779B97F4A7C15L
            );

            DivisionState D = new DivisionState(G);
            D.I = br.indexI;

            Map<String, List<EncryptedPoint>> safe = new ConcurrentHashMap<>();
            for (var e : br.tagToSubset.entrySet()) {
                safe.put(e.getKey(), new CopyOnWriteArrayList<>(e.getValue()));
            }
            D.tagToSubset = safe;

            D.w = Math.max(1, (int) Math.ceil(PAPER_W_ALPHA * pts.size() / (double) this.divisions));

            logger.info("Division {} paper-w target set to {} (alpha={} * |D|={} / ℓ={})",
                    (divIdx + 1), D.w, PAPER_W_ALPHA, pts.size(), this.divisions);

            S.divisions.add(D);
        }

        S.staged.clear();
        S.stagedCodes.clear();
    }

    private List<Integer> findCoveringIntervals(BitSet code, List<GreedyPartitioner.SubsetBounds> I) {
        if (I.isEmpty()) return Collections.emptyList();
        GreedyPartitioner.CodeComparator cmp = new GreedyPartitioner.CodeComparator(I.get(0).codeBits);
        int lo = 0, hi = I.size() - 1, ans = -1;

        // rightmost lower <= code
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (cmp.compare(I.get(mid).lower, code) <= 0) { ans = mid; lo = mid + 1; }
            else { hi = mid - 1; }
        }

        if (ans < 0) return Collections.emptyList();

        List<Integer> hits = new ArrayList<>();
        for (int j = Math.max(0, ans - 1); j <= Math.min(I.size() - 1, ans + 1); j++) {
            if (cmp.compare(I.get(j).lower, code) <= 0 && cmp.compare(code, I.get(j).upper) <= 0) {
                hits.add(j);
            }
        }
        return hits.isEmpty() ? List.of(Math.max(0, Math.min(ans, I.size() - 1))) : hits;
    }

    private int nearestIdx(BitSet Cq, List<GreedyPartitioner.SubsetBounds> I) {
        if (I == null || I.isEmpty()) return -1;
        int best = -1, bestGap = Integer.MAX_VALUE;
        for (int j = 0; j < I.size(); j++) {
            GreedyPartitioner.SubsetBounds sb = I.get(j);
            BitSet rep = (sb.lower != null) ? sb.lower : sb.upper;
            int gap = hammingGap(Cq, rep);
            if (gap < bestGap) { bestGap = gap; best = j; }
        }
        return best;
    }

    private int hammingGap(BitSet a, BitSet b) {
        if (a == null || b == null) return Integer.MAX_VALUE;
        BitSet x = (BitSet) a.clone();
        x.xor(b);
        return x.cardinality();
    }

    public int getDivisions() {
        return this.divisions;
    }

    // --------------------------------------------------------------------
    // Extra helpers for SecureLSHIndexService / re-encryption
    // --------------------------------------------------------------------

    public Set<Integer> getRegisteredDimensions() {
        return Collections.unmodifiableSet(dims.keySet());
    }

    public int getTotalVectorCount() {
        int sum = 0;
        for (Integer dim : dims.keySet()) {
            sum += getVectorCountForDimension(dim);
        }
        return sum;
    }

    public void updateCachedPoint(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "pt");
        Integer d = idToDim.get(pt.getId());
        if (d == null) return;
        DimensionState S = dims.get(d);
        if (S == null) return;

        synchronized (S) {
            for (int i = 0; i < S.staged.size(); i++) {
                if (pt.getId().equals(S.staged.get(i).getId())) {
                    S.staged.set(i, pt);
                }
            }
            for (DivisionState div : S.divisions) {
                for (List<EncryptedPoint> subset : div.tagToSubset.values()) {
                    for (int i = 0; i < subset.size(); i++) {
                        if (pt.getId().equals(subset.get(i).getId())) {
                            subset.set(i, pt);
                        }
                    }
                }
            }
        }
    }

    // --------------------------------------------------------------------
    // Legacy bucket helpers (if you ever need bucket IDs for logging)
    // --------------------------------------------------------------------

    private int computeBucket(BitSet code) {
        if (code == null) return 0;

        int bucket = 0;
        for (int i = 0; i < lambda; i++) {
            if (code.get(i)) {
                bucket |= (1 << i);
            }
        }
        return bucket;
    }

    public int[] getBucketsForCodes(BitSet[] codes) {
        int[] buckets = new int[codes.length];
        for (int t = 0; t < codes.length; t++) {
            buckets[t] = computeBucket(codes[t]);
        }
        return buckets;
    }

    // --------------------------------------------------------------------
    // probe.shards integration helpers (K-adaptive hook)
    // --------------------------------------------------------------------

    /**
     * Global cap for candidates across all divisions.
     * Priority:
     *   1) paper.maxCandidatesAllDivs (if > 0)
     *   2) probe.shards (if > 0)
     *   3) Integer.MAX_VALUE
     */
    private int resolveGlobalCap() {
        if (MAX_ALL_CANDIDATES > 0) {
            return MAX_ALL_CANDIDATES;
        }
        String prop = System.getProperty("probe.shards");
        if (prop != null) {
            try {
                int v = Integer.parseInt(prop.trim());
                if (v > 0) {
                    // defensive clamp
                    int cap = Math.min(v, 1_000_000_000);
                    logger.trace("resolveGlobalCap: using probe.shards={} -> globalCap={}", v, cap);
                    return cap;
                }
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Scale baseTargetPerDiv based on probe.shards (K-adaptive widening).
     *
     * Mapping:
     *   - If probe.shards is absent/invalid/<=0 → factor = 1 (no change).
     *   - Else factor ≈ log2(probe.shards), clamped to [1, 64].
     *
     * Each doubling of probe.shards increases per-division fanout linearly,
     * without exploding too fast. Final result is clamped by paper.maxPerDiv.
     */
    private int applyProbeScaling(int baseTargetPerDiv) {
        String prop = System.getProperty("probe.shards");
        if (prop == null) {
            return baseTargetPerDiv;
        }
        int v;
        try {
            v = Integer.parseInt(prop.trim());
        } catch (NumberFormatException e) {
            return baseTargetPerDiv;
        }
        if (v <= 0) return baseTargetPerDiv;

        // log2-based factor: 1,2,3,... as probe.shards grows
        int factor = (int) Math.round(Math.log(v) / Math.log(2.0));
        if (factor < 1) factor = 1;
        if (factor > 64) factor = 64;

        long scaled = (long) baseTargetPerDiv * (long) factor;
        int result;
        if (scaled > MAX_PER_DIV) {
            result = MAX_PER_DIV;
        } else {
            result = (int) scaled;
        }

        logger.trace("applyProbeScaling: baseTargetPerDiv={} probe.shards={} factor={} -> targetPerDiv={} (maxPerDiv={})",
                baseTargetPerDiv, v, factor, result, MAX_PER_DIV);

        return result;
    }
}
