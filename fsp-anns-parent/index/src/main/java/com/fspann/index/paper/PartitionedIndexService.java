package com.fspann.index.paper;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.index.service.SecureLSHIndexService.PaperSearchEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PartitionedIndexService
 * -----------------------
 * Reference, in-memory implementation of the paper-aligned engine:
 *   - Build ℓ independent divisions with G_m and partitions I (Algorithm-1/2).
 *   - Answer queries by producing tags per division (Algorithm-3), union subsets,
 *     and perform client-side exact kNN re-rank over returned candidates.
 *
 * Persistence is intentionally omitted here (in-memory only) to keep the
 * implementation compact. You can wire RocksDB/Buffer later by persisting:
 *   - G (alpha, r, omega, lambda, seed),
 *   - I (list of ⟨lower, upper, tag⟩),
 *   - tag -> subset mapping (or file path).
 */
public class PartitionedIndexService implements PaperSearchEngine {

    // ----------------------------- Tunables -----------------------------
    private final int m;                // projections per division
    private final int lambda;           // bits per projection
    private final int divisions;        // ℓ
    private final long seedBase;        // base seed to derive per-division seeds
    private final int buildThreshold;   // min items per dimension to build partitions
    private final int maxCandidates;    // safety cap for re-rank set size
    // Paper's recommended widening target w ≈ 2–5% of |D|.
    // We use 3% by default and scale per-division (mSANN_P) as |D| * α / ℓ.
    private static final double PAPER_W_ALPHA = clampAlpha(
            Double.parseDouble(System.getProperty("paper.alpha", "0.02"))
    );
    // hard cap across all divisions (safety)
    private static final int MAX_ALL_CANDIDATES =
            Integer.getInteger("paper.maxCandidatesAllDivs", -1);
    // dimension -> state
    private final Map<Integer, DimensionState> dims = new ConcurrentHashMap<>();
    // quick access by point id (for delete)
    private final Map<String, Integer> idToDim = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(PartitionedIndexService.class);

    public PartitionedIndexService(int m, int lambda, int divisions, long seedBase) {
        this(m, lambda, divisions, seedBase,
                Integer.getInteger("paper.buildThreshold", 2_000),
                Integer.getInteger("paper.maxCandidates", -1)
        );
    }

    public PartitionedIndexService(int m, int lambda, int divisions, long seedBase,
                                   int buildThreshold, int maxCandidates
    ) {
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

    // ------------------------- In-memory state --------------------------

    private static final class DivisionState {
        final Coding.CodeFamily G;  // was Coding.GFunction
        List<GreedyPartitioner.SubsetBounds> I = Collections.emptyList(); // sorted
        Map<String, List<EncryptedPoint>> tagToSubset = new ConcurrentHashMap<>();
        int w = 0;
        DivisionState(Coding.CodeFamily g) { this.G = g; }
    }

    private static final class DimensionState {
        final int d;
        // staged points (before first build)
        final List<EncryptedPoint> staged = new ArrayList<>();
        final List<BitSet[]> stagedCodes = new ArrayList<>();
        // built divisions
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
        final BitSet[] codes = code(plaintextVector); // length == divisions
        insertWithCodes(new com.fspann.index.paper.CodedPoint(pt, codes, this.divisions));
    }


    public void insertWithCodes(com.fspann.index.paper.CodedPoint cp) {
        if (cp == null || cp.pt == null || cp.codes == null || cp.codes.length != divisions) {
            throw new IllegalArgumentException("CodedPoint must provide codes for all divisions.");
        }
        final int d = cp.pt.getVectorLength();
        idToDim.put(cp.pt.getId(), d);
        final DimensionState S = dims.computeIfAbsent(d, DimensionState::new);
        synchronized (S) {
            if (isBuilt(S)) {
                for (int divIdx = 0; divIdx < divisions; divIdx++) {
                    final DivisionState div = S.divisions.get(divIdx);
                    final BitSet code = cp.codes[divIdx];
                    for (int idx : findCoveringIntervals(code, div.I)) {
                        final var sb = div.I.get(idx);
                        div.tagToSubset.computeIfAbsent(sb.tag, k -> new java.util.concurrent.CopyOnWriteArrayList<>())
                                .add(cp.pt);
                    }
                }
            } else {
                // stage point + codes for a later build
                S.staged.add(cp.pt);
                S.stagedCodes.add(cp.codes);
            }
            maybeBuild(S);
        }
    }

    /**
     * Client-side coding for a single plaintext vector.
     * Produces {@code divisions} BitSets; each BitSet has length {@code m} (projections per division).
     * A bit j in division i is set iff the signed random projection (seeded, deterministic) is >= 0.
     * This keeps the server free of plaintext while enabling strict insertWithCodes().
     */
    public BitSet[] code(double[] vec) {
        Objects.requireNonNull(vec, "vec");
        final BitSet[] out = new BitSet[divisions];
        for (int div = 0; div < divisions; div++) {
            final BitSet bits = new BitSet(m);
            // Seed per (division, projection) to keep coding deterministic across runs with same seed.
            for (int proj = 0; proj < m; proj++) {
                long seed = mix64(seedBase ^ (long) div * 0x9E3779B97F4A7C15L ^ (long) proj * 0xBF58476D1CE4E5B9L);
                double dot = 0.0;
                // Signed random projection using a simple LCG-derived pseudo-random vector
                long s = seed;
                for (double v : vec) {
                    s ^= (s << 21); s ^= (s >>> 35); s ^= (s << 4); // xorshift-ish
                    // Map bits to pseudo-normal-ish multiplier in [-1,1]
                    double r = ((s & 0x3fffffffL) / (double) 0x3fffffffL) * 2.0 - 1.0;
                    dot += v * r;
                }
                if (dot >= 0) bits.set(proj);
            }
            out[div] = bits;
        }
        return out;
    }


    // --- helpers (deterministic hashing) ---
    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdl;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53l;
        return z ^ (z >>> 33);
    }

    @Override
    public List<EncryptedPoint> lookup(com.fspann.common.QueryToken token) {
        Objects.requireNonNull(token, "QueryToken");
        final int d = token.getDimension();
        final DimensionState S = dims.get(d);
        if (S == null) return Collections.emptyList();

        ensureBuilt(S);

        final Set<String> seen = new LinkedHashSet<>();
        final List<EncryptedPoint> cands = new ArrayList<>();

        // Prefer codes from the token (privacy-preserving path)
        final BitSet[] codesFromClient = token.getCodes();

        // Legacy test path: compute codes from plaintext ONLY if present (not used in production)
        final double[] qPlain = (codesFromClient == null ? token.getPlaintextQuery() : null);

        // Hard cap across all divisions (optional)
        final int globalCap = MAX_ALL_CANDIDATES > 0 ? MAX_ALL_CANDIDATES : Integer.MAX_VALUE;

        for (int divIdx = 0; divIdx < S.divisions.size(); divIdx++) {
            if (cands.size() >= globalCap) break;

            final DivisionState div = S.divisions.get(divIdx);
            if (div.I.isEmpty()) continue;

            final Coding.GFunction legacyG = toLegacyGFunction(div.G);
            final BitSet Cq =
                    (codesFromClient != null &&
                            divIdx < codesFromClient.length &&
                            codesFromClient[divIdx] != null)
                            ? (BitSet) codesFromClient[divIdx].clone()
                            : ((qPlain != null && legacyG != null) ? Coding.C(qPlain, legacyG) : null);


            if (Cq == null) {
                // No way to obtain a code for this division; skip (keeps privacy guarantees)
                continue;
            }

            // Seed intervals that cover Cq; fallback to nearest interval
            List<Integer> hits = findCoveringIntervals(Cq, div.I);
            if (hits.isEmpty()) {
                int seed = nearestIdx(Cq, div.I);
                if (seed >= 0) hits = java.util.List.of(seed);
            }
            if (hits.isEmpty()) continue;

            final int targetPerDiv = Math.max(1, div.w);
            int gatheredThisDiv = 0;
            final boolean[] visited = new boolean[div.I.size()];

            // 1) anchors
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

            // 2) symmetric widening
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
            int built = 0;
            for (DivisionState div : S.divisions) {
                for (List<EncryptedPoint> subset : div.tagToSubset.values()) built += subset.size();
            }
            // staged may include duplicates across retries; count unique ids
            Set<String> ids = new HashSet<>();
            for (EncryptedPoint p : S.staged) ids.add(p.getId());
            return Math.max(built, ids.size());
        }
    }

    // --------------------------------------------------------------------
    // Build helpers
    // --------------------------------------------------------------------

    static List<Integer> findCoveringIntervals(BitSet code, List<GreedyPartitioner.SubsetBounds> I) {
        if (I.isEmpty()) return Collections.emptyList();
        GreedyPartitioner.CodeComparator cmp = new GreedyPartitioner.CodeComparator(I.get(0).codeBits);
        // binary search lower-bound
        int lo = 0, hi = I.size() - 1, ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (cmp.compare(I.get(mid).lower, code) <= 0) { ans = mid; lo = mid + 1; }
            else { hi = mid - 1; }
        }
        if (ans < 0) return Collections.emptyList();
        // walk around ans for any intervals that include code
        List<Integer> hits = new ArrayList<>();
        for (int j = Math.max(0, ans - 1); j <= Math.min(I.size() - 1, ans + 1); j++) {
            if (cmp.compare(I.get(j).lower, code) <= 0 && cmp.compare(code, I.get(j).upper) <= 0) {
                hits.add(j);
            }
        }
        return hits.isEmpty() ? List.of(Math.max(0, Math.min(ans, I.size() - 1))) : hits;
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

    private boolean isBuilt(DimensionState S) {
        return !S.divisions.isEmpty() && !S.divisions.get(0).I.isEmpty();
    }

    /**
     * Build all divisions using only staged codes (no plaintext).
     * Uses Coding.GMeta (seed-only) so we can persist metadata deterministically.
     */
    private void buildAllDivisions(DimensionState S) {
        // Collect staged items that have codes for all divisions
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

            // Metadata only (no plaintext projections on server)
            Coding.GMeta G = Coding.fromSeedOnly(m, lambda, seed);

            List<GreedyPartitioner.Item> items = new ArrayList<>(pts.size());
            for (int i = 0; i < pts.size(); i++) {
                items.add(new GreedyPartitioner.Item(pts.get(i).getId(), pts.get(i), codes.get(i)[divIdx]));
            }

            GreedyPartitioner.BuildResult br = GreedyPartitioner.build(items, G.codeBits(), seed ^ 0x9E3779B97F4A7C15L);

            DivisionState D = new DivisionState(G); // change DivisionState.G type to accept GMeta
            D.I = br.indexI;

            Map<String, List<EncryptedPoint>> safe = new ConcurrentHashMap<>();
            for (var e : br.tagToSubset.entrySet()) {
                safe.put(e.getKey(), new java.util.concurrent.CopyOnWriteArrayList<>(e.getValue()));
            }
            D.tagToSubset = safe;

            D.w = Math.max(1, (int) Math.ceil(PAPER_W_ALPHA * pts.size() / (double) this.divisions));
            logger.info("Division {} paper-w target set to {} (alpha={} * |D|={} / ℓ={})",
                    (divIdx + 1), D.w, PAPER_W_ALPHA, pts.size(), this.divisions);

            S.divisions.add(D);
        }

        // SECURITY: clear staged references
        S.staged.clear();
        S.stagedCodes.clear();
    }

    private static long mix(long seedBase, int d, int m, int lambda, int division) {
        long x = seedBase;
        x ^= ((long)d * 0xBF58476D1CE4E5B9L);
        x ^= ((long)m * 0x94D049BB133111EBL);
        x ^= ((long)lambda * 0x2545F4914F6CDD1DL);
        x ^= ((long)division * 0xD1342543DE82EF95L);
        x ^= (x >>> 33); x *= 0xff51afd7ed558ccdl;
        x ^= (x >>> 33); x *= 0xc4ceb9fe1a85ec53l;
        x ^= (x >>> 33);
        return x;
    }

    private static int hammingGap(BitSet a, BitSet b) {
        if (a == null || b == null) return Integer.MAX_VALUE;
        BitSet x = (BitSet) a.clone();
        x.xor(b);
        return x.cardinality();
    }

    private static int nearestIdx(BitSet Cq, List<GreedyPartitioner.SubsetBounds> I) {
        if (I == null || I.isEmpty()) return -1;
        int best = -1, bestGap = Integer.MAX_VALUE;
        for (int j = 0; j < I.size(); j++) {
            GreedyPartitioner.SubsetBounds sb = I.get(j);
            // use lower as a stable representative; fallback to upper if needed
            BitSet rep = (sb.lower != null) ? sb.lower : sb.upper;
            int gap = hammingGap(Cq, rep); // |Cq XOR rep|
            if (gap < bestGap) { bestGap = gap; best = j; }
        }
        return best;
    }

    // Legacy-only adapter: returns a GFunction if (and only if) the division stored one.
    // In production (seed-only GMeta), this returns null so we do NOT compute codes server-side.
    private static Coding.GFunction toLegacyGFunction(Coding.CodeFamily g) {
        return (g instanceof Coding.GFunction) ? (Coding.GFunction) g : null;
    }

    // Expose paper params for logging/artifacts
    public int getM() { return m; }
    public int getLambda() { return lambda; }
    public int getDivisions() { return divisions; }
    public long getSeedBase() { return seedBase; }
    private static double clampAlpha(double x) {
        // keep sane & safe bounds to prevent massive candidate leaks
        if (Double.isNaN(x)) return 0.03;
        if (x < 0.005) return 0.005;   // 0.5%
        if (x > 0.10)  return 0.10;    // 10%
        return x;
    }
}
