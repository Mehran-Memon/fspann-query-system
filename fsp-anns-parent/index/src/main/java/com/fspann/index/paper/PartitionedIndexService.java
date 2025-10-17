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
    private static final Logger logger = LoggerFactory.getLogger(PartitionedIndexService.class);

    public PartitionedIndexService(int m, int lambda, int divisions, long seedBase) {
        this(m, lambda, divisions, seedBase,
                Integer.getInteger("paper.buildThreshold", 2_000),
                Integer.getInteger("paper.maxCandidates", -1)); // 0 means disabled
    }

    public PartitionedIndexService(int m, int lambda, int divisions, long seedBase, int buildThreshold, int maxCandidates) {
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
        final Coding.GFunction G;
        List<GreedyPartitioner.SubsetBounds> I = Collections.emptyList(); // sorted
        Map<String, List<EncryptedPoint>> tagToSubset = new ConcurrentHashMap<>();
        int w = 0;
        DivisionState(Coding.GFunction g) { this.G = g; }
    }

    private static final class DimensionState {
        final int d;
        // staged points (before first build)
        final List<EncryptedPoint> staged = new ArrayList<>();
        final List<double[]> stagedVectors = new ArrayList<>();
        // built divisions
        final List<DivisionState> divisions = new ArrayList<>();
        DimensionState(int d) { this.d = d; }
    }

    // dimension -> state
    private final Map<Integer, DimensionState> dims = new ConcurrentHashMap<>();
    // quick access by point id (for delete)
    private final Map<String, Integer> idToDim = new ConcurrentHashMap<>();

    // --------------------------------------------------------------------
    // PaperSearchEngine API
    // --------------------------------------------------------------------

    @Override
    public void insert(EncryptedPoint pt) {
        // Without plaintext vector, we cannot code; keep staged until caller provides vector later,
        // or drop it if your workflow always calls insert(pt, vector). Here we stage with a null vector.
        Objects.requireNonNull(pt, "EncryptedPoint");
        idToDim.put(pt.getId(), pt.getVectorLength());
        DimensionState S = dims.computeIfAbsent(pt.getVectorLength(), DimensionState::new);
        synchronized (S) {
            S.staged.add(pt);
            S.stagedVectors.add(null); // will be ignored unless vector available for coding
            maybeBuild(S);
        }
    }

    @Override
    public void insert(EncryptedPoint pt, double[] plaintextVector) {
        Objects.requireNonNull(pt, "EncryptedPoint");
        Objects.requireNonNull(plaintextVector, "plaintextVector");
        if (pt.getVectorLength() != plaintextVector.length) {
            throw new IllegalArgumentException("Vector length mismatch for id=" + pt.getId());
        }
        idToDim.put(pt.getId(), pt.getVectorLength());
        DimensionState S = dims.computeIfAbsent(pt.getVectorLength(), DimensionState::new);
        synchronized (S) {
            if (isBuilt(S)) {
                // Fast-path: place directly into existing partitions (no staging)
                for (DivisionState div : S.divisions) {
                    BitSet code = Coding.C(plaintextVector, div.G);
                    List<Integer> hits = findCoveringIntervals(code, div.I);
                    if (hits.isEmpty()) continue;
                    for (int idx : hits) {
                        GreedyPartitioner.SubsetBounds sb = div.I.get(idx);
                        div.tagToSubset
                                .computeIfAbsent(sb.tag, k -> new java.util.concurrent.CopyOnWriteArrayList<>())
                                .add(pt);
                    }
                }
            } else {
                // Pre-build: stage until we have G and I
                S.staged.add(pt);
                S.stagedVectors.add(plaintextVector.clone());
                maybeBuild(S);
            }
        }
    }

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken");
        final int d = token.getDimension();
        final DimensionState S = dims.get(d);
        if (S == null) return Collections.emptyList();

        ensureBuilt(S); // one-time build if needed

        // ---- purely (m, λ)-derived budgets/radii ----
        // per-division targets ensure we don't cap globally or by K
        final int perDivTarget  = Math.max(1, Math.max(m * lambda,  S.divisions.isEmpty() ? 1 : S.divisions.get(0).w));
        final int perDivTarget2 = Math.max(perDivTarget, (int)Math.ceil(1.5 * m * lambda));

        // symmetric widening around the hit interval(s)
        final int maxRadius     = Math.max(1, lambda);       // pass-1
        final int maxRadiusHard = Math.max(maxRadius + 1, 2 * lambda); // pass-2

        final Set<String> seen = new LinkedHashSet<>();
        final List<EncryptedPoint> cands = new ArrayList<>();
        final double[] q = token.getPlaintextQuery();

        // ---------- PASS 1: hits + bounded symmetric expansion ----------
        for (DivisionState div : S.divisions) {
            if (div.I.isEmpty()) continue;
            BitSet Cq = Coding.C(q, div.G);

            List<Integer> hitIdxs = findCoveringIntervals(Cq, div.I);
            if (hitIdxs.isEmpty()) continue;

            int gatheredThisDiv = 0;
            for (int idx : hitIdxs) {
                for (int radius = 0; radius <= maxRadius && gatheredThisDiv < perDivTarget; radius++) {
                    for (int j = idx - radius; j <= idx + radius && gatheredThisDiv < perDivTarget; j++) {
                        if (j < 0 || j >= div.I.size()) continue;
                        GreedyPartitioner.SubsetBounds sb = div.I.get(j);
                        List<EncryptedPoint> subset = div.tagToSubset.get(sb.tag);
                        if (subset == null) continue;

                        for (EncryptedPoint p : subset) {
                            if (seen.add(p.getId())) {
                                cands.add(p);
                                if (++gatheredThisDiv >= perDivTarget) break;
                            }
                        }
                    }
                }
            }
        }

        // ---------- PASS 2: gentle widening (still only (m, λ) driven) ----------
        for (DivisionState div : S.divisions) {
            if (div.I.isEmpty()) continue;
            BitSet Cq = Coding.C(q, div.G);

            List<Integer> hitIdxs = findCoveringIntervals(Cq, div.I);
            if (hitIdxs.isEmpty()) continue;

            int gatheredThisDiv = 0;
            for (int idx : hitIdxs) {
                for (int radius = maxRadius + 1; radius <= maxRadiusHard && gatheredThisDiv < perDivTarget2; radius++) {
                    for (int j = idx - radius; j <= idx + radius && gatheredThisDiv < perDivTarget2; j++) {
                        if (j < 0 || j >= div.I.size()) continue;
                        GreedyPartitioner.SubsetBounds sb = div.I.get(j);
                        List<EncryptedPoint> subset = div.tagToSubset.get(sb.tag);
                        if (subset == null) continue;

                        for (EncryptedPoint p : subset) {
                            if (seen.add(p.getId())) {
                                cands.add(p);
                                if (++gatheredThisDiv >= perDivTarget2) break;
                            }
                        }
                    }
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
                    S.stagedVectors.remove(i);
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

    private void buildAllDivisions(DimensionState S) {
        // Gather only items with available plaintext vectors
        List<EncryptedPoint> pts = new ArrayList<>();
        List<double[]> vecs = new ArrayList<>();
        for (int i = 0; i < S.staged.size(); i++) {
            double[] v = S.stagedVectors.get(i);
            if (v != null) {
                pts.add(S.staged.get(i));
                vecs.add(v);
            }
        }
        if (pts.isEmpty()) return; // nothing to build

        int d = S.d;

        // Reset divisions
        S.divisions.clear();

        // For each division, build independent G from a sample then partition
        for (int div = 0; div < divisions; div++) {
            long seed = mix(seedBase, d, m, lambda, div);

            // Build G from a sample (use all vecs here; you can subsample for speed)
            Coding.GFunction G = Coding.buildFromSample(vecs.toArray(new double[0][]), m, lambda, seed);
            DivisionState D = new DivisionState(G);

            // Create items with codes
            List<GreedyPartitioner.Item> items = new ArrayList<>(pts.size());
            for (int i = 0; i < pts.size(); i++) {
                items.add(new GreedyPartitioner.Item(pts.get(i).getId(), pts.get(i), Coding.C(vecs.get(i), G)));
            }

            // Build partitions (Algorithm-2)
            GreedyPartitioner.BuildResult br = GreedyPartitioner.build(items, G.codeBits(), seed ^ 0x9E3779B97F4A7C15L);
            D.I = br.indexI;
            // Wrap with thread-safe collections for concurrent reads/writes
            Map<String, List<EncryptedPoint>> safe = new ConcurrentHashMap<>();
            for (var e : br.tagToSubset.entrySet()) {
                safe.put(e.getKey(), new java.util.concurrent.CopyOnWriteArrayList<>(e.getValue()));
            }
            D.tagToSubset = safe;
            D.w = br.w;
            S.divisions.add(D);
        }

        // Clear staged (we have built with these)
        S.staged.clear();
        S.stagedVectors.clear();
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

    // Expose paper params for logging/artifacts
    public int getM() { return m; }
    public int getLambda() { return lambda; }
    public int getDivisions() { return divisions; }
    public long getSeedBase() { return seedBase; }
}
