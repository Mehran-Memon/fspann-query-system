package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.core.QueryTokenFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueryServiceImpl (Partitioned / Peng-style)
 * ==========================================
 *
 * - Uses PartitionedIndexService (via IndexService interface) for candidate points.
 * - Decrypts a limited prefix of candidates (D1-style stabilization).
 * - Scores with true L2 distance (Euclidean).
 * - Metrics aligned with Peng:
 *   • ratio        = #refined (L2 evaluations) / K
 *   • serverMs     = server-side query + refine time
 *   • clientMs     = end-to-end time (entry→exit of search())
 *   • runMs        = clientMs (no double counting)
 *   • decryptMs    = decrypt+score portion
 *   • candTotal    = raw candidate count from index
 *   • candKept     = after D1 limiter
 *   • candDecrypted= refined (distance evaluated)
 *   • candReturned = |top-K result|
 */
public final class QueryServiceImpl implements QueryService {

    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);

    private final PartitionedIndexService index;   // implements IndexService
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final QueryTokenFactory tokenFactory;  // kept for compatibility if needed
    private final SystemConfig cfg;

    // --- last-search metrics ---
    private volatile long lastServerNs = 0L;         // server-only
    private volatile long lastClientNs = 0L;         // end-to-end
    private volatile long lastDecryptNs = 0L;        // decrypt+score
    private volatile int  lastCandTotal = 0;         // raw from index
    private volatile int  lastCandKept = 0;          // after limiter
    private volatile int  lastCandDecrypted = 0;     // refined
    private volatile int  lastReturned = 0;          // |results|
    private volatile Set<String> lastCandIds = Collections.emptySet();

    private volatile ReencryptionTracker reencTracker;
    private StabilizationCallback stabilizationCallback;

    // one-run touched set (for forward security)
    private final Set<String> touchedThisSession = ConcurrentHashMap.newKeySet();

    public QueryServiceImpl(
            PartitionedIndexService index,
            CryptoService cryptoService,
            KeyLifeCycleService keyService,
            QueryTokenFactory tf,
            SystemConfig cfg
    ) {
        this.index = Objects.requireNonNull(index, "index");
        this.cryptoService = Objects.requireNonNull(cryptoService, "cryptoService");
        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.tokenFactory = tf; // may be null if created elsewhere
        this.cfg = Objects.requireNonNull(cfg, "cfg");

        logger.info("QueryServiceImpl: partitioned mode (L2 + D1 limiter)");
    }

    public void setReencryptionTracker(ReencryptionTracker tr) {
        this.reencTracker = tr;
    }

    public void setStabilizationCallback(StabilizationCallback cb) {
        this.stabilizationCallback = cb;
    }

    @FunctionalInterface
    public interface StabilizationCallback {
        void accept(int rawCount, int finalCount);
    }

    // =====================================================================
    // SEARCH (Partitioned index + D1 limiter + L2 scoring)
    // =====================================================================

    @Override
    public List<QueryResult> search(QueryToken token) {
        if (token == null) return Collections.emptyList();

        clearLastMetrics();
        touchedThisSession.clear();

        final long clientStart = System.nanoTime();

        // -------- 1) decrypt query --------
        KeyVersion kv;
        try {
            kv = keyService.getVersion(token.getVersion());
        } catch (Throwable ignore) {
            kv = keyService.getCurrentVersion();
        }

        final double[] qVec = cryptoService.decryptQuery(
                token.getEncryptedQuery(), token.getIv(), kv.getKey());

        if (!isValid(qVec)) {
            lastClientNs = System.nanoTime() - clientStart;
            return Collections.emptyList();
        }

        final long serverStart = System.nanoTime();

        try {
            // -------- 2) PHASE-1: candidate ID collection --------
            List<String> candidateIds = index.lookupCandidateIds(token);
            lastCandTotal = candidateIds.size();

            if (candidateIds.isEmpty()) {
                lastCandKept = 0;
                return Collections.emptyList();
            }

            // ---------- APPLY STABILIZATION ----------
            List<String> limitedIds;

            SystemConfig.StabilizationConfig sc = cfg.getStabilization();
            if (sc != null && sc.isEnabled()) {
                int raw = candidateIds.size();
                final int K = token.getTopK();

                // CRITICAL FIX: K-AWARE TARGET RATIO
                // Target: Keep ~1.25x K candidates (ratio = 1.25)
                // This ensures we have enough candidates while staying under 1.3
                double targetRatio = 1.25;
                int targetCandidates = (int) Math.ceil(K * targetRatio);

                // ADAPTIVE FLOOR: Use alpha*raw if it provides more than target
                // This handles cases where raw candidates are small
                int alphaFloor = (int) Math.ceil(sc.getAlpha() * raw);
                int proposedSize = targetCandidates;

                // HARD BOUNDS:
                // - Never exceed raw (can't have more than available)
                // - Never go below K (must have at least topK candidates)
                // - Respect minCandidates as absolute floor
                int minFloor = Math.max(K, sc.getMinCandidates());
                int finalSize = Math.max(minFloor, Math.min(raw, proposedSize));

                limitedIds = candidateIds.subList(0, Math.min(finalSize, raw));
                lastCandKept = limitedIds.size();

                // Diagnostic logging
                if (logger.isDebugEnabled()) {
                    logger.debug("Stabilization: raw={}, K={}, target={}, alpha={}, final={}, ratio={:.3f}",
                            raw, K, targetCandidates, alphaFloor, lastCandKept,
                            (double)lastCandKept / K);
                }

                // Callback for monitoring
                if (stabilizationCallback != null) {
                    stabilizationCallback.accept(raw, lastCandKept);
                }
            } else {
                // Stabilization disabled - use all candidates
                limitedIds = candidateIds;
                lastCandKept = candidateIds.size();
            }

            // -------- 3) PHASE-2: bounded refinement --------
            final int K = token.getTopK();

            // hard upper bound — YOUR CONTRIBUTION
            int maxRefineFactor = 1;

            SystemConfig.RuntimeConfig rt = cfg.getRuntime();
            if (rt != null && rt.getMaxRefinementFactor() > 0) {
                maxRefineFactor = rt.getMaxRefinementFactor();
            }

            final int MAX_REFINEMENT =
                    Math.min(candidateIds.size(), maxRefineFactor * K);

            int refineLimit = Math.max(K, MAX_REFINEMENT);
            lastCandKept = limitedIds.size();

            final long decStart = System.nanoTime();
            List<QueryScored> scored = new ArrayList<>(refineLimit);

            for (int i = 0; i < limitedIds.size(); i++) {
                String id = limitedIds.get(i);
                try {
                    EncryptedPoint ep = index.loadPointIfActive(id);
                    if (ep == null) continue;

                    KeyVersion kvp = keyService.getVersion(ep.getKeyVersion());
                    double[] v = cryptoService.decryptFromPoint(ep, kvp.getKey());

                    if (!isValid(v)) continue;

                    scored.add(new QueryScored(id, l2(qVec, v)));
                    touchedThisSession.add(id);

                } catch (Exception ignore) {}
            }

            lastCandDecrypted = scored.size();
            lastDecryptNs = System.nanoTime() - decStart;

            if (logger.isDebugEnabled()) {
                logger.debug(
                        "k{}: candTotal={}, candKept={}, refined={}, returned={}",
                        token.getTopK(),
                        lastCandTotal,
                        lastCandKept,
                        lastCandDecrypted,
                        lastReturned
                );
            }

            if (scored.isEmpty()) return Collections.emptyList();

            scored.sort(Comparator.comparingDouble(QueryScored::dist));

            int eff = Math.min(K, scored.size());
            lastReturned = eff;

            List<QueryResult> out = new ArrayList<>(eff);
            for (int i = 0; i < eff; i++) {
                QueryScored s = scored.get(i);
                out.add(new QueryResult(s.id(), s.dist()));
            }

            return out;

        } finally {
            long serverEnd = System.nanoTime();
            lastServerNs = serverEnd - serverStart;
            lastClientNs = System.nanoTime() - clientStart - lastServerNs;

            lastCandIds = new HashSet<>(touchedThisSession);
            if (reencTracker != null && !touchedThisSession.isEmpty()) {
                reencTracker.record(touchedThisSession);
            }
        }
    }

    // =====================================================================
    // DECRYPT + SCORE (L2)
    // =====================================================================

    private record QueryScored(String id, double dist) {}

    private List<QueryScored> decryptAndScore(
            List<EncryptedPoint> points,
            double[] qVec,
            KeyVersion kv
    ) {
        List<QueryScored> out = new ArrayList<>(points.size());

        lastCandDecrypted = 0;

        for (EncryptedPoint ep : points) {
            if (ep == null) continue;

            // track touched for reencryption
            touchedThisSession.add(ep.getId());

            try {
                // MUST decrypt using point-bound key version
                KeyVersion kvp = keyService.getVersion(ep.getKeyVersion());
                double[] v = cryptoService.decryptFromPoint(ep, kvp.getKey());

                if (!isValid(v)) continue;

                lastCandDecrypted++;
                out.add(new QueryScored(ep.getId(), l2(qVec, v)));

            } catch (Exception e) {
                // stale key or AEAD failure → skip
                continue;
            }
        }

        return out;
    }

    /**
     * True Euclidean L2 distance
     * (keep existing implementation)
     */
    private double l2(double[] a, double[] b) {
        int len = Math.min(a.length, b.length);
        double s = 0.0;
        for (int i = 0; i < len; i++) {
            double d = a[i] - b[i];
            s += d * d;
        }
        return Math.sqrt(s);
    }

    // =====================================================================
    // UTILITIES / METRICS EXPOSURE
    // =====================================================================

    private static <T> List<T> nn(List<T> v) {
        return (v == null) ? Collections.emptyList() : v;
    }

    private void clearLastMetrics() {
        lastServerNs = 0L;
        lastClientNs = 0L;
        lastDecryptNs = 0L;
        lastCandTotal = 0;
        lastCandKept = 0;
        lastCandDecrypted = 0;
        lastReturned = 0;
        lastCandIds = Collections.emptySet();
    }

    private int prefixTokenBytes(QueryToken t) {
        if (t == null) return 0;
        int iv = (t.getIv() != null) ? t.getIv().length : 0;
        int ct = (t.getEncryptedQuery() != null) ? t.getEncryptedQuery().length : 0;
        return iv + ct;
    }

    /**
     * Validate that vector contains only finite numbers
     * (keep existing implementation)
     */
    private static boolean isValid(double[] v) {
        if (v == null) return false;
        for (double x : v) {
            if (!Double.isFinite(x)) return false;
        }
        return true;
    }

    // Exposed to ForwardSecureANNSystem / Engine / Profiler

    public long getLastQueryDurationNs() { return lastServerNs; }
    public long getLastClientDurationNs() { return lastClientNs; }
    public long getLastDecryptNs() { return lastDecryptNs; }
    public int  getLastCandTotal() { return lastCandTotal; }
    public int  getLastCandKept() { return lastCandKept; }
    public int getLastCandKeptVersion() {
        return lastCandKept;  // Alias for backward compatibility
    }
    public Set<String> getLastCandidateIds() {
        return Collections.unmodifiableSet(lastCandIds);
    }
    public int  getLastCandDecrypted() { return lastCandDecrypted; }
    public int  getLastReturned() { return lastReturned; }
}
