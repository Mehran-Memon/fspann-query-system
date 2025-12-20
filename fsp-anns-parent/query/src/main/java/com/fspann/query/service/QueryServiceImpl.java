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

        logger.info("QueryServiceImpl: partitioned/Peng mode (L2 + D1 limiter)");
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

        // --------------------------
        // 1) Decrypt query (client)
        // --------------------------
        KeyVersion kv;
        try {
            kv = keyService.getVersion(token.getVersion());
        } catch (Throwable ignore) {
            kv = keyService.getCurrentVersion();
        }

        final double[] qVec = cryptoService.decryptQuery(
                token.getEncryptedQuery(), token.getIv(), kv.getKey());

        if (!isValid(qVec)) {
            logger.warn("Query decryption failed or invalid vector; aborting search");
            lastClientNs = Math.max(0L, System.nanoTime() - clientStart);
            return Collections.emptyList();
        }

        // --------------------------
        // 2) Server-side path
        // --------------------------
        final long serverStart = System.nanoTime();
        List<QueryResult> finalResults = Collections.emptyList();

        try {
            // 2.1 raw candidates from partitioned index
            List<EncryptedPoint> raw;

            if (cfg.getSearchMode() == com.fspann.config.SearchMode.PAPER_BASELINE) {

                // ===== PAPER BASELINE: SINGLE LOOKUP =====
                raw = index.lookup(token);
                raw = (raw != null) ? raw : Collections.emptyList();
                lastCandTotal = index.getLastTouchedCount();

            } else {

                // ===== OPTIMIZED MODE =====
                SystemConfig.StabilizationConfig sc = cfg.getStabilization();
                int minCand = (sc != null && sc.isEnabled()) ? sc.getMinCandidates() : 0;

                int baseProbe = cfg.getPaper().probeLimit;
                int probe = baseProbe;
                int attempts = 0;
                final int MAX_ATTEMPTS = 4;

                raw = Collections.emptyList();

//                while (attempts == 0 || (raw.size() < minCand && attempts < MAX_ATTEMPTS)) {
                while (attempts < MAX_ATTEMPTS) {

                    index.setProbeOverride(probe);
                    raw = index.lookup(token);
                    raw = (raw != null) ? raw : Collections.emptyList();

                    if (raw.size() >= minCand) break;

                    probe *= 2;
                    attempts++;
                }

                index.clearProbeOverride();
                lastCandTotal = raw.size();

                if (raw.size() < minCand) {
                    logger.warn(
                            "minCandidates={} not reached (got {}). Final probeLimit={}",
                            minCand, raw.size(), probe
                    );
                }
            }

            if (raw.isEmpty()) {
                lastCandKept = 0;
                return Collections.emptyList();
            }

            List<EncryptedPoint> limited = applyCandidateLimiter(raw);
            lastCandKept = limited.size();

            if (limited.isEmpty()) {
                return Collections.emptyList();
            }

            touchedThisSession.addAll(index.getLastTouchedIds());

            // 2.3 decrypt + score (L2)
            final long decStart = System.nanoTime();
            List<QueryScored> scored = decryptAndScore(limited, qVec, kv);
            final long decEnd = System.nanoTime();
            lastDecryptNs = Math.max(0L, decEnd - decStart);

            if (scored.isEmpty()) {
                return Collections.emptyList();
            }

            // sort by L2 ascending
            scored.sort(Comparator.comparingDouble(QueryScored::dist));

            // 2.4 top-K cut
            int k = token.getTopK();
            int eff = Math.min(k, scored.size());
            lastReturned = eff;

            List<QueryResult> out = new ArrayList<>(eff);
            for (int i = 0; i < eff; i++) {
                QueryScored qs = scored.get(i);
                out.add(new QueryResult(qs.id(), qs.dist()));
            }
            finalResults = out;
            return out;

        } finally {
        long serverEnd = System.nanoTime();
        lastServerNs = Math.max(0L, serverEnd - serverStart);

        long clientEnd = System.nanoTime();
        long totalNs = Math.max(0L, clientEnd - clientStart);

        // client-only = total - server
        long clientOnlyNs = Math.max(0L, totalNs - lastServerNs);
        lastClientNs = clientOnlyNs;

        lastCandIds = new HashSet<>(touchedThisSession);
        if (reencTracker != null && !touchedThisSession.isEmpty()) {
            reencTracker.record(touchedThisSession);
        }
    }
}

    // =====================================================================
    // CANDIDATE LIMITER (D1-like) ON ENCRYPTED POINTS
    // =====================================================================

    /**
     * Apply D1-style stabilization limiter on candidate list.
     *
     * raw       = |candidates|
     * alphaCap  = ceil(alpha * raw)
     * finalSize = max(minCandidates, min(alphaCap, raw))
     *
     * We take the first finalSize points in the natural arrival order
     * from the partitioned index.
     */
    private List<EncryptedPoint> applyCandidateLimiter(List<EncryptedPoint> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            if (stabilizationCallback != null) {
                stabilizationCallback.accept(0, 0);
            }
            return Collections.emptyList();
        }

        // ================= PAPER BASELINE =================
        if (cfg.getSearchMode() == com.fspann.config.SearchMode.PAPER_BASELINE) {
            if (stabilizationCallback != null) {
                stabilizationCallback.accept(candidates.size(), candidates.size());
            }
            return candidates;   // no truncation
        }

        // ================= OPTIMIZED MODE =================
        SystemConfig.StabilizationConfig sc = cfg.getStabilization();
        if (sc == null || !sc.isEnabled()) {
            if (stabilizationCallback != null) {
                stabilizationCallback.accept(candidates.size(), candidates.size());
            }
            return candidates;
        }

        int uniqueCandidates = candidates.size();
        int minCand = sc.getMinCandidates();
        double alpha = sc.getAlpha();

        int alphaCap = (int) Math.ceil(alpha * uniqueCandidates);
        int finalSize = Math.min(uniqueCandidates, Math.max(minCand, alphaCap));

        if (stabilizationCallback != null) {
            stabilizationCallback.accept(uniqueCandidates, finalSize);
        }

        return candidates.subList(0, finalSize);
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
