package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private volatile List<String> lastCandIds = Collections.emptyList();

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
            List<EncryptedPoint> raw = index.lookup(token);
            raw = (raw != null) ? raw : Collections.emptyList();

            lastCandTotal = raw.size();
            lastCandIds = raw.stream().map(EncryptedPoint::getId).toList();

            if (raw.isEmpty()) {
                return Collections.emptyList();
            }

            // 2.2 D1-style limiter (stabilization) on EncryptedPoints
            List<EncryptedPoint> limited = applyCandidateLimiter(raw);
            lastCandKept = limited.size();
            if (limited.isEmpty()) {
                return Collections.emptyList();
            }

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
            lastClientNs = Math.max(0L, clientEnd - clientStart);

            // update reencryption tracker once per query
            if (reencTracker != null && !touchedThisSession.isEmpty()) {
                reencTracker.record(touchedThisSession);
            }
        }
    }

    // =====================================================================
    // TOP-K VARIANTS (1,5,10,20,40,60,80,100)
    // =====================================================================

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(
            QueryToken baseToken,
            int queryIndex,
            GroundtruthManager gt
    ) {
        Objects.requireNonNull(baseToken, "baseToken");

        List<Integer> variants = List.of(1, 5, 10, 20, 40, 60, 80, 100);

        // Single search run with max-K token
        List<QueryResult> base = nn(search(baseToken));

        // Per Peng:
        // serverMs = server-only
        // clientMs = end-to-end
        // runMs    = clientMs (no double counting)
        long serverMs  = Math.round(lastServerNs  / 1e6);
        long clientMs  = Math.round(lastClientNs  / 1e6);
        long runMs     = clientMs;
        long decryptMs = Math.round(lastDecryptNs / 1e6);

        int candTotal     = lastCandTotal;
        int candKept      = lastCandKept;
        int candDecrypted = lastCandDecrypted;
        int candReturned  = lastReturned;

        int vectorDim  = baseToken.getDimension();
        int tokenBytes = prefixTokenBytes(baseToken);
        int touchedCnt = touchedThisSession.size();

        List<QueryEvaluationResult> out = new ArrayList<>(variants.size());

        for (int k : variants) {
            int upto = Math.min(k, base.size());
            List<QueryResult> prefix = base.subList(0, upto);

            double precision = computePrecision(prefix, gt, queryIndex, k);

            // Peng: ratio@K = # refined / K
            double ratio = (k > 0)
                    ? (double) candDecrypted / (double) k
                    : 0.0;

            out.add(new QueryEvaluationResult(
                    k,                       // topKRequested
                    upto,                    // retrieved
                    ratio,                   // ratio
                    precision,               // precision
                    serverMs,                // serverMs
                    clientMs,                // clientMs
                    runMs,                   // runMs
                    decryptMs,               // decryptMs
                    0L,                      // insertTimeMs (N/A here)
                    candTotal,               // candTotal
                    candKept,                // candKept
                    candDecrypted,           // candDecrypted
                    candReturned,            // candReturned
                    tokenBytes,              // tokenSizeBytes
                    vectorDim,               // vectorDim
                    0,                       // totalFlushedPoints
                    0,                       // flushThreshold
                    touchedCnt,              // touchedCount
                    0,                       // reencryptedCount
                    0L,                      // reencTimeMs
                    0L,                      // reencBytesDelta
                    0L,                      // reencBytesAfter
                    "l2-refine",             // ratioDenomSource
                    k,                       // tokenK
                    k,                       // tokenKBase
                    queryIndex,              // qIndexZeroBased
                    "full"                   // candMetricsMode
            ));
        }

        return out;
    }

    private double computePrecision(List<QueryResult> prefix,
                                    GroundtruthManager gt,
                                    int qIndex,
                                    int k) {

        if (gt == null || k <= 0 || prefix.isEmpty()) return 0.0;

        int[] g = gt.getGroundtruth(qIndex, k);
        if (g == null || g.length == 0) return 0.0;

        Set<String> truth = Arrays.stream(g)
                .mapToObj(String::valueOf)
                .collect(Collectors.toSet());

        int hits = 0;
        for (QueryResult qr : prefix) {
            if (truth.contains(qr.getId())) hits++;
        }

        return hits / (double) k;
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

        SystemConfig.StabilizationConfig sc = cfg.getStabilization();
        if (sc == null || !sc.isEnabled()) {
            if (stabilizationCallback != null) {
                stabilizationCallback.accept(candidates.size(), candidates.size());
            }
            return candidates;
        }

        int raw = candidates.size();
        double alpha = sc.getAlpha();      // e.g. 0.01–0.05
        int minCand = sc.getMinCandidates();

        int alphaCap = (int) Math.ceil(alpha * raw);
        int capped   = Math.min(alphaCap, raw);
        int finalSize = Math.max(minCand, capped);

        if (finalSize >= raw) {
            if (stabilizationCallback != null) {
                stabilizationCallback.accept(raw, raw);
            }
            return candidates;
        }

        List<EncryptedPoint> sub = candidates.subList(0, finalSize);

        if (stabilizationCallback != null) {
            stabilizationCallback.accept(raw, finalSize);
        }

        return sub;
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

        for (EncryptedPoint ep : points) {
            if (ep == null) continue;
            touchedThisSession.add(ep.getId());

            double[] v = cryptoService.decryptFromPoint(ep, kv.getKey());
            if (!isValid(v)) continue;

            lastCandDecrypted++;
            out.add(new QueryScored(ep.getId(), l2(qVec, v)));
        }

        return out;
    }

    // true Euclidean L2
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
        lastCandIds = Collections.emptyList();
    }

    private int prefixTokenBytes(QueryToken t) {
        if (t == null) return 0;
        int iv = (t.getIv() != null) ? t.getIv().length : 0;
        int ct = (t.getEncryptedQuery() != null) ? t.getEncryptedQuery().length : 0;
        return iv + ct;
    }

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
    public int  getLastCandDecrypted() { return lastCandDecrypted; }
    public int  getLastReturned() { return lastReturned; }

    public double getLastRatio() {
        return (lastReturned > 0) ? (double) lastCandDecrypted / lastReturned : 0.0;
    }

    public Set<String> getTouchedIds() {
        return new HashSet<>(touchedThisSession);
    }

    @Override
    public List<String> getLastCandidateIds() {
        return (lastCandIds == null)
                ? Collections.emptyList()
                : Collections.unmodifiableList(lastCandIds);
    }
}
