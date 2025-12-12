package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * QueryServiceImpl (LSH-ONLY VERSION)
 * ===================================
 *
 * Pure LSH query service - single path, clean implementation.
 *
 * Key properties:
 *  • Single indexing engine: MultiTableLSH only
 *  • Query → decrypt → score → return results
 *  • No engine routing, no fallbacks
 *  • Full forward security (stale decrypt allowed)
 *  • Unified metrics collection
 *
 * @author FSP-ANNS Project
 * @version 4.0 (LSH-Only)
 */
public final class QueryServiceImpl implements QueryService {

    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);

    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final QueryTokenFactory tokenFactory;
    private final SystemConfig cfg;

    // LSH index service (required)
    private final SecureLSHIndexService lshService;

    // --- Last search metrics ---
    private volatile long lastQueryDurationNs = 0;
    private volatile long lastClientDurationNs = 0;
    private volatile long lastDecryptNs = 0;
    private volatile int lastCandTotal = 0;
    private volatile int lastCandDecrypted = 0;
    private volatile int lastReturned = 0;
    private volatile List<String> lastCandIds = Collections.emptyList();
    private volatile ReencryptionTracker reencTracker;

    private StabilizationCallback stabilizationCallback;
    private final Set<String> touchedThisSession = ConcurrentHashMap.newKeySet();

    public QueryServiceImpl(
            IndexService indexService,
            CryptoService cryptoService,
            KeyLifeCycleService keyService,
            QueryTokenFactory tf,
            SystemConfig cfg) {

        this.indexService = Objects.requireNonNull(indexService);
        this.cryptoService = Objects.requireNonNull(cryptoService);
        this.keyService = Objects.requireNonNull(keyService);
        this.tokenFactory = tf;
        this.cfg = Objects.requireNonNull(cfg, "cfg");

        // LSH service is required for LSH-only system
        if (indexService instanceof SecureLSHIndexService) {
            this.lshService = (SecureLSHIndexService) indexService;
            logger.info("QueryServiceImpl: LSH-Only mode");
        } else {
            throw new IllegalArgumentException(
                    "LSH-only system requires SecureLSHIndexService");
        }
    }

    public void setReencryptionTracker(ReencryptionTracker tr) {
        this.reencTracker = tr;
    }

    // ====================================================================
    // SEARCH (SINGLE PATH)
    // ====================================================================

    @Override
    public List<QueryResult> search(QueryToken token) {
        if (token == null) return Collections.emptyList();

        clearLastMetrics();
        touchedThisSession.clear();

        final long clientStart = System.nanoTime();

        // Decrypt query
        KeyVersion kv;
        try {
            kv = keyService.getVersion(token.getVersion());
        } catch (Throwable ignore) {
            kv = keyService.getCurrentVersion();
        }

        final double[] qVec = cryptoService.decryptQuery(
                token.getEncryptedQuery(), token.getIv(), kv.getKey());

        if (qVec == null || !isValid(qVec)) {
            return Collections.emptyList();
        }

        final long serverStart = System.nanoTime();
        lastCandIds = new ArrayList<>();

        try {
            // LSH query (only path)
            List<Map.Entry<String, Double>> lshResults = lshService.query(qVec, token.getTopK());

            if (lshResults == null || lshResults.isEmpty()) {
                logger.debug("LSH query returned no results");
                return Collections.emptyList();
            }

            logger.debug("LSH query returned {} candidates", lshResults.size());

            // Convert (vectorId, distance) to EncryptedPoint
            List<EncryptedPoint> candidates = new ArrayList<>();
            EncryptedPointBuffer buffer = indexService.getPointBuffer();

            for (Map.Entry<String, Double> entry : lshResults) {
                String vectorId = entry.getKey();

                EncryptedPoint ep = null;
                try {
                    ep = indexService.getEncryptedPoint(vectorId);
                } catch (Exception e) {
                    logger.warn("Failed to fetch EncryptedPoint: {}", vectorId);
                    continue;
                }

                if (ep != null) {
                    candidates.add(ep);
                }
            }

            if (candidates.isEmpty()) {
                return Collections.emptyList();
            }

            // Deduplicate
            LinkedHashMap<String, EncryptedPoint> uniq = new LinkedHashMap<>();
            for (EncryptedPoint ep : candidates) {
                if (ep == null) continue;
                uniq.putIfAbsent(ep.getId(), ep);
            }

            addCandidateIds(uniq.keySet());
            lastCandTotal = uniq.size();

            // Decrypt + score
            final long decStart = System.nanoTime();
            List<QueryScored> scored = decryptAndScore(uniq, qVec, kv);
            scored = stabilizeScores(scored);
            final long decEnd = System.nanoTime();
            lastDecryptNs = Math.max(0L, decEnd - decStart);

            scored.sort(Comparator.comparingDouble(QueryScored::dist));

            // Top-K cut
            int k = token.getTopK();
            int eff = Math.min(k, scored.size());
            lastReturned = eff;

            List<QueryResult> out = new ArrayList<>(eff);
            for (int i = 0; i < eff; i++) {
                QueryScored qs = scored.get(i);
                out.add(new QueryResult(qs.id(), qs.dist()));
            }

            return out;

        } finally {
            long serverEnd = System.nanoTime();
            lastQueryDurationNs = Math.max(0L, serverEnd - serverStart);

            long clientEnd = System.nanoTime();
            lastClientDurationNs = Math.max(0L, clientEnd - clientStart);
        }
    }

    // ====================================================================
    // DECRYPT + SCORE
    // ====================================================================

    private List<QueryScored> decryptAndScore(
            LinkedHashMap<String, EncryptedPoint> uniq,
            double[] qVec,
            KeyVersion kv) {

        List<QueryScored> out = new ArrayList<>(uniq.size());

        for (EncryptedPoint ep : uniq.values()) {
            if (!touchedThisSession.contains(ep.getId())) {
                touchedThisSession.add(ep.getId());
            }

            double[] vec = cryptoService.decryptFromPoint(ep, kv.getKey());

            if (vec == null || !isValid(vec)) continue;

            lastCandDecrypted++;
            out.add(new QueryScored(ep.getId(), l2sq(qVec, vec)));
        }

        return out;
    }

    // ====================================================================
    // TOP-K VARIANTS
    // ====================================================================

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(
            QueryToken baseToken,
            int queryIndex,
            GroundtruthManager gt) {

        Objects.requireNonNull(baseToken);

        final List<Integer> variants = List.of(1, 5, 10, 20, 40, 60, 80, 100);

        long clientStart = System.nanoTime();
        List<QueryResult> base = nn(search(baseToken));
        long clientEnd = System.nanoTime();

        long clientNs = Math.max(0L, clientEnd - clientStart);
        long serverNs = lastQueryDurationNs;
        long runNs = clientNs + serverNs;

        long clientMs = Math.round(clientNs / 1e6);
        long serverMs = Math.round(serverNs / 1e6);
        long runMs = Math.round(runNs / 1e6);
        long decryptMs = Math.round(lastDecryptNs / 1e6);

        int candTotal = lastCandTotal;
        int candDecrypted = lastCandDecrypted;
        int candReturned = lastReturned;
        int vectorDim = baseToken.getDimension();
        int tokenBytes = prefixTokenBytes(baseToken);

        List<QueryEvaluationResult> out = new ArrayList<>(variants.size());

        for (int k : variants) {
            int upto = Math.min(k, base.size());
            List<QueryResult> prefix = base.subList(0, upto);

            double precision = computePrecision(prefix, gt, queryIndex, k);

            out.add(new QueryEvaluationResult(
                    k, upto, (upto > 0 ? (double) candTotal / upto : 0.0), precision,
                    serverMs, clientMs, runMs, decryptMs,
                    0,
                    candTotal, 0, candDecrypted, candReturned,
                    tokenBytes, vectorDim, 0, 0,
                    0, 0, 0, 0, 0, "lsh",
                    k, k, queryIndex, "full"
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

    // ====================================================================
    // UTILITIES & METRICS
    // ====================================================================

    private record QueryScored(String id, double dist) {}

    private static <T> List<T> nn(List<T> v) {
        return (v == null) ? Collections.emptyList() : v;
    }

    private double l2sq(double[] a, double[] b) {
        int len = Math.min(a.length, b.length);
        double s = 0.0;
        for (int i = 0; i < len; i++) {
            double d = a[i] - b[i];
            s += d * d;
        }
        return s;
    }

    private void clearLastMetrics() {
        lastQueryDurationNs = 0;
        lastClientDurationNs = 0;
        lastDecryptNs = 0;
        lastCandTotal = 0;
        lastCandDecrypted = 0;
        lastReturned = 0;
        lastCandIds = Collections.emptyList();
    }

    private void addCandidateIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) return;

        if (lastCandIds == null || lastCandIds.isEmpty()) {
            lastCandIds = new ArrayList<>(ids);
        } else {
            LinkedHashSet<String> merged = new LinkedHashSet<>(lastCandIds);
            merged.addAll(ids);
            lastCandIds = new ArrayList<>(merged);
        }
    }

    private List<QueryScored> stabilizeScores(List<QueryScored> scored) {
        if (scored == null || scored.isEmpty()) {
            if (stabilizationCallback != null)
                stabilizationCallback.accept(0, 0);
            return scored;
        }

        SystemConfig.StabilizationConfig sc = cfg.getStabilization();
        if (sc == null || !sc.isEnabled()) {
            if (stabilizationCallback != null)
                stabilizationCallback.accept(scored.size(), scored.size());
            return scored;
        }

        int raw = scored.size();
        double alpha = sc.getAlpha();
        int minCandidates = sc.getMinCandidates();

        int alphaCap = (int) Math.ceil(alpha * raw);
        int capped = Math.min(alphaCap, raw);
        int finalSize = Math.max(capped, minCandidates);

        if (finalSize >= raw) {
            if (stabilizationCallback != null)
                stabilizationCallback.accept(raw, raw);
            return scored;
        }

        List<QueryScored> sub = scored.subList(0, finalSize);

        if (stabilizationCallback != null)
            stabilizationCallback.accept(raw, finalSize);

        return sub;
    }

    public void setStabilizationCallback(StabilizationCallback cb) {
        this.stabilizationCallback = cb;
    }

    @FunctionalInterface
    public interface StabilizationCallback {
        void accept(int rawCount, int finalCount);
    }

    @Override
    public List<String> getLastCandidateIds() {
        return (lastCandIds == null)
                ? Collections.emptyList()
                : Collections.unmodifiableList(lastCandIds);
    }

    public long getLastQueryDurationNs() { return lastQueryDurationNs; }
    public int getLastCandTotal() { return lastCandTotal; }
    public int getLastCandDecrypted() { return lastCandDecrypted; }
    public int getLastReturned() { return lastReturned; }
    public long getLastClientDurationNs() { return lastClientDurationNs; }
    public long getLastDecryptNs() { return lastDecryptNs; }

    public double getLastRatio() {
        return (lastReturned > 0) ? (double) lastCandTotal / lastReturned : 0.0;
    }

    private int prefixTokenBytes(QueryToken t) {
        if (t == null) return 0;
        int iv = (t.getIv() != null ? t.getIv().length : 0);
        int ct = (t.getEncryptedQuery() != null ? t.getEncryptedQuery().length : 0);
        return iv + ct;
    }

    private static boolean isValid(double[] v) {
        if (v == null) return false;
        for (double x : v) {
            if (!Double.isFinite(x)) return false;
        }
        return true;
    }

    public Set<String> getTouchedIds() {
        return new HashSet<>(touchedThisSession);
    }
}