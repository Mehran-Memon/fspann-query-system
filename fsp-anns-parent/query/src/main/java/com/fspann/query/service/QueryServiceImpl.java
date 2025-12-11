package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * QueryServiceImpl
 * ----------------------------------------
 * Requirements:
 *  • PartitionedIndexService only (paper engine)
 *  • No LSH fallback, no dimension fallback
 *  • Full forward security (stale decrypt allowed)
 *  • Selective touch rules enforced
 *  • K-adaptive works only when explicitly enabled
 *  • Deterministic union semantics
 */
public final class QueryServiceImpl implements QueryService {

    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);
    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final QueryTokenFactory tokenFactory;

    // --- Last search metrics ---
    private volatile long lastQueryDurationNs = 0;   // server window
    private volatile long lastClientDurationNs = 0;  // wall-clock client window
    private volatile long lastDecryptNs = 0;         // pure decrypt+score
    private volatile long lastRunNs = 0;             // end-to-end (for variant)
    private volatile int  lastCandTotal = 0;
    private volatile int  lastCandKeptVersion = 0;
    private volatile int  lastCandDecrypted = 0;
    private volatile int  lastReturned = 0;
    private volatile List<String> lastCandIds = Collections.emptyList();
    private volatile ReencryptionTracker reencTracker;
    private volatile List<String> lastStabilizedCandIds = Collections.emptyList();

    private StabilizationCallback stabilizationCallback;
    // Forward security touch set for this query
    private final Set<String> touchedThisSession = ConcurrentHashMap.newKeySet();
    private volatile int lastTouchedUniqueSoFar = 0;
    private volatile int lastTouchedCumulativeUnique = 0;

    private final SystemConfig cfg;

    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService,
                            QueryTokenFactory tf,
                            SystemConfig cfg) {
        this.indexService = Objects.requireNonNull(indexService);
        this.cryptoService = Objects.requireNonNull(cryptoService);
        this.keyService = Objects.requireNonNull(keyService);
        this.tokenFactory = tf;
        this.cfg = Objects.requireNonNull(cfg, "cfg");  // NEW
    }

    public void setReencryptionTracker(ReencryptionTracker tr) {
        this.reencTracker = tr;
    }

    // ----------------------------------------------------------------------
    // SEARCH (core)
    // ----------------------------------------------------------------------

    @Override
    public List<QueryResult> search(QueryToken token) {
        if (token == null) return Collections.emptyList();

        clearLastMetrics();
        touchedThisSession.clear();

        final long clientStart = System.nanoTime();

        KeyVersion kv;
        try {
            kv = keyService.getVersion(token.getVersion());
        } catch (Throwable ignore) {
            kv = keyService.getCurrentVersion();
        }

        // decrypt query vector
        final double[] qVec = cryptoService.decryptQuery(
                token.getEncryptedQuery(), token.getIv(), kv.getKey());
        if (qVec == null || !isValid(qVec)) {
            return Collections.emptyList();
        }

        final long serverStart = System.nanoTime();
        lastCandIds = new ArrayList<>();

        try {
            // 1) PartitionedIndexService lookup
            List<EncryptedPoint> raw = safeLookup(token);

            LinkedHashMap<String, EncryptedPoint> uniq = new LinkedHashMap<>();
            for (EncryptedPoint ep : raw) {
                if (ep == null) continue;
                uniq.putIfAbsent(ep.getId(), ep);
            }

            addCandidateIds(uniq.keySet());
            lastCandTotal = uniq.size();

            // 2) Decrypt + score
            final long decStart = System.nanoTime();
            List<QueryScored> scored = decryptAndScore(uniq, qVec, kv);
            scored = stabilizeScores(scored);
            final long decEnd = System.nanoTime();
            lastDecryptNs = Math.max(0L, decEnd - decStart);

            scored.sort(Comparator.comparingDouble(QueryScored::dist));

            // UPDATE: Extract stabilized candidate IDs
            List<String> stabilizedIds = new ArrayList<>();
            for (QueryScored qs : scored) {
                stabilizedIds.add(qs.id());
            }
            lastStabilizedCandIds = Collections.unmodifiableList(stabilizedIds);

            // 3) Optional K-adaptive
            List<QueryScored> finalScored = kAdaptive(scored, uniq, token, qVec, kv);
            finalScored = stabilizeScores(finalScored);
            finalScored.sort(Comparator.comparingDouble(QueryScored::dist));

            // 4) Final top-K cut
            int k = token.getTopK();
            int eff = Math.min(k, finalScored.size());
            lastReturned = eff;

            List<QueryResult> out = new ArrayList<>(eff);
            for (int i = 0; i < eff; i++) {
                QueryScored qs = finalScored.get(i);
                out.add(new QueryResult(qs.id(), qs.dist()));
            }

            return out;

        } finally {
            long serverEnd = System.nanoTime();
            lastQueryDurationNs = Math.max(0L, serverEnd - serverStart);

            long clientEnd = System.nanoTime();
            lastClientDurationNs = Math.max(0L, clientEnd - clientStart);
            lastRunNs = lastClientDurationNs;
        }
    }


    private List<EncryptedPoint> safeLookup(QueryToken token) {
        try {
            List<EncryptedPoint> r = indexService.lookup(token);
            return (r == null) ? Collections.emptyList() : r;
        } catch (Throwable t) {
            logger.error("Error during index lookup: {}", t.getMessage());
            return Collections.emptyList();
        }
    }

    // ----------------------------------------------------------------------
    // Decrypt + score + forward security logic
    // ----------------------------------------------------------------------

    private List<QueryScored> decryptAndScore(
            LinkedHashMap<String, EncryptedPoint> uniq,
            double[] qVec,
            KeyVersion kv)
    {
        List<QueryScored> out = new ArrayList<>(uniq.size());

        for (EncryptedPoint ep : uniq.values()) {
            if (!touchedThisSession.contains(ep.getId())) {
                touchedThisSession.add(ep.getId());
            }

            logger.debug("Decrypting EncryptedPoint: id={}, version={}, iv={}", ep.getId(), ep.getVersion(), Arrays.toString(ep.getIv())); // Log decryption details
            double[] vec = cryptoService.decryptFromPoint(ep, kv.getKey());

            // Ensure no null or invalid vectors are processed
            if (vec == null || !isValid(vec)) continue;

            lastCandKeptVersion++;
            lastCandDecrypted++;

            out.add(new QueryScored(ep.getId(), l2sq(qVec, vec)));
        }

        lastTouchedUniqueSoFar = touchedThisSession.size();
        return out;
    }


    // ----------------------------------------------------------------------
    // K-ADAPTIVE (Option-C)
    // ----------------------------------------------------------------------

    private List<QueryScored> kAdaptive(
            List<QueryScored> scored,
            LinkedHashMap<String, EncryptedPoint> uniq,
            QueryToken token,
            double[] qVec,
            KeyVersion kv)
    {
        return scored;
    }

    public void setStabilizationCallback(StabilizationCallback cb) {
        this.stabilizationCallback = cb;
    }

    // ----------------------------------------------------------------------
    // Top-K variant evaluation
    // ----------------------------------------------------------------------

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(
            QueryToken baseToken,
            int queryIndex,
            GroundtruthManager gt)
    {
        Objects.requireNonNull(baseToken);

        final List<Integer> variants = List.of(1, 5, 10, 20, 40, 60, 80, 100);

        long clientStart = System.nanoTime();
        List<QueryResult> base = nn(search(baseToken));    // performs the actual query
        long clientEnd = System.nanoTime();

        long clientNs = Math.max(0L, clientEnd - clientStart);
        long serverNs = lastQueryDurationNs;
        long runNs    = clientNs + serverNs;

        long clientMs  = Math.round(clientNs / 1e6);
        long serverMs  = Math.round(serverNs / 1e6);
        long runMs     = Math.round(runNs / 1e6);
        long decryptMs = Math.round(lastDecryptNs / 1e6);

        int candTotal      = lastCandTotal;
        int candKept       = lastCandKeptVersion;
        int candDecrypted  = lastCandDecrypted;
        int candReturned   = lastReturned;
        int vectorDim      = baseToken.getDimension();
        int tokenBytes     = prefixTokenBytes(baseToken);

        List<QueryEvaluationResult> out = new ArrayList<>(variants.size());

        for (int k : variants) {
            int upto = Math.min(k, base.size());
            List<QueryResult> prefix = base.subList(0, upto);

            double precision = computePrecision(prefix, gt, queryIndex, k);

            out.add(new QueryEvaluationResult(
                    /* topKRequested   */ k,
                    /* retrieved       */ upto,
                    /* ratio           */ Double.NaN,
                    /* precision       */ precision,

                    /* timeMs(server)  */ serverMs,
                    /* clientTimeMs    */ clientMs,
                    /* runTimeMs       */ runMs,
                    /* decryptTimeMs   */ decryptMs,
                    /* insertTimeMs    */ 0,

                    /* candTotal       */ candTotal,
                    /* candKept        */ candKept,
                    /* candDecrypted   */ candDecrypted,
                    /* candReturned    */ candReturned,

                    /* tokenBytes      */ tokenBytes,
                    /* vectorDim       */ vectorDim,
                    /* totalFlushed    */ 0,
                    /* flushThreshold  */ 0,

                    /* touchedCount    */ lastTouchedCumulativeUnique,
                    /* reencCount      */ 0,
                    /* reencTimeMs     */ 0,
                    /* reencDelta      */ 0,
                    /* reencAfter      */ 0,
                    /* ratioDenomSrc   */ "eval",

                    /* tokenK          */ k,
                    /* tokenKBase      */ k,
                    /* qIndex          */ queryIndex,
                    /* candMode        */ "full"
            ));
        }
        return out;
    }


    private double computePrecision(List<QueryResult> prefix,
                                    GroundtruthManager gt,
                                    int qIndex,
                                    int k)
    {
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

    // ----------------------------------------------------------------------
    // Utilities and metrics
    // ----------------------------------------------------------------------

    private record QueryScored(String id, double dist) {}

    private static <T> List<T> nn(List<T> v) { return (v == null) ? Collections.emptyList() : v; }

    private double l2sq(double[] a, double[] b) {
        int len = Math.min(a.length, b.length);
        double s = 0.0;
        for (int i = 0; i < len; i++) {
            double d = a[i] - b[i];
            s += d*d;
        }
        return s;
    }

    private void clearLastMetrics() {
        lastQueryDurationNs = 0;
        lastClientDurationNs = 0;
        lastDecryptNs = 0;
        lastRunNs = 0;
        lastCandTotal = 0;
        lastCandKeptVersion = 0;
        lastCandDecrypted = 0;
        lastReturned = 0;
        lastCandIds = Collections.emptyList();
        lastStabilizedCandIds = Collections.emptyList();  // ADD THIS
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
        logger.debug("Added candidate IDs: {}", lastCandIds); // Log candidates added
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
        logger.debug("After stabilization: raw={}, finalSize={}, candidates={}", raw, finalSize, sub.size()); // Log candidate size after stabilization

        if (stabilizationCallback != null)
            stabilizationCallback.accept(raw, finalSize);

        return sub;
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
    public List<String> getLastStabilizedCandidateIds() {
        return (lastStabilizedCandIds == null)
                ? Collections.emptyList()
                : Collections.unmodifiableList(lastStabilizedCandIds);
    }
    public long getLastQueryDurationNs() { return lastQueryDurationNs; }
    public int  getLastCandTotal()       { return lastCandTotal; }
    public int  getLastCandKeptVersion() { return lastCandKeptVersion; }
    public int  getLastCandDecrypted()   { return lastCandDecrypted; }
    public int  getLastReturned()        { return lastReturned; }
    public long getLastClientDurationNs() { return lastClientDurationNs; }
    public long getLastDecryptNs()        { return lastDecryptNs; }
    public long getLastRunNs()            { return lastRunNs; }
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
