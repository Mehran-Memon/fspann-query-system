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

    // --- NN rank diagnostics ---
    private volatile int lastTrueNNRank = -1;
    private volatile boolean lastTrueNNSeen = false;
    private volatile String trueNearestId;

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

        KeyVersion qkv;
        try {
            qkv = keyService.getVersion(token.getVersion());
        } catch (Throwable t) {
            qkv = keyService.getCurrentVersion();
        }

        final double[] qVec = cryptoService.decryptQuery(
                token.getEncryptedQuery(),
                token.getIv(),
                qkv.getKey()
        );

        if (!isValid(qVec)) {
            lastClientNs = System.nanoTime() - clientStart;
            return Collections.emptyList();
        }

        final long serverStart = System.nanoTime();

        boolean retried = false;

        try {
            final int K = token.getTopK();

            while (true) {

                // -------------------------------
                // STAGE A — candidate IDs
                // -------------------------------
                List<String> candidateIds = index.lookupCandidateIds(token);

                lastCandTotal = index.getLastRawCandidateCount();
                lastCandKept  = candidateIds.size();

                if (candidateIds.isEmpty()) return Collections.emptyList();

                // -------------------------------
                // STAGE B — bounded refinement
                // -------------------------------
                final int minDecryptFloor = Math.max(3500, 35 * K);

                final int refineLimit = Math.min(
                        candidateIds.size(),
                        Math.max(
                                minDecryptFloor,
                                Math.max(
                                        K,
                                        (int) Math.ceil(
                                                K * cfg.getStabilization().getMinCandidatesRatio()
                                        )
                                )
                        )
                );

                List<QueryScored> scored = new ArrayList<>(refineLimit);
                long decryptStart = System.nanoTime();

                int notFound = 0;
                int invalidVector = 0;
                int decryptError = 0;

                for (int i = 0; i < refineLimit; i++) {
                    String id = candidateIds.get(i);
                    try {
                        EncryptedPoint ep = index.loadPointIfActive(id);
                        if (ep == null) {
                            notFound++;
                            if (notFound <= 5) {  // Log first 5 only
                                logger.warn("Point {} not found in metadata", id);
                            }
                            continue;
                        }

                        KeyVersion kvp = keyService.getVersion(ep.getKeyVersion());
                        double[] v = cryptoService.decryptFromPoint(ep, kvp.getKey());

                        if (!isValid(v)) {
                            invalidVector++;
                            if (invalidVector <= 5) {  // Log first 5 only
                                logger.warn("Point {} decrypted to invalid vector (dim={}, null={})",
                                        id, v != null ? v.length : -1, v == null);
                            }
                            continue;
                        }

                        scored.add(new QueryScored(id, l2(qVec, v)));
                        touchedThisSession.add(id);

                    } catch (Exception e) {
                        decryptError++;
                        if (decryptError <= 5) {  // Log first 5 only
                            logger.error("Failed to decrypt {}: {}", id, e.getMessage());
                        }
                    }
                }

                lastDecryptNs = System.nanoTime() - decryptStart;
                lastCandDecrypted = scored.size();

                logger.info("Refinement: limit={} | notFound={} | invalidVec={} | decryptErr={} | scored={}",
                        refineLimit, notFound, invalidVector, decryptError, scored.size());

                if (scored.isEmpty()) return Collections.emptyList();
                // -------------------------------
                // STAGE C — rank & return
                // -------------------------------
                scored.sort(Comparator.comparingDouble(QueryScored::dist));

                int eff = K;
                if (scored.size() < K) {
                    logger.warn(
                            "Returned <K results: returned={} requested={}",
                            scored.size(), K
                    );
                    eff = scored.size();
                }

                List<QueryResult> out = new ArrayList<>(eff);
                LinkedHashSet<String> finalIds = new LinkedHashSet<>(eff);

                for (int i = 0; i < eff; i++) {
                    QueryScored s = scored.get(i);
                    out.add(new QueryResult(s.id(), s.dist()));
                    finalIds.add(s.id());
                }

                lastReturned = eff;
                lastCandIds  = finalIds;

                logger.info("QueryService returning: requested K={}, scored={}, returning={}",
                        K, scored.size(), out.size());

                // -------------------------------
                // PATCH 3 — adaptive retry (ONCE)
                // -------------------------------
                if (!retried && needRetry(K)) {
                    retried = true;

                    logger.debug(
                            "Adaptive retry triggered: returned={}, decrypted={}, K={}",
                            lastReturned, lastCandDecrypted, K
                    );

                    index.setProbeOverride(10);
                    continue;                    // retry whole A–C once
                }

                return out;
            }

        } finally {
            index.clearProbeOverride();  // CRITICAL: always reset

            lastServerNs = System.nanoTime() - serverStart;
            lastClientNs = System.nanoTime() - clientStart;

            if (reencTracker != null && !touchedThisSession.isEmpty()) {
                reencTracker.record(touchedThisSession);
            }
        }
    }

    // =====================================================================
    // DECRYPT + SCORE (L2)
    // =====================================================================

    private record QueryScored(String id, double dist) {}

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
        lastTrueNNRank = -1;
        lastTrueNNSeen = false;

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

    public List<String> getLastFinalResultIds() {
        return lastCandIds == null
                ? Collections.emptyList()
                : new ArrayList<>(lastCandIds);
    }
    public void setTrueNearestId(String id) {
        this.trueNearestId = id;
    }
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
    public int getLastTrueNNRank() {
        return lastTrueNNRank;
    }
    public boolean wasLastTrueNNSeen() {
        return lastTrueNNSeen;
    }
    private boolean needRetry(int K) {
        // retry if we failed to return K
        // OR refinement was too shallow to be reliable
        return lastReturned < K
                || lastCandDecrypted < (10 * K);
    }
    public QueryToken deriveToken(QueryToken base, int k) {
        if (tokenFactory == null) {
            throw new IllegalStateException("QueryTokenFactory not available");
        }
        return tokenFactory.derive(base, k);
    }


}
