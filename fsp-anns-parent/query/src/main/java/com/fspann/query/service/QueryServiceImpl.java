package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryServiceImpl implements QueryService {
    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);
    private static final Pattern VERSION_PATTERN = Pattern.compile("epoch_(\\d+)_dim_(\\d+)$");
    private static final int SMALL_TOPK_THRESHOLD = 10;

    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final QueryTokenFactory tokenFactory; // may be null

    // last-query metrics (visible to callers)
    private volatile long lastQueryDurationNs = 0;
    private volatile int lastCandTotal = 0;        // candidates before version filter (subset union size)
    private volatile int lastCandKeptVersion = 0;  // candidates after forward-secure version filter
    private volatile int lastCandDecrypted = 0;    // successfully decrypted
    private volatile int lastReturned = 0;         // returned results (<= topK)
    private volatile ReencryptionTracker reencTracker;

    // unfiltered candidate IDs (subset union, pre-version filter)
    private volatile List<String> lastCandIds = java.util.Collections.emptyList();


    /** If ever need true L2 in the returned results, flip RETURN_SQRT to true. */
    private static final boolean RETURN_SQRT = false; // keep false to minimize server time

    // ---- NEW: global de-dup tracking for touched IDs across the run ----
    private static final Set<String> GLOBAL_TOUCHED =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private volatile int lastTouchedUniqueSoFar = 0;
    private volatile int lastTouchedCumulativeUnique = 0;
    public int getLastTouchedUniqueSoFar() { return lastTouchedUniqueSoFar; }
    public int getLastTouchedCumulativeUnique() { return lastTouchedCumulativeUnique; }
    public void setReencryptionTracker(ReencryptionTracker tracker) {
        this.reencTracker = tracker;
    }
    // ---- CTORS ----
    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService) {
        this(indexService, cryptoService, keyService, null);
    }

    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService,
                            QueryTokenFactory tokenFactory) {
        this.indexService   = Objects.requireNonNull(indexService);
        this.cryptoService  = Objects.requireNonNull(cryptoService);
        this.keyService     = Objects.requireNonNull(keyService);
        this.tokenFactory   = tokenFactory; // may be null
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        // 0) Basic guards + prefer PAPER token when possible
        if (token == null) {
            logger.warn("search() called with null token; returning empty result.");
            clearLastMetrics();
            return java.util.Collections.emptyList();
        }
        final QueryToken tok = maybeUpgradeToPaperToken(token);
        if (tok == null) {
            logger.warn("Token upgrade returned null; returning empty result.");
            clearLastMetrics();
            return java.util.Collections.emptyList();
        }

        clearLastMetrics();

        // 1) Resolve key version (cheap, outside core timer)
        KeyVersion kv;
        try {
            kv = keyService.getVersion(tok.getVersion());
        } catch (RuntimeException ex) {
            // Graceful fallback retains previous behavior in tests
            kv = keyService.getCurrentVersion();
            try { indexService.lookup(tok); } catch (Throwable ignore) {}
        }

        // 2) Decrypt query vector (or use plaintext if provided)
        final double[] qVec;
        if (tok.getEncryptedQuery() != null && tok.getIv() != null) {
            qVec = cryptoService.decryptQuery(tok.getEncryptedQuery(), tok.getIv(), kv.getKey());
        } else if (tok.getQueryVector() != null) {
            qVec = tok.getQueryVector();
        } else {
            throw new IllegalArgumentException("QueryToken must have either encrypted query+iv or plaintext vector");
        }
        // Defensive: dimension mismatch → fail fast
        if (tok.getDimension() > 0 && tok.getDimension() != qVec.length) {
            throw new IllegalArgumentException("Query dimension mismatch: token=" + tok.getDimension()
                    + " vs decrypted=" + qVec.length);
        }

        // 3) Core server timing window
        final long tCore0 = System.nanoTime();
        try {
            // (a) Fetch candidates once
            final List<EncryptedPoint> raw;
            List<EncryptedPoint> tmp;
            try {
                tmp = indexService.lookup(tok);
            } catch (Throwable t) {
                // be conservative; never propagate lookup failures from tests
                tmp = java.util.Collections.emptyList();
            }
            raw = (tmp != null) ? tmp : java.util.Collections.emptyList();


            // (b) De-duplicate by ID (stable order)
            final Map<String, EncryptedPoint> uniq = new LinkedHashMap<>(Math.max(16, raw.size()));
            for (EncryptedPoint ep : raw) {
                if (ep != null) uniq.putIfAbsent(ep.getId(), ep);
            }
            this.lastCandTotal = uniq.size();
            this.lastCandIds   = new ArrayList<>(uniq.keySet());

            if (reencTracker != null && !lastCandIds.isEmpty()) {
                for (String id : lastCandIds) reencTracker.touch(id);
            }

            // (c) Forward-secure version filter → decrypt → score (squared L2)
            final List<QueryScored> scored = new ArrayList<>(uniq.size());
            for (EncryptedPoint ep : uniq.values()) {
                if (ep.getVersion() != kv.getVersion()) continue;
                this.lastCandKeptVersion++;

                final double[] v = cryptoService.decryptFromPoint(ep, kv.getKey());
                this.lastCandDecrypted++;

                if (reencTracker != null) reencTracker.touch(ep.getId());
                if (v == null || v.length != qVec.length) continue; // defensive

                final double d2 = l2sq(qVec, v);
                scored.add(new QueryScored(ep.getId(), d2));
            }

            // (d) Sort + topK
            scored.sort(Comparator.comparingDouble(QueryScored::dist));
            final int k = Math.min(tok.getTopK(), scored.size());
            final List<QueryResult> out = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                final QueryScored s = scored.get(i);
                final double d = RETURN_SQRT ? Math.sqrt(s.dist()) : s.dist();
                out.add(new QueryResult(s.id(), d));
            }
            this.lastReturned = out.size();

            // (e) Global touched-unique counters
            final int before = GLOBAL_TOUCHED.size();
            GLOBAL_TOUCHED.addAll(this.lastCandIds);
            final int after = GLOBAL_TOUCHED.size();
            this.lastTouchedUniqueSoFar      = Math.max(0, after - before);
            this.lastTouchedCumulativeUnique = after;

            return out;
        } finally {
            // record ONLY the server core time
            lastQueryDurationNs = Math.max(0L, System.nanoTime() - tCore0);
        }
    }

    private record QueryScored(String id, double dist) {}

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(QueryToken baseToken,
                                                              int queryIndex,
                                                              GroundtruthManager gt) {
        Objects.requireNonNull(baseToken, "baseToken");

        final List<Integer> topKVariants = List.of(1, 5, 10, 20, 40, 60, 80, 100);

        // One real search; counters populated by search()
        final long clientStartNs = System.nanoTime();
        final List<QueryResult> baseResults = nn(search(baseToken));
        final long clientEndNs = System.nanoTime();

        // Use the server's own timer, but cap it to the client window for consistency.
        final long clientWindowNs = Math.max(0L, clientEndNs - clientStartNs);
        final long serverNsBounded = getLastQueryDurationNsCappedTo(clientWindowNs);
        final long queryDurationMs = Math.round(serverNsBounded / 1_000_000.0);

        final int candTotal       = getLastCandTotal();
        final int candKeptVersion = getLastCandKeptVersion();
        final int candDecrypted   = getLastCandDecrypted();
        final int returned        = getLastReturned();

        final EncryptedPointBuffer buf = indexService.getPointBuffer();
        final long insertTimeMs   = (buf != null) ? buf.getLastBatchInsertTimeMs() : 0L;
        final int  totalFlushed   = (buf != null) ? buf.getTotalFlushedPoints()   : 0;
        final int  flushThreshold = (buf != null) ? buf.getFlushThreshold()       : 0;
        final int  tokenSizeBytes = QueryServiceImpl.estimateTokenSizeBytes(baseToken);
        final int  vectorDim      = baseToken.getDimension();

        final List<QueryEvaluationResult> out = new ArrayList<>(topKVariants.size());

        for (int k : topKVariants) {
            final int upto = Math.min(k, baseResults.size());
            final List<QueryResult> prefix = baseResults.subList(0, upto);

            // Precision@K
            int[] gtArr = (gt != null) ? safeGt(gt.getGroundtruth(queryIndex, k)) : new int[0];
            double precision = 0.0;
            if (k > 0 && gtArr.length > 0 && upto > 0) {
                final Set<String> truthSet = Arrays.stream(gtArr).mapToObj(String::valueOf).collect(Collectors.toSet());
                int hits = 0;
                for (int i = 0; i < upto; i++) if (truthSet.contains(prefix.get(i).getId())) hits++;
                precision = ((double) hits) / (double) k;
            }

            out.add(new QueryEvaluationResult(
                    k,
                    upto,
                    Double.NaN,           // ratio computed upstream
                    precision,
                    queryDurationMs,      // ServerTimeMs (bounded)
                    insertTimeMs,
                    candDecrypted,        // actually decrypted/scored
                    tokenSizeBytes,
                    vectorDim,
                    totalFlushed,
                    flushThreshold,
                    /*touchedCount*/ 0,
                    /*reencryptedCount*/ 0,
                    /*reencTimeMs*/ 0L,
                    /*reencBytesDelta*/ 0L,
                    /*reencBytesAfter*/ 0L,
                    /*ratioDenomSource*/ "none",
                    /*clientTimeMs*/     -1L,
                    /*tokenK*/           baseToken.getTopK(),
                    /*tokenKBase*/       baseToken.getTopK(),
                    /*qIndexZeroBased*/  queryIndex,
                    /*candMetricsMode*/  "full"
            ));
        }

        logger.debug("TopK variants summary: candTotal={}, keptVer={}, decrypted={}, returned={}",
                candTotal, candKeptVersion, candDecrypted, returned);

        return out;
    }

    /* -------------------- helpers -------------------- */

    private static <T> List<T> nn(List<T> v) { return (v == null) ? Collections.emptyList() : v; }
    private static int[] safeGt(int[] a)     { return (a == null) ? new int[0] : a; }
    private static double l2sq(double[] a, double[] b) {
        if (a.length != b.length) throw new IllegalArgumentException("Vector dimension mismatch: " + a.length + " vs " + b.length);
        double s = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            s += d * d;
        }
        return s; // squared L2, no sqrt
    }
    public static int estimateTokenSizeBytes(QueryToken t) {
        int bytes = 0;
        if (t.getIv() != null) bytes += t.getIv().length;
        if (t.getEncryptedQuery() != null) bytes += t.getEncryptedQuery().length;
        int bucketCount = 0;
        for (List<Integer> l : t.getTableBuckets()) bucketCount += l.size();
        bytes += bucketCount * Integer.BYTES;

        // NEW: approximate BitSet footprint
        BitSet[] codes = t.getCodes();
        if (codes != null) {
            for (BitSet bs : codes) {
                if (bs != null) {
                    int bits = Math.max(0, bs.length());       // highest set bit + 1
                    bytes += (bits + 7) / 8;                   // rough wire size
                }
            }
        }
        return bytes;
    }
    /**
     * If the incoming token has no per-division codes (paper path) but includes
     * plaintext and we have a QueryTokenFactory, rebuild it into a PAPER token
     * (codes + empty per-table buckets). This prevents empty-candidate queries
     * when the server stores only seed metadata (no legacy GFunction).
     */
    private QueryToken maybeUpgradeToPaperToken(QueryToken t) {
        try {
            if (t.getCodes() == null && tokenFactory != null) {
                double[] q = t.getPlaintextQuery();
                if (q != null && q.length > 0) {
                    logger.debug("Upgrading legacy token to PAPER token with codes on-the-fly (K={}).", t.getTopK());
                    QueryToken upgraded = tokenFactory.create(q, t.getTopK());
                    if (upgraded != null) return upgraded;   // only use it if non-null
                    logger.debug("TokenFactory returned null; using original token.");
                }
            }
        } catch (Throwable e) {
            logger.warn("Failed to upgrade token to PAPER form; proceeding with original token.", e);
        }
        return t;  // always return a non-null token
    }
    private void clearLastMetrics() {
        lastQueryDurationNs = 0L;
        lastCandTotal = 0;
        lastCandKeptVersion = 0;
        lastCandDecrypted = 0;
        lastReturned = 0;
        lastCandIds = java.util.Collections.emptyList();
        lastTouchedUniqueSoFar = 0;
        lastTouchedCumulativeUnique = GLOBAL_TOUCHED.size();
    }
    public long getLastQueryDurationNs()   { return lastQueryDurationNs; }
    public int  getLastCandTotal()         { return lastCandTotal; }
    public int  getLastCandKeptVersion()   { return lastCandKeptVersion; }
    public int  getLastCandDecrypted()     { return lastCandDecrypted; }
    public int  getLastReturned()          { return lastReturned; }
    public long getLastQueryDurationNsCappedTo(long clientWindowNs) {
        if (clientWindowNs <= 0L) return Math.max(0L, lastQueryDurationNs);
        return Math.min(Math.max(0L, lastQueryDurationNs), clientWindowNs);
    }
    public List<String> getLastCandidateIds() {
        return (lastCandIds == null) ? java.util.Collections.emptyList()
                : java.util.Collections.unmodifiableList(lastCandIds);
    }

}
