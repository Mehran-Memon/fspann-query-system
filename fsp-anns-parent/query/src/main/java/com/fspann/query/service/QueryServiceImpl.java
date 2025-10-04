package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.index.service.SecureLSHIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.util.*;
import java.util.regex.Matcher;
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
        Objects.requireNonNull(token, "token");

        final long t0 = System.nanoTime();
        clearLastMetrics();

        // Resolve key version
        KeyVersion kv;
        try {
            kv = keyService.getVersion(token.getVersion());
        } catch (RuntimeException ex) {
            kv = keyService.getCurrentVersion();
            try { indexService.lookupWithDiagnostics(token); } catch (Throwable ignore) {}
        }

        // Decrypt query
        final double[] qVec = cryptoService.decryptQuery(token.getEncryptedQuery(), token.getIv(), kv.getKey());

        // Diagnostics probe (best-effort)
        try { indexService.lookupWithDiagnostics(token); } catch (Throwable ignore) {}

        // 1) Lookup candidates
        List<EncryptedPoint> raw = indexService.lookup(token);
        if (raw == null) raw = java.util.Collections.emptyList();

        // 2) De-duplicate by ID (subset union, stable order)
        final Map<String, EncryptedPoint> uniq = new LinkedHashMap<>(Math.max(16, raw.size()));
        for (EncryptedPoint ep : raw) {
            if (ep != null) uniq.putIfAbsent(ep.getId(), ep);
        }
        this.lastCandTotal = uniq.size();                     // ScannedCandidates (union size)
        this.lastCandIds   = new ArrayList<>(uniq.keySet());  // touched IDs (union)

        // 3) Version filter → decrypt → score (squared L2)
        final List<QueryScored> scored = new ArrayList<>(uniq.size());
        for (EncryptedPoint ep : uniq.values()) {
            if (ep.getVersion() != kv.getVersion()) continue;
            this.lastCandKeptVersion++;
            final double[] v = cryptoService.decryptFromPoint(ep, kv.getKey());
            this.lastCandDecrypted++;
            if (reencTracker != null) reencTracker.touch(ep.getId());
            final double d2 = l2sq(qVec, v);
            scored.add(new QueryScored(ep.getId(), d2));
        }

        // 4) Sort + take TopK
        scored.sort(Comparator.comparingDouble(QueryScored::dist));
        final int k = Math.min(token.getTopK(), scored.size());
        final List<QueryResult> out = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            final QueryScored s = scored.get(i);
            final double d = RETURN_SQRT ? Math.sqrt(s.dist()) : s.dist();
            out.add(new QueryResult(s.id(), d));
        }
        this.lastReturned = out.size();

        // 5) Update global touched-unique counters
        final int before = GLOBAL_TOUCHED.size();
        GLOBAL_TOUCHED.addAll(this.lastCandIds);
        final int after = GLOBAL_TOUCHED.size();
        this.lastTouchedUniqueSoFar      = Math.max(0, after - before);
        this.lastTouchedCumulativeUnique = after;

        this.lastQueryDurationNs = System.nanoTime() - t0;
        return out;
    }

    private static double l2(double[] a, double[] b) {
        double s = 0.0;
        for (int i = 0; i < a.length; i++) { double d = a[i] - b[i]; s += d*d; }
        return Math.sqrt(s);
    }

    private record QueryScored(String id, double dist) {}


    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(QueryToken baseToken,
                                                              int queryIndex,
                                                              GroundtruthManager gt) {
        Objects.requireNonNull(baseToken, "baseToken");

        final List<Integer> topKVariants = List.of(1, 5, 10, 20, 40, 60, 80, 100);

        // One real search; counters populated by search()
        final long t0 = System.nanoTime();
        final List<QueryResult> baseResults = nn(search(baseToken));
        final long queryDurationMs = Math.round((System.nanoTime() - t0) / 1_000_000.0);

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
                    queryDurationMs,      // ServerTimeMs
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
                    /*ratioDenomSource*/ "none",      // <-- moved above
                    /*clientTimeMs*/     -1L,         // <-- moved below
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

    private static void sqrtInPlace(List<QueryResult> results) {
        if (!RETURN_SQRT) return;
        for (int i = 0; i < results.size(); i++) {
            QueryResult r = results.get(i);
            // Re-wrap with sqrt distance; assumes QueryResult(id, distance)
            results.set(i, new QueryResult(r.getId(), Math.sqrt(r.getDistance())));
        }
    }

    private Integer parseVersionFromContext(String ctx) {
        if (ctx == null) return null;
        Matcher m = VERSION_PATTERN.matcher(ctx);
        if (!m.matches()) return null;
        try { return Integer.parseInt(m.group(1)); } catch (Exception ignore) { return null; }
    }

    private static List<List<Integer>> deepCopy(List<List<Integer>> src) {
        List<List<Integer>> out = new ArrayList<>(src.size());
        for (List<Integer> l : src) out.add(new ArrayList<>(l));
        return out;
    }

    public static int estimateTokenSizeBytes(QueryToken t) {
        int bytes = 0;
        if (t.getIv() != null) bytes += t.getIv().length;
        if (t.getEncryptedQuery() != null) bytes += t.getEncryptedQuery().length;
        int bucketCount = 0;
        for (List<Integer> l : t.getTableBuckets()) bucketCount += l.size();
        bytes += bucketCount * Integer.BYTES;
        return bytes;
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
    public static int getGlobalTouchedUniqueCount() { return GLOBAL_TOUCHED.size(); }
    public List<String> getLastCandidateIds() {
        return (lastCandIds == null) ? java.util.Collections.emptyList()
                : java.util.Collections.unmodifiableList(lastCandIds);
    }
}
