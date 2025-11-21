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
    private static final int MAX_BUCKETS = 100;

    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final QueryTokenFactory tokenFactory; // may be null (kept for future use if needed)

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
        if (token == null) {
            logger.warn("search() called with null token; returning empty result.");
            return Collections.emptyList();
        }

        clearLastMetrics();

        final QueryToken tok = token; // no more upgrade / plaintext path

        KeyVersion kv;
        try {
            kv = keyService.getVersion(tok.getVersion());
        } catch (RuntimeException ex) {
            // Fallback: current key (in case token version is missing)
            kv = keyService.getCurrentVersion();
            try { indexService.lookup(tok); } catch (Throwable ignore) {}
        }

        // Encrypted-only design: we require iv + encryptedQuery
        if (tok.getEncryptedQuery() == null || tok.getIv() == null) {
            throw new IllegalArgumentException("QueryToken must have encryptedQuery + iv; plaintext is not supported.");
        }

        final double[] qVec = cryptoService.decryptQuery(
                tok.getEncryptedQuery(),
                tok.getIv(),
                kv.getKey()
        );

        final long tCore0 = System.nanoTime();
        try {
            List<EncryptedPoint> raw;
            try {
                raw = indexService.lookup(tok);
            } catch (Throwable t) {
                raw = Collections.emptyList();
            }

            // De-dup stable
            final Map<String, EncryptedPoint> uniq = new LinkedHashMap<>(Math.max(16, raw.size()));
            for (EncryptedPoint ep : raw) {
                if (ep != null) uniq.putIfAbsent(ep.getId(), ep);
            }

            // record unfiltered candidate ids and total
            lastCandIds = new ArrayList<>(uniq.keySet());
            lastCandTotal = uniq.size();

            final List<QueryScored> scored = new ArrayList<>(uniq.size());
            for (EncryptedPoint ep : uniq.values()) {
                if (ep.getVersion() != kv.getVersion()) continue;
                lastCandKeptVersion++;

                final double[] v = cryptoService.decryptFromPoint(ep, kv.getKey());
                if (v == null || v.length == 0) continue;

                lastCandDecrypted++;
                final double d2 = l2sq(qVec, v);
                scored.add(new QueryScored(ep.getId(), d2));
            }

            scored.sort(Comparator.comparingDouble(QueryScored::dist));
            final int k = calculateOptimalTopK(tok, scored.size());
            if (k <= 0) {
                logger.warn("No candidates available for topK={} (adjusting search strategy).", k);
                lastReturned = 0;
                return Collections.emptyList();
            }

            final int validTopK = Math.min(k, scored.size());
            final List<QueryResult> out = new ArrayList<>(validTopK);
            for (int i = 0; i < validTopK; i++) {
                final QueryScored s = scored.get(i);
                final double d = RETURN_SQRT ? Math.sqrt(s.dist()) : s.dist();
                out.add(new QueryResult(s.id(), d));
            }

            lastReturned = out.size();
            return out;
        } finally {
            lastQueryDurationNs = Math.max(0L, System.nanoTime() - tCore0);
        }
    }

    // Optimized candidate retrieval method based on topK (currently unused but kept)
    private List<EncryptedPoint> fetchCandidatesBasedOnTopK(QueryToken token) {
        List<EncryptedPoint> raw;
        try {
            raw = indexService.lookup(token);
            int topK = token.getTopK();
            if (raw.size() > topK) {
                raw = raw.subList(0, topK);
            }
        } catch (Throwable t) {
            raw = Collections.emptyList();
        }
        return raw;
    }

    // New method to calculate optimal TopK based on dynamic fetching and ART optimization
    private int calculateOptimalTopK(QueryToken token, int fetchedSize) {
        final int requestedTopK = Math.max(1, token.getTopK());
        if (fetchedSize <= 0) return 0;
        return Math.min(requestedTopK, fetchedSize);
    }

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(QueryToken baseToken,
                                                              int queryIndex,
                                                              GroundtruthManager gt) {
        Objects.requireNonNull(baseToken, "baseToken");

        final List<Integer> topKVariants = List.of(1, 5, 10, 20, 40, 60, 80, 100);
        final long clientStartNs = System.nanoTime();
        final List<QueryResult> baseResults = nn(search(baseToken));
        final long clientEndNs = System.nanoTime();

        final long clientWindowNs = Math.max(0L, clientEndNs - clientStartNs);
        final long serverNsBounded = getLastQueryDurationNsCappedTo(clientWindowNs);
        final long queryDurationMs = Math.round(serverNsBounded / 1_000_000.0);

        final int candTotal        = getLastCandTotal();
        final int candKeptVersion  = getLastCandKeptVersion();
        final int candDecrypted    = getLastCandDecrypted();
        final int returned         = getLastReturned();

        final List<QueryEvaluationResult> out = new ArrayList<>(topKVariants.size());
        for (int k : topKVariants) {
            final int upto = Math.min(k, baseResults.size());
            final List<QueryResult> prefix = baseResults.subList(0, upto);

            // Precision@K
            int[] gtArr = (gt != null) ? safeGt(gt.getGroundtruth(queryIndex, k)) : new int[0];
            double precision = 0.0;
            if (k > 0 && gtArr.length > 0 && upto > 0) {
                final Set<String> truthSet = Arrays.stream(gtArr)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.toSet());
                int hits = 0;
                for (int i = 0; i < upto; i++) {
                    if (truthSet.contains(prefix.get(i).getId())) hits++;
                }
                precision = ((double) hits) / (double) k;
            }

            out.add(new QueryEvaluationResult(
                    k,                  // topKRequested
                    upto,               // retrieved
                    Double.NaN,         // ratio (computed elsewhere if needed)
                    precision,          // precision
                    queryDurationMs,    // timeMs (server)
                    0,                  // insertTimeMs
                    candDecrypted,      // candDecrypted
                    0,                  // tokenSizeBytes (can be set by caller if needed)
                    0,                  // vectorDim
                    0,                  // totalFlushedPoints
                    0,                  // flushThreshold
                    0,                  // touchedCount
                    0,                  // reencryptedCount
                    0,                  // reencTimeMs
                    0,                  // reencBytesDelta
                    0,                  // reencBytesAfter
                    "test",             // ratioDenomSource
                    0,                  // clientTimeMs (if you want, pass Math.round(clientWindowNs/1e6))
                    k,                  // tokenK
                    k,                  // tokenKBase
                    0,                  // qIndexZeroBased
                    "test"              // candMetricsMode
            ));
        }
        return out;
    }

    /* -------------------- helpers -------------------- */

    private record QueryScored(String id, double dist) {}

    private static <T> List<T> nn(List<T> v) { return (v == null) ? Collections.emptyList() : v; }
    private static int[] safeGt(int[] a)     { return (a == null) ? new int[0] : a; }

    /**
     * Dimension-tolerant squared L2 distance.
     * If vectors differ in length, we compute over the overlapping prefix
     * and log a warning instead of throwing.
     */
    private double l2sq(double[] a, double[] b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Vectors must not be null");
        }

        int len = Math.min(a.length, b.length);
        if (a.length != b.length) {
            logger.warn(
                    "Vector dimension mismatch in l2sq: a.length={} b.length={}; using first {} dimensions.",
                    a.length, b.length, len
            );
        }

        double s = 0.0;
        for (int i = 0; i < len; i++) {
            double d = a[i] - b[i];
            s += d * d;
        }
        return s; // squared L2 distance
    }

    public static int estimateTokenSizeBytes(QueryToken t) {
        int bytes = 0;
        if (t.getIv() != null) bytes += t.getIv().length;
        if (t.getEncryptedQuery() != null) bytes += t.getEncryptedQuery().length;
        int bucketCount = 0;
        for (List<Integer> l : t.getTableBuckets()) bucketCount += l.size();
        bytes += bucketCount * Integer.BYTES;

        // approximate BitSet footprint
        BitSet[] codes = t.getCodes();
        if (codes != null) {
            for (BitSet bs : codes) {
                if (bs != null) {
                    int bits = Math.max(0, bs.length());
                    bytes += (bits + 7) / 8;
                }
            }
        }
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

    public long getLastQueryDurationNsCappedTo(long clientWindowNs) {
        if (clientWindowNs <= 0L) return Math.max(0L, lastQueryDurationNs);
        return Math.min(Math.max(0L, lastQueryDurationNs), clientWindowNs);
    }

    public List<String> getLastCandidateIds() {
        return (lastCandIds == null) ? java.util.Collections.emptyList()
                : java.util.Collections.unmodifiableList(lastCandIds);
    }

    public long getLastQueryDurationNs()   { return lastQueryDurationNs; }
    public int  getLastCandTotal()         { return lastCandTotal; }
    public int  getLastCandKeptVersion()   { return lastCandKeptVersion; }
    public int  getLastCandDecrypted()     { return lastCandDecrypted; }
    public int  getLastReturned()          { return lastReturned; }
}
