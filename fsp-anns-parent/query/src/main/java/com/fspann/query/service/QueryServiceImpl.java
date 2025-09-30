package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
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

    // unfiltered candidate IDs (subset union, pre-version filter)
    private volatile List<String> lastCandIds = java.util.Collections.emptyList();
    public List<String> getLastCandidateIds() { return lastCandIds; }

    /** If ever need true L2 in the returned results, flip RETURN_SQRT to true. */
    private static final boolean RETURN_SQRT = false; // keep false to minimize server time

    // ---- NEW: global de-dup tracking for touched IDs across the run ----
    private static final Set<String> GLOBAL_TOUCHED =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private volatile int lastTouchedUniqueSoFar = 0;
    private volatile int lastTouchedCumulativeUnique = 0;
    public int getLastTouchedUniqueSoFar() { return lastTouchedUniqueSoFar; }
    public int getLastTouchedCumulativeUnique() { return lastTouchedCumulativeUnique; }

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
        final long start = System.nanoTime();
        Objects.requireNonNull(token, "QueryToken cannot be null");
        if (token.getTopK() <= 0 || token.getEncryptedQuery() == null || token.getIv() == null) {
            throw new IllegalArgumentException("Invalid or incomplete QueryToken");
        }

        // Resolve key
        final int requestedVersion = token.getVersion();
        final Integer contextVersion = parseVersionFromContext(token.getEncryptionContext());
        boolean usedFallback = false;

        KeyVersion kv;
        try {
            kv = keyService.getVersion(requestedVersion);
        } catch (RuntimeException ex) {
            kv = keyService.getCurrentVersion();
            usedFallback = true;
            logger.warn("Requested version v{} unavailable; falling back to current v{} (ctx v={})",
                    requestedVersion, kv.getVersion(), contextVersion);
        }

        final SecretKey key = kv.getKey();
        final int activeVersion = kv.getVersion();

        // Decrypt query vector
        final double[] queryVec;
        try {
            queryVec = cryptoService.decryptQuery(token.getEncryptedQuery(), token.getIv(), key);
            if (queryVec.length != token.getDimension()) {
                throw new IllegalArgumentException("Decrypted query dim=" + queryVec.length +
                        " != token.dim=" + token.getDimension());
            }
        } catch (Exception e) {
            logger.error("Query decryption failed", e);
            throw new RuntimeException("Decryption error: query vector", e);
        }

        final int K = token.getTopK();

        // Optional diagnostics if we fell back on key version
        if (usedFallback && indexService instanceof SecureLSHIndexService svc) {
            try { svc.lookupWithDiagnostics(token); }
            catch (Throwable t) { logger.debug("lookupWithDiagnostics failed (ignored): {}", t.toString()); }
        }

        // --------------------------
        // FETCH POLICY (paper-aligned)
        // --------------------------
        // Fetch the FULL subset-union determined by the token (m, λ, ℓ) — no pre-sizing/fanout.
        final List<EncryptedPoint> candidates = nn(indexService.lookup(token));
        final int candTotal = candidates.size();
        lastCandTotal = candTotal;

        // capture touched IDs (pre-version filter)
        if (candTotal > 0) {
            List<String> ids = new ArrayList<>(candTotal);
            for (EncryptedPoint p : candidates) ids.add(p.getId());
            lastCandIds = java.util.Collections.unmodifiableList(ids);
            logger.debug("Touched (subset union) count = {}", ids.size());
            // update global unique counters
            int before = GLOBAL_TOUCHED.size();
            for (String id : ids) GLOBAL_TOUCHED.add(id);
            lastTouchedUniqueSoFar = GLOBAL_TOUCHED.size() - before;
            lastTouchedCumulativeUnique = GLOBAL_TOUCHED.size();
        } else {
            lastCandIds = java.util.Collections.emptyList();
            lastTouchedUniqueSoFar = 0;
            lastTouchedCumulativeUnique = GLOBAL_TOUCHED.size();
        }

        if (candTotal == 0) {
            lastCandIds = java.util.Collections.emptyList(); // ensure cleared
            logger.warn("No candidates (v={}, dim={}, K={})", activeVersion, token.getDimension(), token.getTopK());
            lastCandKeptVersion = 0;
            lastCandDecrypted = 0;
            lastReturned = 0;
            lastQueryDurationNs = System.nanoTime() - start;
            return java.util.List.of();
        }


        // Forward-secure filter (version match)
        final List<EncryptedPoint> sameVersion = new ArrayList<>(candTotal);
        for (EncryptedPoint p : candidates) if (p.getVersion() == activeVersion) sameVersion.add(p);
        final int candKeptVersion = sameVersion.size();

        if (candTotal >= 10 && candKeptVersion <= Math.max(1, candTotal / 10)) {
            logger.warn("Version-mismatch risk: totalCand={} keptSameVersion={} (v{}), dim={}, K={}",
                    candTotal, candKeptVersion, activeVersion, token.getDimension(), token.getTopK());
        }

        // Decrypt + re-rank
        final List<QueryResult> results;
        int candDecrypted = 0;

        if (K <= SMALL_TOPK_THRESHOLD) {
            results = new ArrayList<>(Math.min(K, candKeptVersion));
            for (EncryptedPoint pt : sameVersion) {
                try {
                    double[] vec = cryptoService.decryptFromPoint(pt, key);
                    candDecrypted++;
                    results.add(new QueryResult(pt.getId(), l2sq(queryVec, vec)));
                } catch (IllegalArgumentException e) {
                    throw e; // hard fail on dim mismatch
                } catch (Exception e) {
                    logger.debug("Decrypt failed for candidate {}", pt.getId());
                    logger.trace("Decrypt exception", e);
                }
            }
            results.sort(Comparator.naturalOrder());
            if (results.size() > K) results.subList(K, results.size()).clear();
            sqrtInPlace(results);
        } else {
            PriorityQueue<QueryResult> heap = new PriorityQueue<>(Comparator.reverseOrder()); // max-heap
            for (EncryptedPoint pt : sameVersion) {
                try {
                    double[] vec = cryptoService.decryptFromPoint(pt, key);
                    candDecrypted++;
                    double dist = l2sq(queryVec, vec);
                    heap.offer(new QueryResult(pt.getId(), dist));
                    if (heap.size() > K) heap.poll();
                } catch (Exception e) {
                    logger.debug("Decrypt failed for candidate {}", pt.getId());
                    logger.trace("Decrypt exception", e);
                }
            }
            results = new ArrayList<>(heap);
            results.sort(Comparator.naturalOrder());
            sqrtInPlace(results);
        }

        // Commit last* counters & time
        lastCandKeptVersion = candKeptVersion;
        lastCandDecrypted = candDecrypted;
        lastReturned = results.size();
        lastQueryDurationNs = System.nanoTime() - start;

        // Guardrails
        if (lastReturned < Math.min(K, 3) && candTotal > 0) {
            int keptPct = (int) Math.round(100.0 * candKeptVersion / Math.max(1, candTotal));
            int decPct  = (int) Math.round(100.0 * candDecrypted   / Math.max(1, candKeptVersion));
            logger.warn("Low return: ret={} of K={}, keptVer={} ({}%) decrypted={} ({}%) candTotal={}",
                    lastReturned, K, candKeptVersion, keptPct, candDecrypted, decPct, candTotal);
        }
        if (candKeptVersion > 0 && candDecrypted == 0) {
            logger.warn("All same-version candidates failed to decrypt (v={}, dim={}, K={}, candTotal={})",
                    activeVersion, token.getDimension(), K, candTotal);
        }

        logger.debug("Q v{}: candTotal={}, keptSameVersion={}, decryptedOk={}, returned={}, time={}ms",
                activeVersion, candTotal, candKeptVersion, candDecrypted,
                lastReturned, lastQueryDurationNs / 1_000_000.0);

        return results;
    }

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

    public long getLastQueryDurationNs()   { return lastQueryDurationNs; }
    public int  getLastCandTotal()         { return lastCandTotal; }
    public int  getLastCandKeptVersion()   { return lastCandKeptVersion; }
    public int  getLastCandDecrypted()     { return lastCandDecrypted; }
    public int  getLastReturned()          { return lastReturned; }
    public static int getGlobalTouchedUniqueCount() { return GLOBAL_TOUCHED.size(); }
}
