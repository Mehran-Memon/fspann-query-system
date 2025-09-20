package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
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
    private final QueryTokenFactory tokenFactory; // optional

    // last-query metrics (visible to callers)
    private volatile long lastQueryDurationNs = 0;
    private volatile int lastCandTotal = 0;        // candidates before version filter
    private volatile int lastCandKeptVersion = 0;  // candidates after version filter
    private volatile int lastCandDecrypted = 0;    // successfully decrypted
    private volatile int lastReturned = 0;         // returned results (<= topK)

    public QueryServiceImpl(IndexService indexService, CryptoService cryptoService, KeyLifeCycleService keyService) {
        this(indexService, cryptoService, keyService, null);
    }

    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService,
                            QueryTokenFactory tokenFactory) {
        this.indexService = Objects.requireNonNull(indexService);
        this.cryptoService = Objects.requireNonNull(cryptoService);
        this.keyService = Objects.requireNonNull(keyService);
        this.tokenFactory = tokenFactory; // may be null
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        final long start = System.nanoTime();
        Objects.requireNonNull(token, "QueryToken cannot be null");
        if (token.getTopK() <= 0 || token.getEncryptedQuery() == null || token.getIv() == null) {
            throw new IllegalArgumentException("Invalid or incomplete QueryToken");
        }

        // Resolve key for this token (prefer explicit version on token; context string is best-effort sanity)
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

        // Pull candidates from the index.
        // If we had to fallback versions, also invoke diagnostics for observability.
        if (usedFallback) {
            try {
                indexService.lookupWithDiagnostics(token); // ignore return; tests expect we call this on fallback
            } catch (Throwable t) {
                logger.debug("lookupWithDiagnostics failed (ignored in fallback path): {}", t.toString());
            }
        }
        final List<EncryptedPoint> candidates = nn(indexService.lookup(token));
        final int candTotal = candidates.size();
        lastCandTotal = candTotal;

        if (candTotal == 0) {
            logger.warn("No candidates (v={}, dim={}, K={})", activeVersion, token.getDimension(), token.getTopK());
            lastCandKeptVersion = 0;
            lastCandDecrypted = 0;
            lastReturned = 0;
            lastQueryDurationNs = System.nanoTime() - start;
            return List.of();
        }

        // Forward-secure filter: only same-version candidates are eligible
        final List<EncryptedPoint> sameVersion = new ArrayList<>(candTotal);
        for (EncryptedPoint p : candidates) if (p.getVersion() == activeVersion) sameVersion.add(p);
        final int candKeptVersion = sameVersion.size();

        if (candTotal >= 10 && candKeptVersion <= Math.max(1, candTotal / 10)) {
            logger.warn("Version-mismatch risk: totalCand={} keptSameVersion={} (v{}), dim={}, K={}",
                    candTotal, candKeptVersion, activeVersion, token.getDimension(), token.getTopK());
        }

        // Re-rank by decrypting same-version candidates and computing L2 against query
        final int K = token.getTopK();
        final List<QueryResult> results;
        int candDecrypted = 0;

        if (K <= SMALL_TOPK_THRESHOLD) {
            results = new ArrayList<>(Math.min(K, candKeptVersion));
            for (EncryptedPoint pt : sameVersion) {
                try {
                    double[] vec = cryptoService.decryptFromPoint(pt, key);
                    candDecrypted++;
                    results.add(new QueryResult(pt.getId(), l2(queryVec, vec)));
                } catch (IllegalArgumentException e) {
                    throw e; // hard fail on dim mismatch (data corruption)
                } catch (Exception e) {
                    logger.debug("Decrypt failed for candidate {}", pt.getId());
                    logger.trace("Decrypt exception", e);
                }
            }
            results.sort(Comparator.naturalOrder()); // ascending distance
            if (results.size() > K) results.subList(K, results.size()).clear();
        } else {
            PriorityQueue<QueryResult> heap = new PriorityQueue<>(Comparator.reverseOrder()); // max-heap
            for (EncryptedPoint pt : sameVersion) {
                try {
                    double[] vec = cryptoService.decryptFromPoint(pt, key);
                    candDecrypted++;
                    double dist = l2(queryVec, vec);
                    heap.offer(new QueryResult(pt.getId(), dist));
                    if (heap.size() > K) heap.poll();
                } catch (Exception e) {
                    logger.debug("Decrypt failed for candidate {}", pt.getId());
                    logger.trace("Decrypt exception", e);
                }
            }
            results = new ArrayList<>(heap);
            results.sort(Comparator.naturalOrder());
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
        final List<Integer> topKVariants = List.of(1, 20, 40, 60, 80, 100);
        final List<QueryEvaluationResult> out = new ArrayList<>(topKVariants.size());

        for (int k : topKVariants) {
            final QueryToken variant = (tokenFactory != null)
                    ? tokenFactory.derive(baseToken, k)
                    : new QueryToken(
                    deepCopy(baseToken.getTableBuckets()),
                    baseToken.getIv(),
                    baseToken.getEncryptedQuery(),
                    baseToken.getQueryVector(),   // keep plaintext if present on token
                    k,
                    baseToken.getNumTables(),
                    baseToken.getEncryptionContext(),
                    baseToken.getDimension(),
                    baseToken.getVersion()
            );

            final long t0 = System.nanoTime();
            final List<QueryResult> retrievedAll = search(variant);
            final long queryDurationMs = (System.nanoTime() - t0) / 1_000_000L;

            final List<QueryResult> retrieved = (retrievedAll == null)
                    ? Collections.emptyList()
                    : retrievedAll.subList(0, Math.min(k, retrievedAll.size()));
            final int retrievedCount = retrieved.size();

            // Recall@K (denominator = K). Ratio@K is deferred (NaN here).
            double precision = 0.0;
            int[] gtArr = new int[0];
            if (gt != null) {
                try { gtArr = gt.getGroundtruth(queryIndex, k); } catch (Exception ignore) {}
            }
            if (k > 0 && gtArr.length > 0 && retrievedCount > 0) {
                Set<String> truthSet = Arrays.stream(gtArr).mapToObj(String::valueOf).collect(Collectors.toSet());
                int hits = 0;
                int upto = Math.min(k, retrievedCount);
                for (int i = 0; i < upto; i++) if (truthSet.contains(retrieved.get(i).getId())) hits++;
                precision = ((double) hits) / (double) k;
            }

            final EncryptedPointBuffer buf = indexService.getPointBuffer();
            final long insertTimeMs   = (buf != null) ? buf.getLastBatchInsertTimeMs() : 0L;
            final int  totalFlushed   = (buf != null) ? buf.getTotalFlushedPoints()   : 0;
            final int  flushThreshold = (buf != null) ? buf.getFlushThreshold()       : 0;
            final int  tokenSizeBytes = QueryServiceImpl.estimateTokenSizeBytes(variant);
            final int  vectorDim      = variant.getDimension();

            out.add(new QueryEvaluationResult(
                    k, retrievedCount,
                    Double.NaN,              // Literature Ratio@K computed by ForwardSecureANNSystem
                    precision,               // Recall@K (named "precision" historically in CSVs)
                    queryDurationMs,
                    insertTimeMs,
                    getLastCandKeptVersion(), // candidates tracked in last search()
                    tokenSizeBytes,
                    vectorDim,
                    totalFlushed,
                    flushThreshold
            ));
        }

        return out;
    }

    /* -------------------- helpers -------------------- */

    private static double l2(double[] a, double[] b) {
        if (a.length != b.length) throw new IllegalArgumentException("Vector dimension mismatch: " + a.length + " vs " + b.length);
        double s = 0;
        for (int i = 0; i < a.length; i++) { double d = a[i] - b[i]; s += d * d; }
        return Math.sqrt(s);
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

    private static <T> List<T> nn(List<T> v) { return (v == null) ? Collections.emptyList() : v; }

    public long getLastQueryDurationNs()   { return lastQueryDurationNs; }
    public int  getLastCandTotal()         { return lastCandTotal; }
    public int  getLastCandKeptVersion()   { return lastCandKeptVersion; }
    public int  getLastCandDecrypted()     { return lastCandDecrypted; }
    public int  getLastReturned()          { return lastReturned; }
}
