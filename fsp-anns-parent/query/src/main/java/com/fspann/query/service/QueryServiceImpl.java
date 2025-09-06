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
    private final QueryTokenFactory tokenFactory; // maybe null
    private long lastQueryDurationNs = 0;

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
        this.tokenFactory = tokenFactory; // optional
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        long start = System.nanoTime();
        Objects.requireNonNull(token, "QueryToken cannot be null");
        if (token.getTopK() <= 0 || token.getEncryptedQuery() == null || token.getIv() == null) {
            throw new IllegalArgumentException("Invalid or incomplete QueryToken");
        }

//        keyService.rotateIfNeeded();
        KeyVersion queryVersion = resolveKeyVersion(token.getEncryptionContext());
        SecretKey key = queryVersion.getKey();

        final double[] queryVec;
        try {
            queryVec = cryptoService.decryptQuery(token.getEncryptedQuery(), token.getIv(), key);
        } catch (Exception e) {
            logger.error("Query decryption failed", e);
            throw new RuntimeException("Decryption error: query vector", e);
        }

        List<EncryptedPoint> candidates = indexService.lookup(token);
        if (candidates.isEmpty()) {
            logger.debug("No candidate vectors found for query version {}", queryVersion.getVersion());
            lastQueryDurationNs = System.nanoTime() - start;
            return List.of();
        }

        final List<QueryResult> results;
        if (token.getTopK() <= SMALL_TOPK_THRESHOLD) {
            results = new ArrayList<>();
            for (EncryptedPoint pt : candidates) {
                if (pt.getVersion() != queryVersion.getVersion()) continue; // forward-secure filter
                try {
                    double[] ptVec = cryptoService.decryptFromPoint(pt, key);
                    results.add(new QueryResult(pt.getId(), l2(queryVec, ptVec)));
                } catch (IllegalArgumentException e) {
                    throw e; // dimension mismatch should fail fast
                } catch (Exception e) {
                    logger.warn("Failed to decrypt candidate {}. Skipping.", pt.getId(), e);
                }
            }
            results.sort(Comparator.naturalOrder());
            if (results.size() > token.getTopK()) results.subList(token.getTopK(), results.size()).clear();
        } else {
            PriorityQueue<QueryResult> topKQueue = new PriorityQueue<>(Comparator.reverseOrder());
            for (EncryptedPoint pt : candidates) {
                if (pt.getVersion() != queryVersion.getVersion()) continue;
                try {
                    double[] ptVec = cryptoService.decryptFromPoint(pt, key);
                    double dist = l2(queryVec, ptVec);
                    topKQueue.offer(new QueryResult(pt.getId(), dist));
                    if (topKQueue.size() > token.getTopK()) topKQueue.poll();
                } catch (Exception e) {
                    logger.warn("Failed to decrypt candidate {}. Skipping.", pt.getId(), e);
                }
            }
            results = new ArrayList<>(topKQueue);
            results.sort(Comparator.naturalOrder());
        }

        lastQueryDurationNs = System.nanoTime() - start;
        logger.debug("Query version={}, candidates={}, returned={}, time={}ms",
                queryVersion.getVersion(), candidates.size(), results.size(), lastQueryDurationNs / 1_000_000);
        return results;
    }

    private KeyVersion resolveKeyVersion(String context) {
        if (context == null) return keyService.getCurrentVersion();
        Matcher m = VERSION_PATTERN.matcher(context);
        if (!m.matches()) return keyService.getCurrentVersion();
        try {
            int ver = Integer.parseInt(m.group(1));
            return keyService.getVersion(ver);
        } catch (Exception e) {
            logger.warn("Failed to parse key version from context '{}'", context, e);
            return keyService.getCurrentVersion();
        }
    }

    private static double l2(double[] a, double[] b) {
        if (a.length != b.length) throw new IllegalArgumentException("Vector dimension mismatch: " + a.length + " vs " + b.length);
        double s = 0;
        for (int i = 0; i < a.length; i++) { double d = a[i] - b[i]; s += d * d; }
        return Math.sqrt(s);
    }

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(QueryToken baseToken, int queryIndex, GroundtruthManager gt) {
        Objects.requireNonNull(baseToken, "baseToken");
        List<Integer> topKVariants = List.of(1, 20, 40, 60, 80, 100);
        List<QueryEvaluationResult> out = new ArrayList<>(topKVariants.size());

        for (int k : topKVariants) {
            final QueryToken variant = (tokenFactory != null)
                    ? tokenFactory.derive(baseToken, k)
                    : new QueryToken(
                    deepCopy(baseToken.getTableBuckets()),
                    baseToken.getIv(),
                    baseToken.getEncryptedQuery(),
                    baseToken.getPlaintextQuery(),
                    k,
                    baseToken.getNumTables(),
                    baseToken.getEncryptionContext(),
                    baseToken.getDimension(),
                    baseToken.getVersion()
            );

            long t0 = System.nanoTime();
            List<QueryResult> retrieved = search(variant);
            long queryDurationMs = (System.nanoTime() - t0) / 1_000_000;

            // true candidate count would require an IndexService API; proxy with returned size for now.
            final int candidateCount;
            if (indexService instanceof SecureLSHIndexService
                    && "partitioned".equalsIgnoreCase(SecureLSHIndexService.getMode())) {
                // In partitioned mode, the engine already executed the lookup — don't do it twice.
                candidateCount = retrieved.size();
            } else {
                candidateCount = indexService.candidateCount(variant);
            }

            EncryptedPointBuffer buf = indexService.getPointBuffer();
            long insertTimeMs = (buf != null) ? buf.getLastBatchInsertTimeMs() : 0;
            int totalFlushed   = (buf != null) ? buf.getTotalFlushedPoints()   : 0;
            int flushThreshold = (buf != null) ? buf.getFlushThreshold()        : 0;

            int tokenSizeBytes = QueryServiceImpl.estimateTokenSizeBytes(baseToken);
            int vectorDim = variant.getDimension();

            // Groundtruth
            int[] truth = (gt != null) ? safeGt(gt, queryIndex, k) : new int[0];
            Set<String> truthSet = Arrays.stream(truth).mapToObj(String::valueOf).collect(Collectors.toSet());

            int retrievedCount = retrieved.size();
            long matchCount = retrieved.stream().map(QueryResult::getId).filter(truthSet::contains).count();

            double ratio  = (retrievedCount == 0) ? 0.0 : matchCount / (double) retrievedCount;
            // Recall is now a dataset-level statistic, not per-query/k. Mark as NaN for the row.
            double recall = Double.NaN;

            out.add(new QueryEvaluationResult(
                    k, retrievedCount, ratio, recall, queryDurationMs, insertTimeMs,
                    candidateCount, tokenSizeBytes, vectorDim, totalFlushed, flushThreshold
            ));
        }
        return out;
    }

    private static List<List<Integer>> deepCopy(List<List<Integer>> src) {
        List<List<Integer>> out = new ArrayList<>(src.size());
        for (List<Integer> l : src) out.add(new ArrayList<>(l));
        return out;
    }

    private static int[] safeGt(GroundtruthManager gt, int idx, int k) {
        try {
            int[] arr = gt.getGroundtruth(idx, k);
            return (arr != null) ? arr : new int[0];
        } catch (Exception e) {
            return new int[0];
        }
    }

    public static int estimateTokenSizeBytes(QueryToken t) {
        int bytes = 0;
        bytes += (t.getIv() != null) ? t.getIv().length : 0;
        bytes += (t.getEncryptedQuery() != null) ? t.getEncryptedQuery().length : 0;
        // rough accounting for per-table buckets (ints = 4 bytes)
        int bucketCount = 0;
        for (List<Integer> l : t.getTableBuckets()) bucketCount += l.size();
        bytes += bucketCount * Integer.BYTES;
        return bytes;
    }

    public long getLastQueryDurationNs() {
        return lastQueryDurationNs;
    }
}
