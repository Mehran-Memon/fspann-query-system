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
    private final QueryTokenFactory tokenFactory; // may be null
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
        this.tokenFactory = tokenFactory; // can be null -> fallback path
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        long start = System.nanoTime();
        Objects.requireNonNull(token, "QueryToken cannot be null");

        if (token.getTopK() <= 0 || token.getEncryptedQuery() == null || token.getIv() == null) {
            throw new IllegalArgumentException("Invalid or incomplete QueryToken");
        }

        keyService.rotateIfNeeded();
        KeyVersion queryVersion = resolveKeyVersion(token.getEncryptionContext());
        SecretKey key = queryVersion.getKey();

        double[] queryVec;
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

        List<QueryResult> results;
        if (token.getTopK() <= SMALL_TOPK_THRESHOLD) {
            results = new ArrayList<>();
            for (EncryptedPoint pt : candidates) {
                if (pt.getVersion() != queryVersion.getVersion()) {
                    logger.debug("Skip mismatched version: pt.version={} != token.version={}", pt.getVersion(), queryVersion.getVersion());
                    continue;
                }
                try {
                    double[] ptVec = cryptoService.decryptFromPoint(pt, key);
                    double dist = computeDistance(queryVec, ptVec);
                    results.add(new QueryResult(pt.getId(), dist));
                } catch (IllegalArgumentException e) {
                    throw e; // propagate to tests
                } catch (Exception e) {
                    logger.warn("Failed to decrypt candidate {}. Skipping.", pt.getId(), e);
                }
            }
            results.sort(Comparator.naturalOrder());
            if (results.size() > token.getTopK()) {
                results = results.subList(0, token.getTopK());
            }
        } else {
            PriorityQueue<QueryResult> topKQueue = new PriorityQueue<>(Comparator.reverseOrder());
            for (EncryptedPoint pt : candidates) {
                if (pt.getVersion() != queryVersion.getVersion()) {
                    logger.debug("Skip mismatched version: pt.version={} != token.version={}", pt.getVersion(), queryVersion.getVersion());
                    continue;
                }
                try {
                    double[] ptVec = cryptoService.decryptFromPoint(pt, key);
                    double dist = computeDistance(queryVec, ptVec);
                    topKQueue.offer(new QueryResult(pt.getId(), dist));
                    if (topKQueue.size() > token.getTopK()) {
                        topKQueue.poll(); // remove the worst
                    }
                } catch (Exception e) {
                    logger.warn("Failed to decrypt candidate {}. Skipping.", pt.getId(), e);
                }
            }
            results = new ArrayList<>(topKQueue);
            results.sort(Comparator.naturalOrder());
        }

        lastQueryDurationNs = System.nanoTime() - start;
        logger.debug("Query version={}, retrieved={}, returned={}, time={}ms",
                queryVersion.getVersion(), candidates.size(), results.size(),
                lastQueryDurationNs / 1_000_000);
        return results;
    }

    private KeyVersion resolveKeyVersion(String context) {
        Matcher matcher = VERSION_PATTERN.matcher(context);
        if (!matcher.matches()) {
            logger.warn("Invalid encryption context: '{}'. Using current key version.", context);
            return keyService.getCurrentVersion();
        }
        try {
            int version = Integer.parseInt(matcher.group(1));
            return keyService.getVersion(version);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse version from context '{}'. Using current version.", context, e);
            return keyService.getCurrentVersion();
        }
    }

    private double computeDistance(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vector dimension mismatch: " + a.length + " vs " + b.length);
        }
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(
            QueryToken baseToken,
            int queryIndex,
            GroundtruthManager groundtruthManager
    ) {
        Objects.requireNonNull(baseToken);
        Objects.requireNonNull(groundtruthManager);

        List<Integer> topKVariants = List.of(1, 20, 40, 60, 80, 100);
        List<QueryEvaluationResult> results = new ArrayList<>();

        for (int k : topKVariants) {
            final QueryToken variant;
            if (tokenFactory != null) {
                variant = tokenFactory.derive(baseToken, k); // recompute per-table expansions per K
            } else if (baseToken.hasPerTable()) {
                variant = new QueryToken(
                        baseToken.getTableBuckets(),
                        baseToken.getIv(),
                        baseToken.getEncryptedQuery(),
                        baseToken.getPlaintextQuery(),
                        k,
                        baseToken.getNumTables(),
                        baseToken.getEncryptionContext(),
                        baseToken.getDimension(),
                        baseToken.getVersion()
                );
            } else {
                // legacy: keep same flat buckets (index service will expand internally if needed)
                variant = new QueryToken(
                        baseToken.getCandidateBuckets(),
                        baseToken.getIv(),
                        baseToken.getEncryptedQuery(),
                        baseToken.getPlaintextQuery(),
                        k,
                        baseToken.getNumTables(),
                        baseToken.getEncryptionContext(),
                        baseToken.getDimension(),
                        /*shard*/ 0,
                        baseToken.getVersion()
                );
            }

            long start = System.nanoTime();
            List<QueryResult> retrieved = search(variant);
            long durationMs = (System.nanoTime() - start) / 1_000_000;

            int[] truth = groundtruthManager.getGroundtruth(queryIndex, k);
            // groundtruth ids are integer doc ids; our retrieved ids are strings –
            // adapting this mapping if our ids are not exactly these ints as strings.
            Set<String> truthSet = Arrays.stream(truth).mapToObj(String::valueOf).collect(Collectors.toSet());

            long matchCount = retrieved.stream()
                    .map(QueryResult::getId)
                    .filter(truthSet::contains)
                    .count();

            double ratio  = matchCount / (double) k;                  // “precision at K”
            double recall = truthSet.isEmpty() ? 0.0 : ratio;         // if GT@K is K, recall==precision@K


            results.add(new QueryEvaluationResult(k, retrieved.size(), ratio, recall, durationMs));
        }

        return results;
    }

    public long getLastQueryDurationNs() {
        return lastQueryDurationNs;
    }
}
