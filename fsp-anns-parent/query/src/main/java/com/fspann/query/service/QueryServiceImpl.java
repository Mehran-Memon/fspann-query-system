package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.common.IndexService;
import com.fspann.loader.GroundtruthManager;
import javax.crypto.SecretKey;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fspann.query.core.QueryEvaluationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServiceImpl implements QueryService {
    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);
    private static final Pattern VERSION_PATTERN = Pattern.compile("epoch_(\\d+)_dim_(\\d+)$");
    private static final int SMALL_TOPK_THRESHOLD = 10;

    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private long lastQueryDurationNs = 0;

    public QueryServiceImpl(IndexService indexService, CryptoService cryptoService, KeyLifeCycleService keyService) {
        this.indexService = Objects.requireNonNull(indexService, "IndexService cannot be null");
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService cannot be null");
        this.keyService = Objects.requireNonNull(keyService, "KeyLifeCycleService cannot be null");
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        long startTime = System.nanoTime();
        Objects.requireNonNull(token, "QueryToken cannot be null");

        if (token.getTopK() <= 0 || token.getEncryptedQuery() == null || token.getIv() == null) {
            throw new IllegalArgumentException("Invalid or incomplete QueryToken.");
        }

        keyService.rotateIfNeeded();
        KeyVersion queryVersion = resolveKeyVersion(token.getEncryptionContext());
        SecretKey key = queryVersion.getKey();

        double[] queryVec;
        try {
            queryVec = cryptoService.decryptQuery(token.getEncryptedQuery(), token.getIv(), key);
        } catch (Exception e) {
            logger.error("Failed to decrypt query vector", e);
            throw new RuntimeException("Decryption error: query vector", e);
        }

        List<EncryptedPoint> candidates = indexService.lookup(token);
        if (candidates.isEmpty()) {
            logger.debug("No candidates retrieved for query token version {}", queryVersion.getVersion());
            lastQueryDurationNs = System.nanoTime() - startTime;
            return List.of();
        }

        List<QueryResult> results;
        if (token.getTopK() <= SMALL_TOPK_THRESHOLD) {
            results = new ArrayList<>();
            for (EncryptedPoint pt : candidates) {
                if (pt.getVersion() != queryVersion.getVersion()) {
                    logger.warn("Skipping candidate {} with mismatched version {}", pt.getId(), pt.getVersion());
                    continue;
                }
                try {
                    double[] ptVec = cryptoService.decryptFromPoint(pt, key);
                    double dist = computeDistance(queryVec, ptVec);
                    results.add(new QueryResult(pt.getId(), dist));
                } catch (Exception e) {
                    logger.warn("Skipped candidate {} due to decryption failure", pt.getId(), e);
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
                    logger.warn("Skipping candidate {} with mismatched version {}", pt.getId(), pt.getVersion());
                    continue;
                }
                try {
                    double[] ptVec = cryptoService.decryptFromPoint(pt, key);
                    double dist = computeDistance(queryVec, ptVec);
                    QueryResult result = new QueryResult(pt.getId(), dist);
                    topKQueue.offer(result);
                    if (topKQueue.size() > token.getTopK()) {
                        topKQueue.poll();
                    }
                } catch (Exception e) {
                    logger.warn("Skipped candidate {} due to decryption failure", pt.getId(), e);
                }
            }
            results = new ArrayList<>(topKQueue);
            results.sort(Comparator.naturalOrder());
        }

        logger.debug("Query processed: version={}, candidates={}, results={}",
                queryVersion.getVersion(), candidates.size(), results.size());
        lastQueryDurationNs = System.nanoTime() - startTime;
        return results;
    }

    private KeyVersion resolveKeyVersion(String context) {
        try {
            Matcher matcher = VERSION_PATTERN.matcher(context);
            if (!matcher.matches()) {
                logger.warn("Invalid encryption context format: {}, using current version", context);
                return keyService.getCurrentVersion();
            }
            int version = Integer.parseInt(matcher.group(1));
            return keyService.getVersion(version);
        } catch (Exception e) {
            logger.warn("Failed to parse encryption context: {}, using current version", context, e);
            return keyService.getCurrentVersion();
        }
    }

    private double computeDistance(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vector dimension mismatch: " + a.length + " vs " + b.length);
        }
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(
            QueryToken baseToken,
            int queryIndex,
            GroundtruthManager groundtruthManager
    ) {
        Objects.requireNonNull(baseToken, "Base token cannot be null");
        Objects.requireNonNull(groundtruthManager, "GroundtruthManager cannot be null");
        List<Integer> topKVariants = List.of(1, 20, 40, 60, 80, 100);
        List<QueryEvaluationResult> results = new ArrayList<>();

        for (int k : topKVariants) {
            QueryToken token = new QueryToken(
                    baseToken.getCandidateBuckets(),
                    baseToken.getIv(),
                    baseToken.getEncryptedQuery(),
                    baseToken.getPlaintextQuery(),
                    k,
                    baseToken.getNumTables(),
                    baseToken.getEncryptionContext(),
                    baseToken.getDimension(),
                    baseToken.getShardId(),
                    baseToken.getVersion()
            );

            long start = System.nanoTime();
            List<QueryResult> retrieved = search(token);
            long duration = System.nanoTime() - start;

            int[] groundtruth = groundtruthManager.getGroundtruth(queryIndex, k);
            Set<String> truthIds = Arrays.stream(groundtruth)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.toSet());

            long matchCount = retrieved.stream()
                    .map(QueryResult::getId)
                    .filter(truthIds::contains)
                    .count();

            double ratio = (double) retrieved.size() / k;
            double recall = (double) matchCount / k;

            results.add(new QueryEvaluationResult(
                    k,
                    retrieved.size(),
                    ratio,
                    recall,
                    duration / 1_000_000
            ));
        }

        return results;
    }

    public long getLastQueryDurationNs() {
        return lastQueryDurationNs;
    }
}