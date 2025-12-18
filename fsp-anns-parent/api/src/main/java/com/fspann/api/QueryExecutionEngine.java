package com.fspann.api;

import com.fspann.common.Profiler;
import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.service.QueryServiceImpl;

import java.nio.file.Path;
import java.util.*;

/**
 * Unified evaluation engine.
 *
 * Guarantees:
 *  - Peng-consistent timing split (server / client / ART)
 *  - ratio@K = refined / K
 *  - selective re-encryption AFTER search
 *  - profiler emits exactly one row per query (kMax)
 */
public final class QueryExecutionEngine {

    private final ForwardSecureANNSystem sys;
    private final Profiler profiler;
    private final int[] K_VARIANTS;

    public QueryExecutionEngine(
            ForwardSecureANNSystem sys,
            Profiler profiler,
            int[] kVariants
    ) {
        this.sys = Objects.requireNonNull(sys);
        this.profiler = profiler;
        this.K_VARIANTS = Objects.requireNonNull(kVariants);
    }

    /* ======================================================================
     * SIMPLE QUERY (no GT, no ratios)
     * ====================================================================== */

    public List<QueryResult> evalSimple(double[] q, int topK, int dim, boolean cloak) {
        Objects.requireNonNull(q);

        final long totalStart = System.nanoTime();

        QueryToken token = cloak
                ? sys.cloakQuery(q, dim, topK)
                : sys.getFactoryForDim(dim).create(q, topK);

        sys.recordRecentVector(q);

        final String cacheKey = sys.getCacheKeyOf(token);
        Map<String, List<QueryResult>> cache = sys.getQueryCache();

        List<QueryResult> cached = cache.get(cacheKey);
        if (cached != null) {
            sys.addQueryTime(System.nanoTime() - totalStart);
            return cached;
        }

        QueryServiceImpl qs = sys.getQueryServiceImpl();
        List<QueryResult> ret = qs.search(token);

        long serverNs = qs.getLastQueryDurationNs();
        long totalNs  = System.nanoTime() - totalStart;
        long clientNs = Math.max(0L, totalNs - serverNs);

        cache.put(cacheKey, ret);
        sys.addQueryTime(serverNs + clientNs);

        return ret;
    }

    /* ======================================================================
     * FULL BATCH EVALUATION (Peng-style)
     * ====================================================================== */

    public void evalBatch(
            List<double[]> queries,
            int dim,
            GroundtruthManager gt,
            Path outDir,
            boolean gtTrusted
    ) {
        if (queries == null || queries.isEmpty()) return;

        QueryServiceImpl qs = sys.getQueryServiceImpl();
        int kMax = Arrays.stream(K_VARIANTS).max().orElse(100);

        for (int qIndex = 0; qIndex < queries.size(); qIndex++) {

            double[] q = queries.get(qIndex);

            /* --------------------------------------------------------------
             * 0) Optional K-adaptive probe-only widening
             * -------------------------------------------------------------- */
            if (sys.kAdaptiveProbeEnabled()) {
                sys.runKAdaptiveProbeOnly(qIndex, q, dim, qs);
                sys.resetProbeShards();
            }

            /* --------------------------------------------------------------
             * 1) Per-K evaluation loop
             * -------------------------------------------------------------- */
            List<QueryEvaluationResult> perK = new ArrayList<>(K_VARIANTS.length);

            boolean firstRun = true;

            int candTotal = 0;
            int candKept = 0;
            int candDec = 0;
            int returned = 0;

            int touchedCount = 0;
            int reencCount = 0;
            long reencMs = 0;
            long reencDelta = 0;
            long reencAfter = 0;

            for (int k : K_VARIANTS) {

                final long start = System.nanoTime();

                QueryToken tokK = sys.getFactoryForDim(dim).create(q, k);
                List<QueryResult> retK = qs.search(tokK);

                final long end = System.nanoTime();

                long serverNs = qs.getLastQueryDurationNs();
                long totalNs  = end - start;
                long clientNs = Math.max(0L, totalNs - serverNs);

                // capture once (kMax semantics)
                if (firstRun) {
                    candTotal = qs.getLastCandTotal();
                    candKept  = qs.getLastCandKeptVersion();
                    candDec   = qs.getLastCandDecrypted();
                    returned = qs.getLastReturned();

                    // selective re-encryption AFTER first real search
                    ForwardSecureANNSystem.ReencOutcome ro =
                            sys.doReencrypt("Q" + qIndex, qs);

                    touchedCount = Math.max(0, ro.cumulativeUnique);
                    reencDelta  = ro.rep.getBytesDelta();
                    reencAfter  = ro.rep.getBytesAfter();
                    reencCount = ro.rep.getReencryptedCount();
                    reencMs    = ro.rep.getTimeMs();

                    // ART accumulation once per query
                    sys.addQueryTime(serverNs + clientNs);

                    firstRun = false;
                }

                int upto = Math.min(k, retK.size());
                List<QueryResult> prefix = retK.subList(0, upto);

                double ratio = (k > 0)
                        ? (candDec / (double) k)
                        : 0.0;

                int[] truth = gt.getGroundtruth(qIndex, k);
                double precision = sys.computePrecision(prefix, truth);

                perK.add(new QueryEvaluationResult(
                        k,
                        upto,
                        ratio,
                        precision,
                        Math.round(serverNs / 1e6),
                        Math.round(clientNs / 1e6),
                        Math.round((serverNs + clientNs) / 1e6),
                        Math.round(qs.getLastDecryptNs() / 1e6),
                        sys.lastInsertMs(),
                        candTotal,
                        candKept,
                        candDec,
                        returned,
                        tokK.estimateSerializedSizeBytes(),
                        dim,
                        sys.totalFlushed(),
                        sys.flushThreshold(),
                        touchedCount,
                        reencCount,
                        reencMs,
                        reencDelta,
                        reencAfter,
                        "refine/K",
                        k,
                        kMax,
                        qIndex,
                        "full"
                ));
            }

            /* --------------------------------------------------------------
             * 2) PROFILER EMISSION (kMax only)
             * -------------------------------------------------------------- */
            if (profiler != null && !perK.isEmpty()) {

                QueryEvaluationResult maxRow =
                        perK.stream()
                                .filter(r -> r.getTopKRequested() == kMax)
                                .findFirst()
                                .orElse(perK.get(perK.size() - 1));

                profiler.recordQueryRow(
                        "Q" + qIndex,
                        maxRow.getTimeMs(),
                        maxRow.getClientTimeMs(),
                        maxRow.getRunTimeMs(),
                        maxRow.getDecryptTimeMs(),
                        maxRow.getInsertTimeMs(),

                        maxRow.getRatio(),
                        maxRow.getPrecision(),

                        candTotal,
                        candKept,
                        candDec,
                        returned,

                        maxRow.getTokenSizeBytes(),
                        dim,
                        kMax,
                        kMax,
                        qIndex,

                        sys.totalFlushed(),
                        sys.flushThreshold(),

                        touchedCount,
                        reencCount,
                        reencMs,
                        reencDelta,
                        reencAfter,

                        "refine/K",
                        "full",

                        sys.getLastStabilizedRaw(),
                        sys.getLastStabilizedFinal()
                );
            }
        }
    }
}
