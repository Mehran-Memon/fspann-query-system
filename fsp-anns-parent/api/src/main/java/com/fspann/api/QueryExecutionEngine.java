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
 * Uses ONLY the public façade methods exposed by ForwardSecureANNSystem.
 * No private-member access.
 */
public final class QueryExecutionEngine {

    private final ForwardSecureANNSystem sys;
    private final Profiler profiler;
    private final int[] K_VARIANTS;

    public QueryExecutionEngine(ForwardSecureANNSystem sys,
                                Profiler profiler,
                                int[] kVariants) {
        this.sys = Objects.requireNonNull(sys);
        this.profiler = profiler;
        this.K_VARIANTS = Objects.requireNonNull(kVariants);
    }

    /* ====================================================================== */
    /*                           SIMPLE QUERY                                 */
    /* ====================================================================== */

    /**
     * Simple query:
     *   – no GT
     *   – no K variants
     *   – no ratios
     * Uses façade API only.
     */
    public List<QueryResult> evalSimple(double[] q, int topK, int dim, boolean cloak) {
        Objects.requireNonNull(q);

        long clientStart = System.nanoTime();

        QueryToken token = cloak
                ? sys.cloakQuery(q, dim, topK)
                : sys.getFactoryForDim(dim).create(q, topK);

        sys.recordRecentVector(q);

        final String cacheKey = sys.getCacheKeyOf(token);
        Map<String, List<QueryResult>> cache = sys.getQueryCache();

        // Cache hit
        List<QueryResult> cached = cache.get(cacheKey);
        if (cached != null) {
            long end = System.nanoTime();
            sys.addQueryTime(end - clientStart);
            return cached;
        }

        // Miss → real search
        QueryServiceImpl qs = sys.getQueryServiceImpl();
        List<QueryResult> ret = qs.search(token);
        long clientEnd = System.nanoTime();

        cache.put(cacheKey, ret);
        sys.addQueryTime((clientEnd - clientStart) + qs.getLastQueryDurationNs());

        return ret;
    }

    /* ====================================================================== */
    /*                   FULL BATCH EVALUATION (K-variants)                    */
    /* ====================================================================== */

    /**
     * Full evaluation with:
     *   – per-K ratio@K (SANNP)
     *   – per-K precision@K (PP-ANN)
     *   – selective re-encryption
     *   – unified row emission to profiler
     */
    public void evalBatch(List<double[]> queries,
                          int dim,
                          GroundtruthManager gt,
                          Path outDir,
                          boolean gtTrusted) {

        if (queries == null || queries.isEmpty()) return;

        QueryServiceImpl qs = sys.getQueryServiceImpl();

        for (int qIndex = 0; qIndex < queries.size(); qIndex++) {

            double[] q = queries.get(qIndex);

            // --------------------------------------------------------------
            // 0) K-adaptive probe-only widening (for ablation)
            // --------------------------------------------------------------
            if (sys.kAdaptiveProbeEnabled()) {
                sys.runKAdaptiveProbeOnly(qIndex, q, dim, qs);
                sys.resetProbeShards();                  // << EXACT location
            }

            /* --------------------------------------------------------------
             * 1) Token at maximum K
             * -------------------------------------------------------------- */
            sys.setStabilizationStats(0,0);   // reset for new query
            int baseK = sys.baseKForToken();
            QueryToken baseTok = sys.getFactoryForDim(dim).create(q, baseK);


            /* --------------------------------------------------------------
             * 2) Run search
             * -------------------------------------------------------------- */
            long clientStart = System.nanoTime();
            List<QueryResult> ret = qs.search(baseTok);
            long clientEnd = System.nanoTime();

            double serverMs = sys.boundedServerMs(qs, clientStart, clientEnd);
            double clientMs = (clientEnd - clientStart) / 1_000_000.0;
            double runMs = serverMs + clientMs;
            double decryptMs = qs.getLastDecryptNs() / 1_000_000.0;

            int candTotal = qs.getLastCandTotal();
            int candKept = qs.getLastCandKeptVersion();
            int candDec = qs.getLastCandDecrypted();
            int returned = qs.getLastReturned();

            sys.addQueryTime(Math.max(0L,
                    (clientEnd - clientStart) + qs.getLastQueryDurationNs()));

            /* --------------------------------------------------------------
             * 3) Selective re-encryption
             * -------------------------------------------------------------- */
            ForwardSecureANNSystem.ReencOutcome ro =
                    sys.doReencrypt("Q" + qIndex, qs);

            int touchedCount = Math.max(0, ro.cumulativeUnique);
            int reencCount   = ro.rep.getReencryptedCount();
            long reencMs     = ro.rep.getTimeMs();
            long reencDelta  = ro.rep.getBytesDelta();
            long reencAfter  = ro.rep.getBytesAfter();

            /* --------------------------------------------------------------
             * 4) Additional metrics
             * -------------------------------------------------------------- */
            int tokenSizeBytes = baseTok.estimateSerializedSizeBytes();
            int vectorDim = dim;

            long insertMs = sys.lastInsertMs();
            int flushed = sys.totalFlushed();
            int flushThreshold = sys.flushThreshold();

            /* --------------------------------------------------------------
             * 5) Per-K evaluation loop
             * -------------------------------------------------------------- */
            List<QueryEvaluationResult> perK = new ArrayList<>(K_VARIANTS.length);

            for (int k : K_VARIANTS) {

                int upto = Math.min(k, ret.size());
                List<QueryResult> prefix = ret.subList(0, upto);

                // ratio@K (SANNP)
                double ratio =
                        sys.computeRatio(q, prefix, k, qIndex, gt, gtTrusted);

                // precision@K (PP-ANN)
                int[] truth = gt.getGroundtruth(qIndex, k);
                double precision = sys.computePrecision(prefix, truth);

                perK.add(new QueryEvaluationResult(
                        /* topKRequested     */ k,
                        /* retrieved         */ upto,
                        /* ratio             */ ratio,
                        /* precision         */ precision,

                        /* timeMs(server)    */ Math.round(serverMs),
                        /* clientTimeMs      */ Math.round(clientMs),
                        /* runTimeMs         */ Math.round(runMs),
                        /* decryptTimeMs     */ Math.round(decryptMs),
                        /* insertTimeMs      */ insertMs,

                        /* candTotal         */ candTotal,
                        /* candKept          */ candKept,
                        /* candDecrypted     */ candDec,
                        /* candReturned      */ returned,

                        /* tokenBytes        */ tokenSizeBytes,
                        /* vectorDim         */ vectorDim,
                        /* totalFlushed      */ flushed,
                        /* flushThreshold    */ flushThreshold,

                        /* touchedCount      */ touchedCount,
                        /* reencCount        */ reencCount,
                        /* reencTimeMs       */ reencMs,
                        /* reencBytesDelta   */ reencDelta,
                        /* reencBytesAfter   */ reencAfter,

                        /* ratioDenomSource  */ sys.ratioDenomLabelPublic(gtTrusted),

                        /* tokenK            */ baseK,
                        /* tokenKBase        */ baseK,
                        /* qIndexZeroBased   */ qIndex,
                        /* candMetricsMode   */ "full"
                ));
            }

            /* --------------------------------------------------------------
             * 6) PROFILER EMISSION (ONE ROW PER QUERY)
             * -------------------------------------------------------------- */
            if (profiler != null) {

                int kMax = Arrays.stream(K_VARIANTS).max().orElse(100);
                QueryEvaluationResult maxRow =
                        perK.stream()
                                .filter(r -> r.getTopKRequested() == kMax)
                                .findFirst()
                                .orElse(perK.get(perK.size() - 1));

                profiler.recordQueryRow(
                        "Q"+qIndex,
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

                        tokenSizeBytes,
                        vectorDim,
                        baseK,
                        baseK,
                        qIndex,

                        flushed,
                        flushThreshold,

                        touchedCount,
                        reencCount,
                        reencMs,
                        reencDelta,
                        reencAfter,

                        sys.ratioDenomLabelPublic(gtTrusted),
                        "full",

                        sys.getLastStabilizedRaw(),
                        sys.getLastStabilizedFinal()
                );
            }
        }
    }
}
