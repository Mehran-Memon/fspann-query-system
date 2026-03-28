package com.fspann.query.core;

import com.fspann.common.Profiler;
import java.util.*;

/**
 * Aggregates extracted from Profiler.QueryRow for EvaluationSummaryPrinter.
 */
public final class Aggregates {

    // PRIMARY (paper metric)
    public double avgDistanceRatio;         // ← Paper ratio (quality)

    // SECONDARY (search efficiency)
    public double avgCandidateRatio;        // ← Search cost

    public double avgRecall;
    public double avgServerMs;
    public double avgClientMs;
    public double avgRunMs;
    public double avgDecryptMs;

    public double avgTokenBytes;
    public double avgWorkUnits;

    public double avgRefinementLimit;

    public double avgCandTotal;
    public double avgCandKept;
    public double avgCandDecrypted;
    public double avgReturned;

    public int reencryptCount;
    public long reencryptBytes;
    public double reencryptMs;

    public long spaceMetaBytes;
    public long spacePointsBytes;

    public Map<Integer, Double> recallAtK = new HashMap<>();
    public Map<Integer, Double> distanceRatioAtK = new HashMap<>();   // ← ADDED

    public Aggregates() {}

    public double getAvgArtMs() {
        return avgRunMs;
    }

    // =================== Aggregates.fromProfiler ===================
    public static Aggregates fromProfiler(Profiler p) {
        Aggregates a = new Aggregates();
        List<Profiler.QueryRow> rows = p.getQueryRows();
        if (rows.isEmpty()) return a;

        Map<Integer, List<Double>> drAtK = new HashMap<>();
        Map<Integer, List<Double>> rAtK  = new HashMap<>();

        double sumCandRatio = 0.0; int candCnt = 0;
        double sumRecall = 0.0; int recallCnt = 0;

        double sumServer = 0, sumClient = 0, sumRun = 0, sumDecrypt = 0;
        int runCnt = 0;

        double sumTokenBytes = 0;
        double sumRefLimit = 0;
        double sumCandTotal = 0;
        double sumCandKept = 0;
        double sumCandDec = 0;
        double sumReturned = 0;

        for (Profiler.QueryRow r : rows) {

            // ---- Ratio@K (paper metric) ----
            if (Double.isFinite(r.distanceRatio)) {
                drAtK.computeIfAbsent(r.tokenK, k -> new ArrayList<>())
                        .add(r.distanceRatio);
            }

            // ---- Candidate expansion factor ----
            if (Double.isFinite(r.candidateRatio)) {
                sumCandRatio += r.candidateRatio;
                candCnt++;
            }

            // ---- Recall ----
            if (r.recall >= 0 && r.recall <= 1.0) {
                sumRecall += r.recall;
                recallCnt++;
                rAtK.computeIfAbsent(r.tokenK, k -> new ArrayList<>())
                        .add(r.recall);
            }

            // ---- Timings ----
            sumServer  += nz(r.serverMs);
            sumClient  += nz(r.clientMs);
            sumDecrypt += nz(r.decryptMs);
            if (r.runMs > 0) { sumRun += r.runMs; runCnt++; }

            // ---- Costs / counts ----
            sumTokenBytes += r.tokenBytes;
            sumRefLimit   += r.refinementLimit;
            sumCandTotal  += r.candTotal;
            sumCandKept   += r.candKept;
            sumCandDec    += r.candDecrypted;
            sumReturned   += r.candReturned;
        }

        // ---- Per-K aggregates ----
        for (var e : drAtK.entrySet()) {
            a.distanceRatioAtK.put(
                    e.getKey(),
                    e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN)
            );
        }

        for (var e : rAtK.entrySet()) {
            a.recallAtK.put(
                    e.getKey(),
                    e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0)
            );
        }

        // ---- Headline paper metrics ----
        a.avgDistanceRatio =
                a.distanceRatioAtK.getOrDefault(100, Double.NaN);

        a.avgRecall         = a.recallAtK.getOrDefault(10, 0.0);
        a.avgCandidateRatio = candCnt > 0 ? sumCandRatio / candCnt : 0.0;

        int n = rows.size();

        a.avgServerMs        = sumServer / n;
        a.avgClientMs        = sumClient / n;
        a.avgDecryptMs       = sumDecrypt / n;
        a.avgRunMs           = runCnt > 0 ? sumRun / runCnt : 0.0;

        a.avgTokenBytes      = sumTokenBytes / n;
        a.avgRefinementLimit = sumRefLimit / n;
        a.avgCandTotal       = sumCandTotal / n;
        a.avgCandKept        = sumCandKept / n;
        a.avgCandDecrypted   = sumCandDec / n;
        a.avgReturned        = sumReturned / n;

        a.avgWorkUnits       = 0.0; // intentionally unused

        return a;
    }

    private static double nz(double v) {
        return Double.isFinite(v) ? v : 0.0;
    }
}