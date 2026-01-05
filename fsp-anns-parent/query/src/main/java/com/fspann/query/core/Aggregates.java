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

    public static Aggregates fromProfiler(Profiler p) {
        Aggregates a = new Aggregates();

        List<Profiler.QueryRow> rows = p.getQueryRows();
        if (rows.isEmpty()) return a;

        int count = rows.size();
        int runCount = 0;

        double sumDistanceRatio = 0.0;      // ← Paper ratio
        int distanceRatioCount = 0;

        double sumCandidateRatio = 0.0;     // ← Search efficiency
        int candidateRatioCount = 0;

        double sumRecall = 0.0;
        int recallCount = 0;

        double sumServer = 0.0;
        double sumClient = 0.0;
        double sumRun = 0.0;
        double sumDecrypt = 0.0;

        double sumTokenBytes = 0.0;
        double sumWorkUnits = 0.0;

        double sumCandT = 0.0;
        double sumCandK = 0.0;
        double sumCandD = 0.0;
        double sumRet   = 0.0;

        Map<Integer, List<Double>> rAtK = new HashMap<>();
        Map<Integer, List<Double>> drAtK = new HashMap<>();  // ← Distance ratio@K

        for (Profiler.QueryRow r : rows) {

            // ---------- Distance Ratio (PAPER METRIC, K >= 20 only) ----------
            if (r.tokenK >= 20 && Double.isFinite(r.distanceRatio) && r.distanceRatio > 0) {
                sumDistanceRatio += r.distanceRatio;
                distanceRatioCount++;

                drAtK.computeIfAbsent(r.tokenK, k -> new ArrayList<>())
                        .add(r.distanceRatio);
            }

            // ---------- Candidate Ratio (SEARCH EFFICIENCY) ----------
            if (Double.isFinite(r.candidateRatio)) {
                sumCandidateRatio += r.candidateRatio;
                candidateRatioCount++;
            }

            // ---------- Timing ----------
            sumServer  += nz(r.serverMs);
            sumClient  += nz(r.clientMs);
            sumDecrypt += nz(r.decryptMs);

            if (r.runMs > 0) {
                sumRun += r.runMs;
                runCount++;
            }

            // ---------- Token / Work ----------
            sumTokenBytes += nz(r.tokenBytes);
            sumWorkUnits  += r.vectorDim;

            // ---------- Candidate pipeline ----------
            if (r.candTotal >= 0)     sumCandT += r.candTotal;
            if (r.candKept >= 0)      sumCandK += r.candKept;
            if (r.candDecrypted >= 0) sumCandD += r.candDecrypted;
            if (r.candReturned >= 0)  sumRet   += r.candReturned;

            // ---------- Recall@K ----------
            if (r.recall >= 0.0 && r.recall <= 1.0) {
                sumRecall += r.recall;
                recallCount++;
                if (r.tokenK >= 20) {
                    rAtK.computeIfAbsent(r.tokenK, k -> new ArrayList<>()).add(r.recall);
                }
            }

            // ---------- Re-encryption ----------
            if (r.reencCount > 0) {
                a.reencryptCount += r.reencCount;
                a.reencryptBytes += Math.max(0, r.reencBytesDelta);
                a.reencryptMs    += Math.max(0, r.reencTimeMs);
            }
        }

        // ---------- Final averages ----------
        a.avgDistanceRatio = distanceRatioCount > 0
                ? (sumDistanceRatio / distanceRatioCount)
                : Double.NaN;

        a.avgCandidateRatio = candidateRatioCount > 0
                ? (sumCandidateRatio / candidateRatioCount)
                : 0.0;

        a.avgServerMs   = sumServer / count;
        a.avgClientMs   = sumClient / count;
        a.avgRunMs      = runCount > 0 ? (sumRun / runCount) : 0.0;
        a.avgDecryptMs  = sumDecrypt / count;

        a.avgTokenBytes = sumTokenBytes / count;
        a.avgWorkUnits  = sumWorkUnits / count;

        a.avgCandTotal     = sumCandT / count;
        a.avgCandKept      = sumCandK / count;
        a.avgCandDecrypted = sumCandD / count;
        a.avgReturned      = sumRet   / count;

        // ---------- Recall@K ----------
        for (var e : rAtK.entrySet()) {
            a.recallAtK.put(
                    e.getKey(),
                    e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0)
            );
        }

        a.avgRecall = a.recallAtK.getOrDefault(10, 0.0);

        // ---------- Distance Ratio@K (PAPER METRIC) ----------
        for (var e : drAtK.entrySet()) {
            a.distanceRatioAtK.put(
                    e.getKey(),
                    e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN)
            );
        }

        return a;
    }

    private static double nz(double v) {
        return Double.isFinite(v) ? v : 0.0;
    }
}