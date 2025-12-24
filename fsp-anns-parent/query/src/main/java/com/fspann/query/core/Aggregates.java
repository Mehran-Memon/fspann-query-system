package com.fspann.query.core;

import com.fspann.common.Profiler;
import java.util.*;

/**
 * Aggregates extracted from Profiler.QueryRow for EvaluationSummaryPrinter.
 */
public final class Aggregates {

    public double avgRatio;
    public double avgPrecision;         // NEW: direct average precision
    public double avgServerMs;
    public double avgClientMs;
    public double avgRunMs;             // TRUE ART
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

    /** Precision broken down by K value */
    public Map<Integer, Double> precisionAtK = new HashMap<>();

    public Aggregates() {}

    /** ART = average runMs (end-to-end), NOT server+client */
    public double getAvgArtMs() {
        return avgRunMs;
    }

    public static Aggregates fromProfiler(Profiler p) {
        Aggregates a = new Aggregates();

        List<Profiler.QueryRow> rows = p.getQueryRows();
        if (rows.isEmpty()) return a;

        int count = rows.size();
        int runCount = 0;

        double sumRatio = 0;
        double sumPrecision = 0;
        double sumServer = 0;
        double sumClient = 0;
        double sumRun = 0;
        double sumDecrypt = 0;

        double sumTokenBytes = 0;
        double sumWorkUnits = 0;

        double sumCandT = 0;
        double sumCandK = 0;
        double sumCandD = 0;
        double sumRet   = 0;

        int precisionCount = 0;
        Map<Integer, List<Double>> pAtK = new HashMap<>();

        for (Profiler.QueryRow r : rows) {

            // ---------------- Core metrics ----------------
            sumRatio   += nz(r.ratio);
            sumServer  += nz(r.serverMs);
            sumClient  += nz(r.clientMs);
            sumDecrypt += nz(r.decryptMs);

            if (r.runMs > 0) {
                sumRun += r.runMs;
                runCount++;
            }
            sumTokenBytes += nz(r.tokenBytes);
            sumWorkUnits  += r.vectorDim;   // Option-C definition

            // ---------------- Candidate pipeline ----------------
            if (r.candTotal >= 0)      sumCandT += r.candTotal;
            if (r.candKept >= 0)       sumCandK += r.candKept;
            if (r.candDecrypted >= 0)  sumCandD += r.candDecrypted;
            if (r.candReturned >= 0)   sumRet   += r.candReturned;

            // ---------------- Precision@K (FIXED: always aggregate) ----------------
            double prec = nz(r.precision);
            if (prec >= 0 && prec <= 1.0) {
                sumPrecision += prec;
                precisionCount++;

                // Also store by K for detailed breakdown
                if (r.tokenK > 0) {
                    pAtK.computeIfAbsent(r.tokenK, z -> new ArrayList<>()).add(prec);
                }
            }

            // ---------------- Re-encryption (clamped) ----------------
            if (r.reencCount > 0) {
                a.reencryptCount += r.reencCount;
                a.reencryptBytes += Math.max(0, r.reencBytesDelta);
                a.reencryptMs    += Math.max(0, r.reencTimeMs);
            }
        }

        // ---------------- Averages ----------------
        a.avgRatio       = sumRatio / count;
        a.avgPrecision   = precisionCount > 0 ? sumPrecision / precisionCount : 0.0;
        a.avgServerMs    = sumServer / count;
        a.avgClientMs    = sumClient / count;
        a.avgRunMs       = runCount > 0 ? sumRun / runCount : 0.0;
        a.avgDecryptMs   = sumDecrypt / count;

        a.avgTokenBytes  = sumTokenBytes / count;
        a.avgWorkUnits   = sumWorkUnits / count;

        a.avgCandTotal      = sumCandT / count;
        a.avgCandKept       = sumCandK / count;
        a.avgCandDecrypted  = sumCandD / count;
        a.avgReturned       = sumRet   / count;

        // ---------------- Avg Precision@K by K value ----------------
        for (var e : pAtK.entrySet()) {
            a.precisionAtK.put(
                    e.getKey(),
                    e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0)
            );
        }

        return a;
    }

    private static double nz(double v) {
        return Double.isFinite(v) ? v : 0.0;
    }
}