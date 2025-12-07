package com.fspann.query.core;

import com.fspann.common.Profiler;
import java.util.*;

/**
 * Aggregates extracted from Profiler.QueryRow for EvaluationSummaryPrinter.
 */
public final class Aggregates {
    public double avgRatio;
    public double avgServerMs;
    public double avgClientMs;
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

    public Map<Integer, Double> precisionAtK = new HashMap<>();

    public Aggregates() {}

    public double getAvgArtMs() {
        return avgServerMs + avgClientMs;
    }

    public static Aggregates fromProfiler(Profiler p) {
        Aggregates a = new Aggregates();

        var rows = p.getQueryRows();
        if (rows.isEmpty()) return a;

        double sumRatio = 0, sumServer = 0, sumClient = 0, sumDecrypt = 0;
        double sumTokenBytes = 0, sumWorkUnits = 0;
        double sumCandT = 0, sumCandK = 0, sumCandD = 0, sumRet = 0;

        int count = rows.size();

        Map<Integer, List<Double>> pAtK = new HashMap<>();

        for (var r : rows) {
            sumRatio   += r.ratio;
            sumServer  += r.serverMs;
            sumClient  += r.clientMs;
            sumDecrypt += r.decryptMs;

            sumTokenBytes += r.tokenBytes;
            sumWorkUnits  += r.vectorDim; // define work-units = dimension, Option-C compliant

            sumCandT += r.candTotal;
            sumCandK += r.candKept;
            sumCandD += r.candDecrypted;
            sumRet   += r.candReturned;

            // Extract precision@K from mode=full rows only
            if ("full".equalsIgnoreCase(r.mode)) {
                pAtK.computeIfAbsent(r.tokenK, z -> new ArrayList<>()).add(r.precision);
            }

            // collect re-encryption
            a.reencryptCount += r.reencCount;
            a.reencryptBytes += r.reencBytesDelta;
            a.reencryptMs    += r.reencTimeMs;
        }

        a.avgRatio   = sumRatio / count;
        a.avgServerMs = sumServer / count;
        a.avgClientMs = sumClient / count;
        a.avgDecryptMs = sumDecrypt / count;
        a.avgTokenBytes = sumTokenBytes / count;
        a.avgWorkUnits  = sumWorkUnits / count;

        a.avgCandTotal = sumCandT / count;
        a.avgCandKept = sumCandK / count;
        a.avgCandDecrypted = sumCandD / count;
        a.avgReturned = sumRet / count;

        // average P@K
        for (var e : pAtK.entrySet()) {
            a.precisionAtK.put(e.getKey(),
                    e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0));
        }

        return a;
    }
}
