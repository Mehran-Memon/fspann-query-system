package com.fspann.common;

/**
 * Immutable per-(query, K) evaluation metrics.
 * SINGLE SOURCE OF TRUTH.
 *
 * Metrics:
 *   - distanceRatioAtK: Quality metric - average(dist_returned / dist_groundtruth) per position
 *   - precisionAtK:     Recall metric - fraction of true k-NNs in returned results
 *   - candidateRatioAtK: Efficiency metric - candidates_examined / K
 */
public final class QueryMetrics {

    private final double distanceRatioAtK;
    private final double precisionAtK;
    private final double recallAtK;              // NEW
    private final double candidateRatioAtK;

    public QueryMetrics(
            double distanceRatioAtK,
            double precisionAtK,
            double recallAtK,
            double candidateRatioAtK
    ) {
        this.distanceRatioAtK = distanceRatioAtK;
        this.precisionAtK = precisionAtK;
        this.recallAtK = recallAtK;
        this.candidateRatioAtK = candidateRatioAtK;
    }

    // Backward compatibility
    public QueryMetrics(double distanceRatioAtK, double precisionAtK, double candidateRatioAtK) {
        this(distanceRatioAtK, precisionAtK, precisionAtK, candidateRatioAtK);
    }

    public double ratioAtK() { return distanceRatioAtK; }
    public double precisionAtK() { return precisionAtK; }
    public double recallAtK() { return recallAtK; }          // NEW
    public double candidateRatioAtK() { return candidateRatioAtK; }

    @Override
    public String toString() {
        return String.format(
                "QueryMetrics{ratio=%.4f, precision=%.4f, recall=%.4f, candRatio=%.4f}",
                distanceRatioAtK, precisionAtK, recallAtK, candidateRatioAtK
        );
    }
}
