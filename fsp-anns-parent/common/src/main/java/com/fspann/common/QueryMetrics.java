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
    private final double candidateRatioAtK;

    /**
     * Full constructor with all three metrics.
     *
     * @param distanceRatioAtK  Quality: avg(dist_ANN_j / dist_GT_j) for positions j=1..K
     * @param precisionAtK      Recall: |true_KNN ∩ returned| / K
     * @param candidateRatioAtK Efficiency: candidates_examined / K
     */
    public QueryMetrics(double distanceRatioAtK, double precisionAtK, double candidateRatioAtK) {
        this.distanceRatioAtK = distanceRatioAtK;
        this.precisionAtK = precisionAtK;
        this.candidateRatioAtK = candidateRatioAtK;
    }

    /**
     * Backward-compatible constructor (candidateRatio defaults to NaN).
     */
    public QueryMetrics(double distanceRatioAtK, double precisionAtK) {
        this(distanceRatioAtK, precisionAtK, Double.NaN);
    }

    /**
     * Distance ratio: measures result quality.
     * For each position j, computes dist(returned_j, query) / dist(trueNN_j, query).
     * Perfect score = 1.0 (returned exactly the true k-NNs).
     */
    public double ratioAtK() {
        return distanceRatioAtK;
    }

    /**
     * Precision@K: measures recall.
     * Fraction of true k-nearest neighbors found in the returned results.
     * Perfect score = 1.0.
     */
    public double precisionAtK() {
        return precisionAtK;
    }

    /**
     * Candidate ratio: measures search efficiency.
     * Number of candidates examined divided by K.
     * Lower is more efficient; minimum possible = 1.0.
     */
    public double candidateRatioAtK() {
        return candidateRatioAtK;
    }

    @Override
    public String toString() {
        return String.format(
                "QueryMetrics{distRatio=%.4f, precision=%.4f, candRatio=%.4f}",
                distanceRatioAtK, precisionAtK, candidateRatioAtK
        );
    }
}