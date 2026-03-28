package com.fspann.common;

/**
 * Immutable per-(query, K) evaluation metrics.
 * SINGLE SOURCE OF TRUTH.
 *
 * DEFINITIONS (PAPER-ALIGNED):
 *
 * Primary (used in paper, plots, comparisons):
 *   - distanceRatioAtK : avg(||returned_i - query|| / ||groundtruth_i - query||)
 *     → This is Peng et al.'s RATIO (result quality)
 *     → Perfect = 1.0, higher = farther from true neighbors
 *
 * Secondary (diagnostic, measures search efficiency):
 *   - candidateRatioAtK : (# refined / distance-evaluated candidates) / K
 *     → Measures search cost (how many vectors decrypted/scored)
 *     → Lower is more efficient
 *
 * Quality:
 *   - recallAtK : |GT ∩ ANN| / K
 */
public final class QueryMetrics {

    // --- SECONDARY (search efficiency) ---
    private final double candidateRatioAtK;

    // --- PRIMARY (paper metric - Peng et al.) ---
    private final double distanceRatioAtK;

    // --- QUALITY ---
    private final double recallAtK;

    public QueryMetrics(
            double candidateRatioAtK,
            double distanceRatioAtK,
            double recallAtK
    ) {
        this.candidateRatioAtK = candidateRatioAtK;
        this.distanceRatioAtK  = distanceRatioAtK;
        this.recallAtK         = recallAtK;
    }

    // ------------------------------------------------------------------
    // ACCESSORS (EXPLICIT, NO AMBIGUITY)
    // ------------------------------------------------------------------

    /**
     * PRIMARY ratio used in the paper (Peng et al.)
     * This measures RESULT QUALITY.
     */
    public double distanceRatioAtK() {
        return distanceRatioAtK;
    }

    /**
     * SECONDARY: Search efficiency metric.
     * This measures SEARCH COST (not result quality).
     */
    public double candidateRatioAtK() {
        return candidateRatioAtK;
    }

    /**
     * Recall@K
     */
    public double recallAtK() {
        return recallAtK;
    }

    // ------------------------------------------------------------------
    // BACKWARD COMPATIBILITY (DO NOT USE IN NEW CODE)
    // ------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "QueryMetrics{distanceRatio=%.4f, candidateRatio=%.4f, recall=%.4f}",
                distanceRatioAtK, candidateRatioAtK, recallAtK
        );
    }
}