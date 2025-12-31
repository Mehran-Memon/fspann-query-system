package com.fspann.common;

/**
 * Immutable per-(query, K) evaluation metrics.
 * SINGLE SOURCE OF TRUTH.
 *
 * DEFINITIONS (PAPER-ALIGNED):
 *
 * Primary (used in paper, plots, comparisons):
 *   - candidateRatioAtK : (# refined / distance-evaluated candidates) / K
 *     → This is Peng et al.'s RATIO (search efficiency)
 *
 * Secondary (diagnostic only, NOT used for comparison):
 *   - distanceRatioAtK  : avg(dist_ann_i / dist_gt_i), i = 1..K
 *
 * Quality:
 *   - recallAtK         : |GT ∩ ANN| / K
 *
 * NOTE:
 *   precisionAtK is intentionally REMOVED (not used, misleading).
 */
public final class QueryMetrics {

    // --- PRIMARY (Peng) ---
    private final double candidateRatioAtK;

    // --- SECONDARY (diagnostic) ---
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
     */
    public double candidateRatioAtK() {
        return candidateRatioAtK;
    }

    /**
     * Distance quality diagnostic (NOT the paper ratio)
     */
    public double distanceRatioAtK() {
        return distanceRatioAtK;
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

    /**
     * @deprecated DO NOT USE.
     * Kept only so legacy code does not break.
     * Historically misused to mean "ratio".
     */
    @Deprecated
    public double ratioAtK() {
        return candidateRatioAtK;
    }

    @Override
    public String toString() {
        return String.format(
                "QueryMetrics{candidateRatio=%.4f, distanceRatio=%.4f, recall=%.4f}",
                candidateRatioAtK, distanceRatioAtK, recallAtK
        );
    }
}
