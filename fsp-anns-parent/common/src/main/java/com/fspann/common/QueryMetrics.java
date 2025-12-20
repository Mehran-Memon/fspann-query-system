package com.fspann.common;

/**
 * Immutable per-(query, K) evaluation metrics.
 * SINGLE SOURCE OF TRUTH.
 */
public final class QueryMetrics {

    private final double ratioAtK;
    private final double precisionAtK;

    public QueryMetrics(double ratioAtK, double precisionAtK) {
        this.ratioAtK = ratioAtK;
        this.precisionAtK = precisionAtK;
    }

    /** Peng-style distance ratio (ANN / GT or BASE) */
    public double ratioAtK() {
        return ratioAtK;
    }

    /** Precision@K */
    public double precisionAtK() {
        return precisionAtK;
    }

    @Override
    public String toString() {
        return "QueryMetrics{ratio=" + ratioAtK + ", precision=" + precisionAtK + '}';
    }
}
