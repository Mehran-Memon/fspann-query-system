package com.fspann.common;

/**
 * Immutable metrics container for a single (query, K) evaluation.
 * Single source of truth: ratio and precision.
 */
public record QueryMetrics(double ratioAtK, double precisionAtK) {

    public static QueryMetrics of(double ratioAtK, double precisionAtK) {
        return new QueryMetrics(ratioAtK, precisionAtK);
    }

    // Backward-friendly accessors (if you prefer method-call style)
    public double ratioAtK() { return ratioAtK; }
    public double precisionAtK() { return precisionAtK; }
}
