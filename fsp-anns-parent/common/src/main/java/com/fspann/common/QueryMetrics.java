package com.fspann.common;

/**
 * Immutable metrics container for a single (query, K) evaluation.
 * Single source of truth for ratio@K and precision@K.
 */
public record QueryMetrics(
        double ratioAtK,
        double precisionAtK
) {}
