package com.fspann.common;

import java.util.List;
import java.util.Set;

public interface IndexService {
    void insert(EncryptedPoint pt);                  // Insert an encrypted point into the index
    void insert(String id, double[] vector);         // Insert with plaintext (will be encrypted)
    List<EncryptedPoint> lookup(QueryToken token);   // Core lookup (subset candidates)
    void delete(String id);                          // Delete by ID

    // Shard / indexing metadata hooks
    void markDirty(int shardId);                     // For legacy shard-based index; no-op in paper mode
    int getIndexedVectorCount();                     // Total indexed vectors (approx is OK)
    Set<Integer> getRegisteredDimensions();          // All dimensions that have vectors
    int getVectorCountForDimension(int dimension);   // Count per dimension

    // Point access & cache management (used by re-encryption, tests, forward security)
    EncryptedPoint getEncryptedPoint(String id);
    void updateCachedPoint(EncryptedPoint pt);
    EncryptedPointBuffer getPointBuffer();

    /**
     * For diagnostics. In pure partitioned/paper mode this may return -1
     * or some sentinel, since shard != paper partitions.
     */
    int getShardIdForVector(double[] vector);

    // ----------------------------
    // Default diagnostics helpers
    // ----------------------------

    /**
     * Default wrapper that calls lookup() and wraps it in a basic SearchDiagnostics
     * with:
     *  - uniqueCandidates = size of returned list
     *  - probedBuckets = 0 (no bucket-level semantics in paper mode)
     *  - fanoutPerTable = empty map
     */
    default LookupWithDiagnostics lookupWithDiagnostics(QueryToken token) {
        List<EncryptedPoint> raw = lookup(token);
        SearchDiagnostics diag = new SearchDiagnostics(
                (raw != null) ? raw.size() : 0,   // uniqueCandidates
                0,                                // probedBuckets (0 in paper mode)
                java.util.Map.of()                // fanoutPerTable (unused in paper mode)
        );
        return new LookupWithDiagnostics(
                (raw != null) ? raw : java.util.List.of(),
                diag
        );
    }

    /**
     * Returns the number of candidate points considered for this token
     * BEFORE distance computations.
     */
    default int candidateCount(QueryToken token) {
        return lookupWithDiagnostics(token).diagnostics().uniqueCandidates();
    }
}
