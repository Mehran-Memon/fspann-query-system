package com.fspann.common;

import java.util.List;
import java.util.Set;

public interface IndexService {
    void insert(EncryptedPoint pt);
    void insert(String id, double[] vector);
    List<EncryptedPoint> lookup(QueryToken token);
    void delete(String id);

    void markDirty(int shardId);
    int getIndexedVectorCount();
    Set<Integer> getRegisteredDimensions();
    int getVectorCountForDimension(int dimension);
    EncryptedPoint getEncryptedPoint(String id);
    void updateCachedPoint(EncryptedPoint pt);
    EncryptedPointBuffer getPointBuffer();
    int getShardIdForVector(double[] vector);

    /** New: richer lookup that can return union size / fanout (fallback wraps lookup()). */
    default LookupWithDiagnostics lookupWithDiagnostics(QueryToken token) {
        List<EncryptedPoint> raw = lookup(token);
        // Fallback: until your index service fills real diagnostics,
        // expose "uniqueCandidates = raw.size()" so CSV stops logging K.
        SearchDiagnostics diag = new SearchDiagnostics(
                (raw != null) ? raw.size() : 0,
                0,
                java.util.Map.of()
        );
        return new LookupWithDiagnostics(raw, diag);
    }

    /**
     * Returns the number of candidate points considered for this token BEFORE distance computations.
     * Default now uses lookupWithDiagnostics().diagnostics().uniqueCandidates.
     */
    default int candidateCount(QueryToken token) {
        return lookupWithDiagnostics(token).diagnostics().uniqueCandidates();
    }
}
