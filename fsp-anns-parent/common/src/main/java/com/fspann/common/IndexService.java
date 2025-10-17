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

    default LookupWithDiagnostics lookupWithDiagnostics(QueryToken token) {
        List<EncryptedPoint> raw = lookup(token);
        SearchDiagnostics diag = new SearchDiagnostics(
                (raw != null) ? raw.size() : 0,  // uniqueCandidates
                0,                                // probedBuckets (0 for paper mode)
                java.util.Map.of()                // fanoutPerTable
        );
        return new LookupWithDiagnostics((raw != null) ? raw : java.util.List.of(), diag);
    }

    public static final class LookupWithDiagnostics {
        private final java.util.List<EncryptedPoint> candidates;
        private final SearchDiagnostics diagnostics;

        public LookupWithDiagnostics(java.util.List<EncryptedPoint> candidates,
                                     SearchDiagnostics diagnostics) {
            this.candidates  = (candidates != null) ? candidates : java.util.List.of();
            this.diagnostics = (diagnostics != null) ? diagnostics : SearchDiagnostics.EMPTY;
        }
        public java.util.List<EncryptedPoint> candidates() { return candidates; }
        public SearchDiagnostics diagnostics()             { return diagnostics; }
    }

    public static final class SearchDiagnostics {
        public static final SearchDiagnostics EMPTY =
                new SearchDiagnostics(0, 0, java.util.Map.of());

        private final int uniqueCandidates; // size of union BEFORE distance re-rank
        private final int probedBuckets;    // optional (0 in paper mode)
        private final java.util.Map<Integer,Integer> fanoutPerTable;

        public SearchDiagnostics(int uniqueCandidates,
                                 int probedBuckets,
                                 java.util.Map<Integer,Integer> fanoutPerTable) {
            this.uniqueCandidates = Math.max(0, uniqueCandidates);
            this.probedBuckets    = Math.max(0, probedBuckets);
            this.fanoutPerTable   = (fanoutPerTable != null) ? java.util.Map.copyOf(fanoutPerTable) : java.util.Map.of();
        }

        public int uniqueCandidates() { return uniqueCandidates; }
        public int probedBuckets()    { return probedBuckets; }
        public java.util.Map<Integer,Integer> fanoutPerTable() { return fanoutPerTable; }
    }



    /**
     * Returns the number of candidate points considered for this token BEFORE distance computations.
     * Default now uses lookupWithDiagnostics().diagnostics().uniqueCandidates.
     */
    default int candidateCount(QueryToken token) {
        return lookupWithDiagnostics(token).diagnostics().uniqueCandidates();
    }
}
