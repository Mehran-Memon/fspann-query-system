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

    /**
     * Returns the number of candidate points that would be considered for this query token
     * before distance computations. Default impl returns retrieved.size() to preserve compatibility.
     */
    default int candidateCount(QueryToken token) {
        List<EncryptedPoint> results = lookup(token);
        return (results != null) ? results.size() : 0;
    }
}
