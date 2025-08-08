package com.fspann.common;

import java.util.List;
import java.util.Set;

public interface IndexService {

    void insert(EncryptedPoint point);

    void insert(String id, double[] vector);

    void delete(String id);

    /** REQUIRED: per-table bucket lookup */
    List<EncryptedPoint> lookup(QueryToken token);

    /** Legacy no-op in new design; left for API compatibility */
    void markDirty(int shardId);

    int getIndexedVectorCount();

    Set<Integer> getRegisteredDimensions();

    int getVectorCountForDimension(int dimension);

    EncryptedPoint getEncryptedPoint(String id);

    EncryptedPointBuffer getPointBuffer();

    int getShardIdForVector(double[] vector);

    void clearCache();
}
