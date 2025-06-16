package com.fspann.index.service;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import java.util.List;

/**
 * Abstraction for index operations: insert, delete, lookup, and dirty marking.
 */
public interface IndexService {
    void insert(EncryptedPoint point);
    void delete(String id);
    List<EncryptedPoint> lookup(QueryToken token);
    void markDirty(int shardId);
    int getIndexedVectorCount();

}