package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import java.util.List;

/**
 * High-level search interface for encrypted ANN queries.
 */
public interface QueryService {
    /**
     * Performs an encrypted ANN search.
     * @param token The query token containing encrypted query and parameters.
     * @return List of QueryResult objects sorted by distance, limited to token.getTopK().
     * @throws RuntimeException if decryption or lookup fails.
     */
    List<QueryResult> search(QueryToken token);
}