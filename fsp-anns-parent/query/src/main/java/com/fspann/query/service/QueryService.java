package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;

import java.util.List;

/**
 * High-level service interface for performing secure Approximate Nearest Neighbor (ANN) search over encrypted vectors.
 */
public interface QueryService {

    /**
     * Executes an encrypted ANN query using the provided query token.
     *
     * @param token A valid {@link QueryToken} containing:
     *              <ul>
     *                  <li>Encrypted query vector</li>
     *                  <li>Target bucket IDs from LSH</li>
     *                  <li>Number of results to return (topK)</li>
     *              </ul>
     * @return A sorted list of {@link QueryResult} based on distance, never null (may be empty).
     *
     * @throws IllegalArgumentException if the token or its fields are invalid (null, empty, or inconsistent).
     * @throws RuntimeException if the query fails due to cryptographic or lookup issues.
     */
    List<QueryResult> search(QueryToken token);
}
