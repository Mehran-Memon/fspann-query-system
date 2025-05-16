package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import java.util.List;

/**
 * High-level search interface for encrypted ANN queries.
 */
public interface QueryService {
    List<QueryResult> search(QueryToken token);
}