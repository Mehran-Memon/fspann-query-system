package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;

import java.util.List;

public interface QueryService {
    List<QueryResult> search(QueryToken token);
    List<QueryEvaluationResult> searchWithTopKVariants(QueryToken baseToken, int queryIndex, GroundtruthManager gt);
}
