package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;

import java.util.List;

public interface QueryService {
    List<QueryResult> search(QueryToken token);

    /**
     * Runs a fixed sweep (1,20,40,60,80,100) and returns per-topK evaluation metrics.
     * @param baseToken     token used as the starting point (dim/context taken from this)
     * @param queryIndex    index in the groundtruth
     * @param gt            groundtruth provider (can be null; recall will be 0 if unavailable)
     */
    List<QueryEvaluationResult> searchWithTopKVariants(QueryToken baseToken, int queryIndex, GroundtruthManager gt);
}
