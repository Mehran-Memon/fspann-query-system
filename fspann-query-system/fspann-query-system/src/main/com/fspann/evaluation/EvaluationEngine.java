package com.fspann.evaluation;

import com.fspann.index.ANN;
import com.fspann.index.DimensionContext;
import com.fspann.query.EncryptedPoint;
import javax.crypto.SecretKey;
import java.util.*;

public class EvaluationEngine {

    /**
     * Evaluate using an external supply of query vectors, ground truth, and contexts.
     *
     * @param queryVectors   the list of query vectors to test
     * @param groundTruth    the corresponding ground truth nearest‐neighbor indices
     * @param contexts       a map from dimensionality to its DimensionContext
     * @param topK           the K in “Recall@K”
     * @param numQueries     how many of the vectors to run (e.g. Math.min(numQueries, queryVectors.size()))
     */
    public static void evaluate(
            List<double[]> queryVectors,
            List<int[]> groundTruth,
            Map<Integer, DimensionContext> contexts,
            int topK,
            int numQueries
    ) throws Exception {
        if (queryVectors.size() != groundTruth.size()) {
            throw new IllegalArgumentException(
                    "Number of queries and groundTruth entries must match"
            );
        }

        List<List<Integer>> predictions = new ArrayList<>();
        long totalTimeNanos = 0;

        int limit = Math.min(numQueries, queryVectors.size());
        for (int i = 0; i < limit; i++) {
            double[] q      = queryVectors.get(i);
            int dims        = q.length;
            DimensionContext ctx = contexts.get(dims);
            if (ctx == null) {
                throw new IllegalStateException("No context for dimension: " + dims);
            }

            // time the query()
            long start = System.nanoTime();
            List<EncryptedPoint> encResults =
                    ctx.getIndex().findNearestNeighborsEncrypted(
                            com.fspann.query.QueryGenerator.generateQueryToken(
                                    q, topK, ctx.getIndex().getNumHashTables(), ctx.getLsh(), ctx.getAnn().getKeyManager()
                            )
                    );
            long end = System.nanoTime();
            totalTimeNanos += (end - start);

            // map encrypted results back to indices
            List<Integer> preds = new ArrayList<>();
            for (EncryptedPoint ep : encResults) {
                preds.add(ep.getIndex());
            }
            predictions.add(preds);
        }

        // compute recall
        int totalCorrect = 0;
        for (int i = 0; i < predictions.size(); i++) {
            Set<Integer> truthSet = new HashSet<>();
            for (int t : groundTruth.get(i)) truthSet.add(t);
            for (int j = 0; j < Math.min(topK, predictions.get(i).size()); j++) {
                if (truthSet.contains(predictions.get(i).get(j))) {
                    totalCorrect++;
                }
            }
        }
        double recall = (double) totalCorrect / predictions.size();
        double avgMs  = totalTimeNanos / 1_000_000.0 / predictions.size();

        System.out.printf("✅ Recall@%d = %.4f%n", topK, recall);
        System.out.printf("⚡ Avg Query Time = %.2f ms%n", avgMs);
    }
}
