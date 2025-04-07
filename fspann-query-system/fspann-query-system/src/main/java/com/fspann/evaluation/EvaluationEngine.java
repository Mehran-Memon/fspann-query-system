package com.fspann.evaluation;

import com.fspann.ForwardSecureANNSystem;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class EvaluationEngine {

    // Utility method to compute Recall@K
    public static double computeRecallAtK(List<int[]> groundTruth, List<List<Integer>> predictions, int topK) {
        int totalCorrect = 0;
        int totalQueries = predictions.size();

        for (int i = 0; i < totalQueries; i++) {
            List<Integer> predicted = predictions.get(i);
            int[] truth = groundTruth.get(i);

            // Count correct predictions within the top K
            Set<Integer> truthSet = new HashSet<>();
            for (int t : truth) {
                truthSet.add(t);
            }

            int correct = 0;
            for (int j = 0; j < Math.min(topK, predicted.size()); j++) {
                if (truthSet.contains(predicted.get(j))) {
                    correct++;
                }
            }
            totalCorrect += correct;
        }

        return totalCorrect / (double) (totalQueries * topK);
    }

    // Evaluate method with range or k-NN query
    public static void evaluate(ForwardSecureANNSystem system, int topK, int numQueries, double range) throws Exception {
        List<double[]> queryVectors = system.getQueryVectors();
        List<int[]> groundTruth = system.getGroundTruth();

        List<List<Integer>> predictions = new java.util.ArrayList<>();
        long totalTime = 0;

        for (int i = 0; i < Math.min(numQueries, queryVectors.size()); i++) {
            double[] q = queryVectors.get(i);
            long start = System.nanoTime();

            // Use range query if range > 0, otherwise use k-NN
            List<double[]> result;
            if (range > 0) {
                result = system.query(q, topK, range); // Range Query
            } else {
                result = system.query(q, topK, 0); // k-NN Query
            }

            long end = System.nanoTime();
            totalTime += (end - start);

            List<Integer> predictedIndices = new java.util.ArrayList<>();
            for (double[] neighbor : result) {
                int bestIndex = findClosestIndex(neighbor, system.getBaseVectors());
                predictedIndices.add(bestIndex);
            }
            predictions.add(predictedIndices);
        }

        // Compute Recall at K
        double recall = computeRecallAtK(groundTruth, predictions, topK);
        double avgTimeMs = totalTime / 1_000_000.0 / numQueries;

        System.out.printf("✅ Recall@%d: %.4f%n", topK, recall);
        System.out.printf("⚡ Avg Query Time: %.2f ms%n", avgTimeMs);
    }

    // Utility method to find the closest index to a given vector
    public static int findClosestIndex(double[] vector, List<double[]> baseVectors) {
        double minDistance = Double.MAX_VALUE;
        int bestIndex = -1;
        for (int i = 0; i < baseVectors.size(); i++) {
            double[] baseVector = baseVectors.get(i);
            double dist = distance(vector, baseVector);
            if (dist < minDistance) {
                minDistance = dist;
                bestIndex = i;
            }
        }
        return bestIndex;
    }

    // Utility method to compute Euclidean distance
    private static double distance(double[] v1, double[] v2) {
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
}
