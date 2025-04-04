package com.fspann.evaluation;

import com.fspann.ForwardSecureANNSystem;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class EvaluationEngine {

    public static double computeRecallAtK(List<int[]> groundTruth, List<List<Integer>> predictions, int k) {
        int totalCorrect = 0;
        int totalPossible = groundTruth.size() * k;

        for (int i = 0; i < groundTruth.size(); i++) {
            Set<Integer> actualSet = new HashSet<>();
            for (int j = 0; j < Math.min(k, groundTruth.get(i).length); j++) {
                actualSet.add(groundTruth.get(i)[j]);
            }

            List<Integer> predicted = predictions.get(i);
            for (int id : predicted) {
                if (actualSet.contains(id)) {
                    totalCorrect++;
                }
            }
        }

        return (double) totalCorrect / totalPossible;
    }

    public static void evaluate(ForwardSecureANNSystem system, int topK, int numQueries) throws Exception {
        List<double[]> queryVectors = system.getQueryVectors();
        List<int[]> groundTruth = system.getGroundTruth();

        List<List<Integer>> predictions = new java.util.ArrayList<>();
        long totalTime = 0;

        for (int i = 0; i < Math.min(numQueries, queryVectors.size()); i++) {
            double[] q = queryVectors.get(i);
            long start = System.nanoTime();
            List<double[]> result = system.query(q, topK);
            long end = System.nanoTime();
            totalTime += (end - start);

            List<Integer> predictedIndices = new java.util.ArrayList<>();
            for (double[] neighbor : result) {
                int bestIndex = findClosestIndex(neighbor, system.getBaseVectors());
                predictedIndices.add(bestIndex);
            }
            predictions.add(predictedIndices);
        }

        double recall = computeRecallAtK(groundTruth, predictions, topK);
        double avgTimeMs = totalTime / 1_000_000.0 / numQueries;

        System.out.printf("✅ Recall@%d: %.4f%n", topK, recall);
        System.out.printf("⚡ Avg Query Time: %.2f ms%n", avgTimeMs);
    }

    private static int findClosestIndex(double[] vec, List<double[]> allVectors) {
        double bestDist = Double.MAX_VALUE;
        int bestIdx = -1;
        for (int i = 0; i < allVectors.size(); i++) {
            double dist = euclidean(vec, allVectors.get(i));
            if (dist < bestDist) {
                bestDist = dist;
                bestIdx = i;
            }
        }
        return bestIdx;
    }

    private static double euclidean(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }
}
