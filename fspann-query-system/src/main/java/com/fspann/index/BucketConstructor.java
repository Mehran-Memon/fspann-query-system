package com.fspann.index;
import java.util.ArrayList;
import java.util.List;

public class BucketConstructor {
    /**
     * Merge sorted data points into buckets of at most maxBucketSize.
     * @param sortedPoints List of data points already sorted by their LSH bucket ID.
     * @param maxBucketSize Maximum number of points per bucket.
     * @return List of buckets, where each bucket is a List of points.
     */
    public static List<List<double[]>> greedyMerge(List<double[]> sortedPoints, int maxBucketSize) {
        List<List<double[]>> buckets = new ArrayList<>();
        List<double[]> currentBucket = new ArrayList<>();

        for (double[] point : sortedPoints) {
            currentBucket.add(point);
            if (currentBucket.size() >= maxBucketSize) {
                buckets.add(new ArrayList<>(currentBucket));
                currentBucket.clear();
            }
        }
        if (!currentBucket.isEmpty()) {
            buckets.add(currentBucket);
        }
        return buckets;
    }
    
    /**
     * Pad each bucket with fake points so that all buckets have the same size.
     * @param buckets List of buckets.
     * @param targetSize Target uniform size.
     * @return New list of buckets with uniform size.
     */
    public static List<List<double[]>> applyFakeAddition(List<List<double[]>> buckets, int targetSize) {
        List<List<double[]>> uniformBuckets = new ArrayList<>();
        for (List<double[]> bucket : buckets) {
            List<double[]> newBucket = new ArrayList<>(bucket);
            int numFake = targetSize - bucket.size();
            for (int i = 0; i < numFake; i++) {
                newBucket.add(generateFakePoint(bucket.get(0).length));  // assume all points have same dimension
            }
            uniformBuckets.add(newBucket);
        }
        return uniformBuckets;
    }
    
    private static double[] generateFakePoint(int dimension) {
        double[] fake = new double[dimension];
        // Populate with dummy values; mark these points as fake (e.g., using a flag in a wrapper class)
        for (int i = 0; i < dimension; i++) {
            fake[i] = 0.0;  // or some predefined dummy value
        }
        return fake;
    }
}
