package com.fspann.index;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import com.fspann.encryption.EncryptionUtils;

public class ANN {

    private EvenLSH lsh;  // The Even LSH instance used for hashing and bucket assignment
    private List<List<byte[]>> buckets;  // The list of LSH buckets (index)

    public ANN(int dimensions, int numBuckets) {
        // Initialize Even LSH with specified dimensions and number of buckets
        this.lsh = new EvenLSH(dimensions, numBuckets);
        this.buckets = new ArrayList<>();
    }

    /**
     * Builds the ANN index from the provided dataset.
     * @param data The dataset to build the ANN index.
     */
    public void buildIndex(List<double[]> data) {
        // Update critical values (bucket boundaries) for Even LSH
        lsh.updateCriticalValues(data);

        // Initialize the buckets (using Even LSH)
        this.buckets.clear();
        for (double[] point : data) {
            int bucketId = lsh.getBucketId(point);  // Get the bucket ID for the point
            while (buckets.size() <= bucketId) {
                buckets.add(new ArrayList<>());  // Ensure the bucket list is large enough
            }
            byte[] encryptedPoint = EncryptionUtils.encryptVector(point, null);  // Encrypt the point
            buckets.get(bucketId - 1).add(encryptedPoint);  // Add the point to the corresponding bucket
        }
    }

    /**
     * Finds the k-nearest neighbors (ANN) for the given query point.
     * @param queryPoint The query point to find nearest neighbors for.
     * @param k The number of nearest neighbors to retrieve.
     * @return A list of k-nearest neighbors.
     */
    public List<byte[]> getApproximateNearestNeighbors(double[] queryPoint, int k) {
        // Get the bucket ID for the query point
        int queryBucketId = lsh.getBucketId(queryPoint);

        // Create a priority queue to store the nearest neighbors (min-heap)
        PriorityQueue<byte[]> nearestNeighbors = new PriorityQueue<>(k, (a, b) -> {
            double[] pointA = EncryptionUtils.decryptVector(a, null);
            double[] pointB = EncryptionUtils.decryptVector(b, null);
            double distanceA = calculateDistance(queryPoint, pointA);  // Calculate distance between query and pointA
            double distanceB = calculateDistance(queryPoint, pointB);  // Calculate distance between query and pointB
            return Double.compare(distanceA, distanceB);  // Compare distances
        });

        // Search for neighbors in the same bucket and neighboring buckets
        for (int i = queryBucketId - 1; i <= queryBucketId + 1; i++) {
            if (i >= 0 && i < buckets.size()) {
                for (byte[] encryptedPoint : buckets.get(i)) {
                    double[] point = EncryptionUtils.decryptVector(encryptedPoint, null);
                    if (nearestNeighbors.size() < k) {
                        nearestNeighbors.add(encryptedPoint);
                    } else {
                        nearestNeighbors.poll();
                        nearestNeighbors.add(encryptedPoint);
                    }
                }
            }
        }

        // Return the k-nearest neighbors
        return new ArrayList<>(nearestNeighbors);
    }

    /**
     * Updates the ANN index by adding a new point.
     * @param newPoint The new point to add to the index.
     */
    public void updateIndex(double[] newPoint) {
        int bucketId = lsh.getBucketId(newPoint);  // Get the bucket ID for the new point
        while (buckets.size() <= bucketId) {
            buckets.add(new ArrayList<>());  // Ensure the bucket list is large enough
        }
        byte[] encryptedPoint = EncryptionUtils.encryptVector(newPoint, null);  // Encrypt the new point
        buckets.get(bucketId - 1).add(encryptedPoint);  // Add the new point to the corresponding bucket
    }

    /**
     * Removes a point from the ANN index.
     * @param point The point to remove.
     */
    public void removePoint(double[] point) {
        int bucketId = lsh.getBucketId(point);  // Get the bucket ID for the point
        List<byte[]> bucket = buckets.get(bucketId - 1);  // Get the bucket

        byte[] encryptedPoint = EncryptionUtils.encryptVector(point, null);  // Encrypt the point
        bucket.remove(encryptedPoint);  // Remove the point from the bucket
    }

    /**
     * Calculates the Euclidean distance between two vectors.
     * @param pointA The first point.
     * @param pointB The second point.
     * @return The Euclidean distance between the two points.
     */
    private double calculateDistance(double[] pointA, double[] pointB) {
        double sum = 0.0;
        for (int i = 0; i < pointA.length; i++) {
            sum += Math.pow(pointA[i] - pointB[i], 2);  // Sum of squared differences
        }
        return Math.sqrt(sum);  // Return the Euclidean distance
    }

    /**
     * Returns the current list of buckets in the ANN index.
     * @return A list of buckets.
     */
    public List<List<byte[]>> getBuckets() {
        return buckets;
    }
}
