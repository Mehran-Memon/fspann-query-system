package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class EvenLSH {
    private double[] projectionVector;  // Unit vector for projection
    private double[] criticalValues;    // Critical values for even bucket division
    private final int dimensions;       // Number of dimensions in the data
    private final int numBuckets;       // Number of buckets (intervals)

    public EvenLSH(int dimensions, int numBuckets) {
        this.dimensions = dimensions;
        this.numBuckets = numBuckets;
        this.projectionVector = generateUnitVector(dimensions);  // Generate a random unit vector for projection
        this.criticalValues = new double[numBuckets];
    }

    /**
     * Generates a random unit vector for projection.
     * @param dimensions The number of dimensions for the vector.
     * @return A normalized unit vector.
     */
    private double[] generateUnitVector(int dimensions) {
        SecureRandom random = new SecureRandom();
        double[] vector = new double[dimensions];
        double norm = 0.0;

        // Generate random Gaussian values and normalize
        for (int i = 0; i < dimensions; i++) {
            vector[i] = random.nextGaussian();
            norm += vector[i] * vector[i];
        }

        norm = Math.sqrt(norm);
        // Normalize the vector to ensure it's a unit vector
        for (int i = 0; i < dimensions; i++) {
            vector[i] /= norm;
        }
        return vector;
    }

    /**
     * Projects a point onto the random hyperplane defined by the projection vector.
     * @param point The data point to be projected.
     * @return The projection value.
     */
    protected double project(double[] point) {
        double projection = 0.0;
        for (int i = 0; i < point.length; i++) {
            projection += point[i] * projectionVector[i];  // Dot product with the projection vector
        }
        return projection;
    }

    /**
     * Computes the bucket ID based on the projected value and critical values (quantiles).
     * @param point The data point.
     * @return The bucket ID.
     */
    public int getBucketId(double[] point) {
        double projection = project(point);
        for (int i = 0; i < criticalValues.length; i++) {
            if (projection <= criticalValues[i]) {
                return i + 1; // 1-indexed bucket IDs
            }
        }
        // Ensure the bucket ID does not exceed the number of buckets
        return Math.min(criticalValues.length + 1, numBuckets);  // Default to the last bucket if out of range
    }

    /**
     * Updates the critical values (bucket boundaries) based on the dataset.
     * @param vectors The dataset to update the critical values.
     */
    public void updateCriticalValues(List<double[]> vectors) {
        // Compute projections for each data point
        List<Double> projections = new ArrayList<>();
        for (double[] vector : vectors) {
            projections.add(project(vector));
        }

        // If projections list is empty, log the issue and return
        if (projections.isEmpty()) {
            throw new IllegalArgumentException("Projections cannot be empty.");
        }

        // Sort the projections and divide them into intervals (buckets)
        projections.sort(Double::compare);
        int numIntervals = numBuckets;

        // Assign critical values as quantiles
        for (int i = 1; i <= numIntervals; i++) {
            int index = (int) Math.floor(i * projections.size() / (double) (numIntervals + 1));
            criticalValues[i - 1] = projections.get(Math.min(index, projections.size() - 1));
        }

        // Log updated critical values for tracking
        System.out.println("Updated critical values: ");
        for (double value : criticalValues) {
            System.out.print(value + " ");
        }
        System.out.println();
    }

    /**
     * Rehashes the projection vector by generating a new random unit vector.
     */
    public void rehash() {
        this.projectionVector = generateUnitVector(dimensions);
    }

    /**
     * Expands the search range by selecting neighboring buckets around a given bucket.
     * @param mainBucket The main bucket ID.
     * @param expansionRange The range of bucket expansion.
     * @return A list of candidate buckets.
     */
    public List<Integer> expandBuckets(int mainBucket, int expansionRange) {
        List<Integer> candidateBuckets = new ArrayList<>();
        for (int i = -expansionRange; i <= expansionRange; i++) {
            int bucket = mainBucket + i;
            if (bucket > 0 && bucket <= numBuckets) {
                candidateBuckets.add(bucket);
            }
        }
        return candidateBuckets;
    }

    /**
     * Returns the critical values (bucket boundaries).
     * @return An array of critical values.
     */
    public double[] getCriticalValues() {
        return criticalValues.clone();
    }
}
