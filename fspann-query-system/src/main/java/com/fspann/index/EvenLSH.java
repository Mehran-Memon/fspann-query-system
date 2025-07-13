package com.fspann.index;

public class EvenLSH {

    private double[] a;               // Unit vector for projection
    private double[] criticalValues;  // Critical values computed for even bucket division

    public EvenLSH(double[] a, double[] criticalValues) {
        this.a = a;
        this.criticalValues = criticalValues;
    }

    /**
     * Map a high-dimensional point to a bucket ID.
     * @param point The data point (array of doubles).
     * @return Bucket ID as an integer.
     */
    public int getBucketId(double[] point) {
        double projection = 0.0;
        for (int i = 0; i < point.length; i++) {
            projection += point[i] * a[i];
        }
        // Assume the buckets are defined by the criticalValues (sorted in ascending order)
        for (int i = 0; i < criticalValues.length; i++) {
            if (projection <= criticalValues[i]) {
                return i + 1;  // using 1-indexing for bucket IDs
            }
        }
        return criticalValues.length + 1;
    }
}
