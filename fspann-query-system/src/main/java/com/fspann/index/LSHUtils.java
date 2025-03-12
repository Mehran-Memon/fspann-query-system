package com.fspann.index;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LSHUtils {
	/**
     * Compute critical values (quantiles) for even division.
     * @param data List of high-dimensional data points.
     * @param a Unit vector for projection.
     * @param numIntervals Number of intervals (buckets) desired.
     * @return Array of critical values.
     */
    public static double[] computeCriticalValues(List<double[]> data, double[] a, int numIntervals) {
        List<Double> projections = new ArrayList<>();
        for (double[] point : data) {
            double proj = 0.0;
            for (int i = 0; i < point.length; i++) {
                proj += point[i] * a[i];
            }
            projections.add(proj);
        }
        Collections.sort(projections);
        int n = projections.size();
        double[] criticalValues = new double[numIntervals];
        for (int i = 1; i <= numIntervals; i++) {
            int index = (int) Math.floor(i * n / (double) numIntervals);
            criticalValues[i - 1] = projections.get(Math.min(index, n - 1));
        }
        return criticalValues;
    }
}
