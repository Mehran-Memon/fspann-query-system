package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import java.util.*;
import java.util.function.Function;

/**
 * Provides static methods for greedy bucket merging and fake-add.
 */
public final class BucketConstructor {
    private BucketConstructor() {}

    /**
     * Greedily merges encrypted points into buckets based on vector similarity.
     * @param points list of encrypted points
     * @param maxSize maximum bucket size
     * @param unpackFunc function to extract plaintext vector from an EncryptedPoint
     * @return list of buckets
     */
    public static List<List<EncryptedPoint>> greedyMerge(
            List<EncryptedPoint> points,
            int maxSize,
            Function<EncryptedPoint, double[]> unpackFunc) {
        // Sort points by descending similarity to the first in each bucket
        points.sort(Comparator.comparingDouble(
                pt -> -dot(unpackFunc.apply(pt), unpackFunc.apply(pt))
        ));

        List<List<EncryptedPoint>> buckets = new ArrayList<>();
        List<EncryptedPoint> current = new ArrayList<>();
        for (EncryptedPoint pt : points) {
            if (current.isEmpty() || current.size() < maxSize) {
                current.add(pt);
            } else {
                buckets.add(current);
                current = new ArrayList<>();
                current.add(pt);
            }
        }
        if (!current.isEmpty()) {
            buckets.add(current);
        }
        return buckets;
    }

    /**
     * Pads each bucket with fake points up to the specified capacity.
     * @param buckets list of existing buckets
     * @param cap target bucket size
     * @param fakeTemplate encrypted point to duplicate as fake entries
     * @return list of buckets with fake points applied
     */
    public static List<List<EncryptedPoint>> applyFake(
            List<List<EncryptedPoint>> buckets,
            int cap,
            EncryptedPoint fakeTemplate) {
        for (List<EncryptedPoint> bucket : buckets) {
            while (bucket.size() < cap) {
                bucket.add(fakeTemplate);
            }
        }
        return buckets;
    }

    // Compute dot product of two plaintext vectors
    private static double dot(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }
}
