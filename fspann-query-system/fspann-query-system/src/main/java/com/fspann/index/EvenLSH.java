package java.com.fspann.index;

import java.com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;

public class EvenLSH {
    private double[] a;               // Unit vector for projection
    private double[] criticalValues;  // Critical values for even bucket division
    private final int dimensions;     // Number of dimensions
    private final int numIntervals;   // Number of intervals (buckets)

    public EvenLSH(int dimensions, int numIntervals, List<double[]> initialData) {
        this.dimensions = dimensions;
        this.numIntervals = numIntervals;
        this.a = generateUnitVector(dimensions); // Generate unit vector using a method
        this.criticalValues = new double[numIntervals];
        if (initialData != null && !initialData.isEmpty()) {
            updateCriticalValues(initialData, numIntervals);
        } else {
            setDefaultCriticalValues();
        }
    }

    private void setDefaultCriticalValues() {
        double step = 20.0 / numIntervals;
        for (int i = 0; i < numIntervals; i++) {
            criticalValues[i] = -10.0 + (i + 1) * step;
        }
    }

    private double[] generateUnitVector(int dimensions) {
        SecureRandom random = new SecureRandom();
        double[] vector = new double[dimensions];
        double norm = 0.0;
        for (int i = 0; i < dimensions; i++) {
            vector[i] = random.nextGaussian();
            norm += vector[i] * vector[i];
        }
        norm = Math.sqrt(norm);
        for (int i = 0; i < dimensions; i++) {
            vector[i] /= norm; // Normalize to unit vector
        }
        return vector;
    }

    private double project(double[] point) {
        double projection = 0.0;
        for (int i = 0; i < point.length; i++) {
            projection += point[i] * a[i];
        }
        return projection;
    }

    public int getBucketId(double[] point) {
        if (point.length != dimensions) {
            throw new IllegalArgumentException("Point dimension mismatch: expected " + dimensions + ", got " + point.length);
        }
        double projection = project(point);
        for (int i = 0; i < criticalValues.length; i++) {
            if (projection <= criticalValues[i]) {
                return i + 1; // 1-indexed bucket IDs
            }
        }
        return criticalValues.length + 1;
    }

    public int getBucketId(byte[] encryptedPoint, SecretKey key) throws Exception {
        double[] point = EncryptionUtils.decryptVector(encryptedPoint, key);
        return getBucketId(point);
    }

    public void updateCriticalValues(List<double[]> vectors, int numIntervals) {
        if (vectors == null || vectors.isEmpty()) return;
        double[] projections = new double[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            projections[i] = project(vectors.get(i));
        }
        Arrays.sort(projections);
        this.criticalValues = new double[numIntervals];
        for (int i = 0; i < numIntervals; i++) {
            int index = (i + 1) * projections.length / (numIntervals + 1);
            criticalValues[i] = projections[Math.min(index, projections.length - 1)];
        }
    }

    public void rehash() {
        // Recompute the random projection vector
        a = generateUnitVector(dimensions);
    }

    public List<Integer> expandBuckets(int mainBucket, int expansionRange) {
        List<Integer> candidateBuckets = new ArrayList<>();
        for (int i = -expansionRange; i <= expansionRange; i++) {
            int bucket = mainBucket + i;
            if (bucket > 0 && bucket <= numIntervals) {
                candidateBuckets.add(bucket);  // Add valid bucket IDs within range
            }
        }
        return candidateBuckets;
    }

    public double[] getCriticalValues() {
        return criticalValues.clone();
    }
}
