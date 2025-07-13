package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public class BucketConstructor {
    private static final double FAKE_POINT_MARKER = -1.0;

    public static List<List<byte[]>> greedyMerge(List<double[]> sortedPoints, int maxBucketSize, SecretKey key) throws Exception {
        List<List<byte[]>> buckets = new ArrayList<>();
        List<byte[]> currentBucket = new ArrayList<>();

        for (double[] point : sortedPoints) {
            byte[] encryptedPoint = (key != null) ? EncryptionUtils.encryptVector(point, key) : doubleToByteArray(point);
            currentBucket.add(encryptedPoint);
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

    public static List<List<byte[]>> applyFakeAddition(List<List<byte[]>> buckets, int targetSize, SecretKey key, int dimension) throws Exception {
        List<List<byte[]>> uniformBuckets = new ArrayList<>();
        for (List<byte[]> bucket : buckets) {
            List<byte[]> newBucket = new ArrayList<>(bucket);
            int numFake = targetSize - bucket.size();
            for (int i = 0; i < numFake; i++) {
                double[] fakePoint = generateFakePoint(dimension);
                byte[] encryptedFake = (key != null) ? EncryptionUtils.encryptVector(fakePoint, key) : doubleToByteArray(fakePoint);
                newBucket.add(encryptedFake);
            }
            uniformBuckets.add(newBucket);
        }
        return uniformBuckets;
    }

    public static List<List<byte[]>> reEncryptBuckets(List<List<byte[]>> buckets, SecretKey oldKey, SecretKey newKey) throws Exception {
        List<List<byte[]>> reEncryptedBuckets = new ArrayList<>();
        for (List<byte[]> bucket : buckets) {
            List<byte[]> newBucket = new ArrayList<>();
            for (byte[] encryptedPoint : bucket) {
                double[] decryptedPoint = EncryptionUtils.decryptVector(encryptedPoint, oldKey);
                byte[] reEncryptedPoint = EncryptionUtils.encryptVector(decryptedPoint, newKey);
                newBucket.add(reEncryptedPoint);
            }
            reEncryptedBuckets.add(newBucket);
        }
        return reEncryptedBuckets;
    }

    private static double[] generateFakePoint(int dimension) {
        double[] fake = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            fake[i] = FAKE_POINT_MARKER + Math.random();
        }
        return fake;
    }

    private static byte[] doubleToByteArray(double[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double value : vector) {
            buffer.putDouble(value);
        }
        return buffer.array();
    }

    public static double[] byteToDoubleArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] vector = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();
        }
        return vector;
    }
}