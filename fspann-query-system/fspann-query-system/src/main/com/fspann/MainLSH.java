package com.fspann;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.List;
import java.util.ArrayList;
import com.fspann.index.EvenLSH;
import com.fspann.index.BucketConstructor;

public class MainLSH {
    public static void main(String[] args) {
        try {
            // Initialize encryption key
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(128);
            SecretKey secretKey = keyGen.generateKey();

            // Example data: points to be inserted and processed
            List<double[]> dataPoints = generateSampleData(1000, 128); // Generate 1000 random points with 128 dimensions

            // Step 1: Perform LSH
            EvenLSH lsh = new EvenLSH(128, 10, dataPoints);
            for (double[] point : dataPoints) {
                int bucketId = lsh.getBucketId(point);
                System.out.println("Bucket ID for point: " + bucketId);
            }

            // Step 2: Perform greedy merge
            List<List<byte[]>> buckets = BucketConstructor.greedyMerge(dataPoints, 100, secretKey);

            // Step 3: Apply fake point insertion
            List<List<byte[]>> updatedBuckets = BucketConstructor.applyFakeAddition(buckets, 200, secretKey, 128);

            // Logging the results
            System.out.println("Buckets after fake point insertion:");
            for (List<byte[]> bucket : updatedBuckets) {
                System.out.println("Bucket size: " + bucket.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper function to generate sample data points
    public static List<double[]> generateSampleData(int numPoints, int dimensions) {
        List<double[]> data = new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int j = 0; j < dimensions; j++) {
                point[j] = Math.random();
            }
            data.add(point);
        }
        return data;
    }
}
