package com.fspann.index.lsh;

import java.io.*;
import java.util.*;

/**
 * RandomProjectionLSH – Hash Family Implementation (FINAL COMPLETE)
 *
 * Implements LSHHashFamily interface using random Gaussian projections for LSH bucket assignment.
 * Implements K hash functions per table via random projections.
 *
 * Includes adaptive bucket width adjustment for parameter tuning.
 *
 * @author FSP-ANNS Project
 * @version 3.0 (Final with adjustBucketWidth)
 */
public class RandomProjectionLSH implements LSHHashFamily {

    private int dimension;
    private int numTables;
    private int numFunctions;
    private int numBuckets;

    // Random projection matrices: [table][function][dimension]
    private double[][][] projections;

    // Bucket widths per table
    private double[] bucketWidths;

    // Random seed
    private long seed = 42L;

    public RandomProjectionLSH() {
    }

    /**
     * Initialize hash family with dimensions and parameters.
     *
     * @param dimension    vector dimension
     * @param numTables    L (number of hash tables)
     * @param numFunctions K (hash functions per table)
     * @param numBuckets   number of buckets per table
     */
    @Override
    public void init(int dimension, int numTables, int numFunctions, int numBuckets) {
        if (dimension <= 0 || numTables <= 0 || numFunctions <= 0 || numBuckets <= 0) {
            throw new IllegalArgumentException(
                    "All parameters must be positive: dimension=" + dimension +
                            ", tables=" + numTables + ", functions=" + numFunctions +
                            ", buckets=" + numBuckets);
        }

        this.dimension = dimension;
        this.numTables = numTables;
        this.numFunctions = numFunctions;
        this.numBuckets = numBuckets;

        // Generate random projections
        this.projections = new double[numTables][numFunctions][dimension];
        this.bucketWidths = new double[numTables];

        Random rand = new Random(seed);
        for (int t = 0; t < numTables; t++) {
            for (int f = 0; f < numFunctions; f++) {
                for (int d = 0; d < dimension; d++) {
                    projections[t][f][d] = rand.nextGaussian();
                }
            }
            bucketWidths[t] = 1.0;
        }
    }

    /**
     * Hash a vector to a bucket in a given table and function.
     *
     * @param vector    vector to hash
     * @param tableId   table index (0 to numTables-1)
     * @param functionId function index (0 to numFunctions-1)
     * @return bucket ID (0 to numBuckets-1)
     */
    @Override
    public int hash(double[] vector, int tableId, int functionId) {
        if (vector == null || vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch. Expected: " + dimension +
                            ", got: " + (vector == null ? "null" : vector.length));
        }

        if (tableId < 0 || tableId >= numTables) {
            throw new IllegalArgumentException("Invalid table ID: " + tableId);
        }

        if (functionId < 0 || functionId >= numFunctions) {
            throw new IllegalArgumentException("Invalid function ID: " + functionId);
        }

        // Compute dot product with projection
        double dot = 0.0;
        double[] proj = projections[tableId][functionId];

        for (int i = 0; i < dimension; i++) {
            dot += vector[i] * proj[i];
        }

        // Quantize to bucket
        double width = bucketWidths[tableId];
        int bucket = (int) Math.floor(dot / width);

        // Map to [0, numBuckets)
        bucket = ((bucket % numBuckets) + numBuckets) % numBuckets;

        return bucket;
    }

    /**
     * Hash a batch of vectors for a specific table.
     *
     * @param vectors   2D array of vectors (n × d)
     * @param tableId   table index
     * @return 2D array of hash values (n × K)
     */
    @Override
    public int[][] hashBatch(double[][] vectors, int tableId) {
        if (vectors == null || vectors.length == 0) {
            return new int[0][numFunctions];
        }

        if (tableId < 0 || tableId >= numTables) {
            throw new IllegalArgumentException("Invalid table ID: " + tableId);
        }

        int n = vectors.length;
        int[][] result = new int[n][numFunctions];

        for (int i = 0; i < n; i++) {
            for (int f = 0; f < numFunctions; f++) {
                result[i][f] = hash(vectors[i], tableId, f);
            }
        }

        return result;
    }

    /**
     * Estimate collision probability for two vectors.
     *
     * @param distance  Euclidean distance between vectors
     * @param tableId   table index
     * @return probability in [0.0, 1.0]
     */
    @Override
    public double collisionProbability(double distance, int tableId) {
        if (tableId < 0 || tableId >= numTables) {
            throw new IllegalArgumentException("Invalid table ID: " + tableId);
        }

        if (distance < 0) {
            throw new IllegalArgumentException("Distance must be non-negative");
        }

        // For random projections with Gaussian vectors:
        // P(h(u) = h(v)) ≈ 1 - 2*distance/(π*width)
        // where width is bucket width and distance is Euclidean distance

        double width = bucketWidths[tableId];

        if (distance == 0) {
            return 1.0;
        }

        double prob = 1.0 - (2.0 * distance) / (Math.PI * width);

        // Clamp to [0, 1]
        return Math.max(0.0, Math.min(1.0, prob));
    }

    /**
     * Serialize hash functions to byte array.
     *
     * Format:
     *  - dimension (4 bytes)
     *  - numTables (4 bytes)
     *  - numFunctions (4 bytes)
     *  - numBuckets (4 bytes)
     *  - seed (8 bytes)
     *  - All projections (numTables * numFunctions * dimension * 8 bytes)
     *  - All bucket widths (numTables * 8 bytes)
     *
     * @return byte array with serialized state
     */
    @Override
    public byte[] serialize() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Write header
            dos.writeInt(dimension);
            dos.writeInt(numTables);
            dos.writeInt(numFunctions);
            dos.writeInt(numBuckets);
            dos.writeLong(seed);

            // Write projections
            if (projections != null) {
                for (int t = 0; t < numTables; t++) {
                    for (int f = 0; f < numFunctions; f++) {
                        for (int d = 0; d < dimension; d++) {
                            dos.writeDouble(projections[t][f][d]);
                        }
                    }
                }
            }

            // Write bucket widths
            if (bucketWidths != null) {
                for (int t = 0; t < numTables; t++) {
                    dos.writeDouble(bucketWidths[t]);
                }
            }

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    /**
     * Deserialize hash functions from byte array.
     *
     * @param data byte array from serialize()
     */
    @Override
    public void deserialize(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            // Read header
            this.dimension = dis.readInt();
            this.numTables = dis.readInt();
            this.numFunctions = dis.readInt();
            this.numBuckets = dis.readInt();
            this.seed = dis.readLong();

            // Read projections
            this.projections = new double[numTables][numFunctions][dimension];
            for (int t = 0; t < numTables; t++) {
                for (int f = 0; f < numFunctions; f++) {
                    for (int d = 0; d < dimension; d++) {
                        projections[t][f][d] = dis.readDouble();
                    }
                }
            }

            // Read bucket widths
            this.bucketWidths = new double[numTables];
            for (int t = 0; t < numTables; t++) {
                bucketWidths[t] = dis.readDouble();
            }
        } catch (IOException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }

    /**
     * Get configuration information.
     *
     * @return string representation of configuration
     */
    @Override
    public String getConfiguration() {
        return String.format(
                "RandomProjectionLSH{dim=%d, tables=%d, functions=%d, buckets=%d, seed=%d}",
                dimension, numTables, numFunctions, numBuckets, seed);
    }

    // ====================================================================
    // ADAPTIVE TUNING METHODS
    // ====================================================================

    /**
     * Tune bucket width to achieve target collision probability.
     *
     * @param targetCollisionProb desired collision probability (e.g., 0.3)
     */
    public void tuneBucketWidth(double targetCollisionProb) {
        if (targetCollisionProb <= 0 || targetCollisionProb >= 1) {
            throw new IllegalArgumentException(
                    "Target collision probability must be in (0, 1)");
        }

        // Approximate: width = (d / (2 * P(c))) where P(c) is collision prob
        // This is a heuristic tuning
        for (int t = 0; t < numTables; t++) {
            double normExpectation = Math.sqrt(dimension / 2.0);
            double newWidth = normExpectation / (2.0 * targetCollisionProb);
            bucketWidths[t] = Math.max(0.1, newWidth);
        }
    }

    /**
     * Adjust bucket width for a specific table by a factor.
     * Used for adaptive parameter tuning based on query metrics.
     *
     * @param tableId   table index
     * @param factor    adjustment factor (< 1.0 to decrease width, > 1.0 to increase)
     * @throws IllegalArgumentException if tableId invalid or factor <= 0
     */
    public void adjustBucketWidth(int tableId, double factor) {
        if (tableId < 0 || tableId >= numTables) {
            throw new IllegalArgumentException("Invalid table ID: " + tableId);
        }

        if (factor <= 0) {
            throw new IllegalArgumentException("Adjustment factor must be positive");
        }

        double oldWidth = bucketWidths[tableId];
        double newWidth = oldWidth * factor;

        // Clamp to reasonable range [0.01, 1000.0]
        newWidth = Math.max(0.01, Math.min(1000.0, newWidth));

        bucketWidths[tableId] = newWidth;
    }

    /**
     * Adjust all bucket widths by a common factor.
     * Used for global parameter tuning.
     *
     * @param factor adjustment factor (< 1.0 to decrease, > 1.0 to increase)
     */
    public void adjustAllBucketWidths(double factor) {
        if (factor <= 0) {
            throw new IllegalArgumentException("Adjustment factor must be positive");
        }

        for (int t = 0; t < numTables; t++) {
            adjustBucketWidth(t, factor);
        }
    }

    /**
     * Get bucket width for a table.
     */
    public double getBucketWidth(int tableId) {
        if (tableId < 0 || tableId >= numTables) {
            throw new IllegalArgumentException("Invalid table ID");
        }
        return bucketWidths[tableId];
    }

    /**
     * Set bucket width for a table (manual tuning).
     */
    public void setBucketWidth(int tableId, double width) {
        if (tableId < 0 || tableId >= numTables) {
            throw new IllegalArgumentException("Invalid table ID");
        }
        if (width <= 0) {
            throw new IllegalArgumentException("Bucket width must be positive");
        }
        bucketWidths[tableId] = width;
    }

    /**
     * Get all bucket widths for diagnostic purposes.
     */
    public double[] getAllBucketWidths() {
        return bucketWidths.clone();
    }

    // ====================================================================
    // QUERY HELPER METHODS
    // ====================================================================

    /**
     * Get dimension.
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Get number of tables.
     */
    public int getNumTables() {
        return numTables;
    }

    /**
     * Get number of functions per table.
     */
    public int getNumFunctions() {
        return numFunctions;
    }

    /**
     * Get number of buckets.
     */
    public int getNumBuckets() {
        return numBuckets;
    }

    /**
     * Check if initialized.
     */
    public boolean isInitialized() {
        return projections != null && projections.length > 0;
    }

    @Override
    public String toString() {
        return String.format(
                "RandomProjectionLSH{dim=%d, tables=%d, functions=%d, buckets=%d}",
                dimension, numTables, numFunctions, numBuckets);
    }
}