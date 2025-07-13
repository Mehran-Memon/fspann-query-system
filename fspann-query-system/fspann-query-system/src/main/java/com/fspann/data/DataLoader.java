package com.fspann.data;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader {
    private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

    /**
     * Reads a list of float vectors from a .fvecs file.
     *
     * @param filename Path to the .fvecs file
     * @return List of double[] vectors
     * @throws IOException If there’s an error reading the file or the data is invalid
     */
    public List<double[]> readFvecs(String filename) throws IOException {
        List<double[]> vectors = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] dimBuffer = new byte[4];
            int vectorCount = 0;
            int expectedDim = -1;

            while (fis.read(dimBuffer) == 4) {
                // Read dimension using Little Endian
                int dim = ByteBuffer.wrap(dimBuffer).order(ByteOrder.LITTLE_ENDIAN).getInt();

                if (dim <= 0 || dim > 10000) {
                    throw new IOException("Invalid dimension " + dim + " in file: " + filename + " at vector " + vectorCount);
                }

                if (expectedDim == -1) {
                    expectedDim = dim;
                } else if (dim != expectedDim) {
                    throw new IOException("Inconsistent dimensions in file: " + filename +
                            ", expected " + expectedDim + ", got " + dim + " at vector " + vectorCount);
                }

                // Read float32 values (4 bytes per float)
                byte[] vectorBytes = new byte[dim * 4];
                if (fis.read(vectorBytes) != vectorBytes.length) {
                    throw new IOException("Incomplete vector at vector " + vectorCount);
                }

                ByteBuffer vecBuffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.LITTLE_ENDIAN);
                double[] vector = new double[dim];
                for (int i = 0; i < dim; i++) {
                    vector[i] = vecBuffer.getFloat(); // Convert float to double
                }

                vectors.add(vector);
                vectorCount++;
            }

            logger.info("✅ Finished reading {} vectors from {}", vectors, filename);
        }

        return vectors;
    }


    /**
     * Reads a list of integer vectors from a .ivecs file.
     *
     * @param filename Path to the .ivecs file
     * @return List of int[] vectors
     * @throws IOException If there’s an error reading the file or the data is invalid
     */
    public List<int[]> readIvecs(String filename) throws IOException {
        List<int[]> vectors = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] dimBuffer = new byte[4];
            int vectorCount = 0;
            int expectedDim = -1;

            while (fis.read(dimBuffer) == 4) {
                int dim = ByteBuffer.wrap(dimBuffer).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (dim <= 0 || dim > 10000) {
                    throw new IOException("Invalid dimension in ivecs file: " + filename + " at vector " + vectorCount + ", got: " + dim);
                }

                if (expectedDim == -1) {
                    expectedDim = dim;
                } else if (dim != expectedDim) {
                    throw new IOException("Inconsistent dimension at vector " + vectorCount + ": expected " + expectedDim + ", got " + dim);
                }

                byte[] vectorBytes = new byte[dim * 4];
                if (fis.read(vectorBytes) != vectorBytes.length) {
                    throw new IOException("Failed to read full vector at index " + vectorCount);
                }

                ByteBuffer vecBuffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.LITTLE_ENDIAN);
                int[] vector = new int[dim];
                for (int i = 0; i < dim; i++) {
                    vector[i] = vecBuffer.getInt();
                }

                vectors.add(vector);
                vectorCount++;
            }

            logger.info("✅ Finished reading {} vectors from {}", vectors.size(), filename);
        }

        return vectors;
    }

}