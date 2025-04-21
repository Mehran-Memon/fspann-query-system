package com.fspann.fspann_query_system.data;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SiftFileReader {

    // Read .fvecs file (float vectors)
    public static float[][] readFvecs(String filename) throws IOException {
        // Read all bytes from the file
        byte[] bytes = Files.readAllBytes(Paths.get(filename));
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        // List to store vectors
        List<float[]> vectors = new ArrayList<>();

        while (buffer.hasRemaining()) {
            // Read dimension (first 4 bytes as int)
            int dimension = buffer.getInt();
            if (dimension <= 0 || dimension * 4 > buffer.remaining()) {
                throw new IOException("Invalid dimension or corrupted file: " + dimension);
            }

            // Read the vector (dimension floats, 4 bytes each)
            float[] vector = new float[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = buffer.getFloat();
            }
            vectors.add(vector);
        }

        // Convert List to 2D array
        return vectors.toArray(new float[0][]);
    }

    // Read .ivecs file (integer vectors)
    public static int[][] readIvecs(String filename) throws IOException {
        // Read all bytes from the file
        byte[] bytes = Files.readAllBytes(Paths.get(filename));
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        // List to store vectors
        List<int[]> vectors = new ArrayList<>();

        while (buffer.hasRemaining()) {
            // Read dimension (first 4 bytes as int)
            int dimension = buffer.getInt();
            if (dimension <= 0 || dimension * 4 > buffer.remaining()) {
                throw new IOException("Invalid dimension or corrupted file: " + dimension);
            }

            // Read the vector (dimension ints, 4 bytes each)
            int[] vector = new int[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = buffer.getInt();
            }
            vectors.add(vector);
        }

        // Convert List to 2D array
        return vectors.toArray(new int[0][]);
    }

    // Main method to test the reading
    public static void main(String[] args) {
        try {
            // Example file paths
            String baseFile = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\sift_dataset\\sift\\sift_base.fvecs";
            String learnFile = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\sift_dataset\\sift\\sift_groundtruth.ivecs";
            String queryFile = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\sift_dataset\\sift\\sift_learn.fvecs";
            String groundtruthFile = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\sift_dataset\\sift\\sift_query.fvecs";

            // Read the files
            float[][] baseVectors = readFvecs(baseFile);
            float[][] learnVectors = readFvecs(learnFile);
            float[][] queryVectors = readFvecs(queryFile);
            int[][] groundtruth = readIvecs(groundtruthFile);

            // Print basic info
            System.out.println("Base vectors: " + baseVectors.length + " x " + baseVectors[0].length);
            System.out.println("Learn vectors: " + learnVectors.length + " x " + learnVectors[0].length);
            System.out.println("Query vectors: " + queryVectors.length + " x " + queryVectors[0].length);
            System.out.println("Ground truth: " + groundtruth.length + " x " + groundtruth[0].length);

            // Optional: Print first vector of base as an example
            System.out.print("First base vector: [");
            for (int i = 0; i < Math.min(5, baseVectors[0].length); i++) {
                System.out.print(baseVectors[0][i] + (i < 4 ? ", " : ""));
            }
            System.out.println("...]");

        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
        }
    }
}
