package com.fspann.data;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader {
    private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

    /**
     * Reads a list of data vectors from a given file, which can be in different formats (CSV, JSON, etc.).
     *
     * @param filename Path to the data file.
     * @param batchSize Number of records to load in each batch.
     * @return List of vectors as doubles or floats.
     * @throws IOException If there’s an error reading the file or the data is invalid.
     */
    public List<double[]> loadData(String filename, int batchSize) throws IOException {
        String format = detectFileFormat(filename); // Detect the file format based on file extension
        try {
            return switch (format.toUpperCase()) {
                case "CSV" -> loadCSV(filename, batchSize);
                case "JSON" -> loadJSON(filename, batchSize);
                case "PARQUET" -> loadParquet(filename, batchSize);
                case "FVECS" -> loadFvecs(filename, batchSize);
                case "IVECS" -> loadIvecs(filename, batchSize);
                case "NPZ" -> loadNPZ(filename, batchSize);
                default -> throw new UnsupportedOperationException("Format " + format + " is not supported.");
            };
        } catch (IOException e) {
            logger.error("Error reading file: {}", filename, e);
            throw new IOException("Failed to load data from file: " + filename, e);
        }
    }


    /**
     * Detects the file format based on the file extension.
     *
     * @param filename The file to check.
     * @return A string representing the file format (CSV, JSON, Parquet, etc.).
     */
    private String detectFileFormat(String filename) {
        String extension = filename.substring(filename.lastIndexOf(".") + 1).toUpperCase();
        logger.info("Detected file format: " + extension);
        return extension;
    }

    /**
     * Loads data from a CSV file.
     * Assumes that the CSV data is numeric and structured in rows and columns.
     *
     * @param filename Path to the CSV file.
     * @param batchSize Number of records to load in each batch.
     * @return A list of double arrays representing the data vectors.
     * @throws IOException
     */
    private List<double[]> loadCSV(String filename, int batchSize) throws IOException {
        List<double[]> data = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                double[] vector = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    try {
                        vector[i] = Double.parseDouble(values[i]);
                    } catch (NumberFormatException e) {
                        logger.warn("Skipping invalid number in line: " + line);
                        continue;
                    }
                }
                data.add(vector);
                if (data.size() >= batchSize) {
                    processBatch(data);
                    data.clear();
                }
            }
        }
        if (!data.isEmpty()) {
            processBatch(data); // Process the remaining data if any
        }
        return data;
    }

    /**
     * Loads data from a JSON file.
     * This method should be further extended to process nested or complex JSON structures.
     *
     * @param filename Path to the JSON file.
     * @param batchSize Number of records to load in each batch.
     * @return A list of double arrays.
     * @throws IOException
     */
    private List<double[]> loadJSON(String filename, int batchSize) throws IOException {
        // Placeholder: Add JSON processing logic here
        logger.info("Loading JSON data from: " + filename);
        return new ArrayList<>(); // Return an empty list as a placeholder
    }

    /**
     * Loads data from a Parquet file.
     * Placeholder for handling Parquet files (typically used with Apache Arrow).
     *
     * @param filename Path to the Parquet file.
     * @param batchSize Number of records to load in each batch.
     * @return A list of double arrays.
     * @throws IOException
     */
    private List<double[]> loadParquet(String filename, int batchSize) throws IOException {
        // Placeholder for Parquet handling logic
        logger.info("Loading Parquet data from: " + filename);
        return new ArrayList<>(); // Return an empty list as a placeholder
    }

    private List<double[]> loadFvecs(String filename, int batchSize) throws IOException {
        List<double[]> vectors = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] dimBuffer = new byte[4];  // 4 bytes for storing the dimension of the vector
            int vectorCount = 0;
            int expectedDim = -1;

            while (fis.read(dimBuffer) != -1) {
                // Set the byte order to little-endian
                ByteBuffer dimByteBuffer = ByteBuffer.wrap(dimBuffer).order(ByteOrder.LITTLE_ENDIAN);

                // Read the dimension of the vector (4 bytes)
                int dim = dimByteBuffer.getInt();

                if (expectedDim == -1) {
                    expectedDim = dim;  // Set expected dimension based on the first vector
                }

                // Create a vector of the given dimension
                double[] vector = new double[dim];
                byte[] vectorBuffer = new byte[dim * 4];  // 4 bytes per float value
                fis.read(vectorBuffer);

                // For .fvecs, interpret data as floats
                ByteBuffer.wrap(vectorBuffer).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(new float[dim]);

                float[] tempVector = new float[dim];
                ByteBuffer.wrap(vectorBuffer).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(tempVector);

                // Convert float vector to double
                for (int i = 0; i < dim; i++) {
                    vector[i] = (double) tempVector[i];
                }

                vectors.add(vector);

                vectorCount++;
                if (vectorCount >= batchSize) {
                    processBatch(vectors);
                    vectors.clear();
                    vectorCount = 0;
                }
                logger.info("Loaded vector with dimension: " + dim);

            }
        } catch (IOException e) {
            logger.error("Error reading .fvecs file: " + filename, e);
            throw new IOException("Failed to load data from .fvecs file: " + filename, e);
        }

        if (!vectors.isEmpty()) {
            processBatch(vectors);
        }
        return vectors;
    }

    private List<double[]> loadIvecs(String filename, int batchSize) throws IOException {
        List<double[]> vectors = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] dimBuffer = new byte[4];  // 4 bytes for storing the dimension of the vector
            int vectorCount = 0;
            int expectedDim = -1;

            while (fis.read(dimBuffer) != -1) {
                // Set the byte order to little-endian
                ByteBuffer dimByteBuffer = ByteBuffer.wrap(dimBuffer).order(ByteOrder.LITTLE_ENDIAN);

                // Read the dimension of the vector (4 bytes)
                int dim = dimByteBuffer.getInt();

                if (expectedDim == -1) {
                    expectedDim = dim;  // Set expected dimension based on the first vector
                }

                // Create a vector of the given dimension
                double[] vector = new double[dim];
                byte[] vectorBuffer = new byte[dim * 4];  // 4 bytes per integer value
                fis.read(vectorBuffer);

                // For .ivecs, interpret data as integers
                int[] intVector = new int[dim];
                ByteBuffer.wrap(vectorBuffer).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(intVector);

                // Convert int vector to double
                for (int i = 0; i < dim; i++) {
                    vector[i] = (double) intVector[i];  // Convert integer vector to double
                }

                vectors.add(vector);

                vectorCount++;
                if (vectorCount >= batchSize) {
                    processBatch(vectors);
                    vectors.clear();
                    vectorCount = 0;
                }
                logger.info("Loaded vector with dimension: " + dim);

            }
        } catch (IOException e) {
            logger.error("Error reading .ivecs file: " + filename, e);
            throw new IOException("Failed to load data from .ivecs file: " + filename, e);
        }

        if (!vectors.isEmpty()) {
            processBatch(vectors);
        }
        return vectors;
    }

    /**
     * Loads data from an NPZ file (compressed NumPy array format).
     * For high-dimensional numerical data, NPZ is efficient.
     *
     * @param filename Path to the NPZ file.
     * @param batchSize Number of records to load in each batch.
     * @return A list of double arrays.
     * @throws IOException
     */
    private List<double[]> loadNPZ(String filename, int batchSize) throws IOException {
        // Placeholder for NPZ handling logic
        logger.info("Loading NPZ data from: " + filename);
        return new ArrayList<>(); // Return an empty list as a placeholder
    }

    /**
     * Processes a batch of data. You can apply transformations, logging, or other processing steps here.
     *
     * @param data The batch of data to process.
     */
    private void processBatch(List<double[]> data) {
        // Implement your batch processing logic here
        logger.info("Processing batch of size: {}", data.size());
    }
}
