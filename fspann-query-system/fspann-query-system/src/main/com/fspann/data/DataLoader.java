package com.fspann.data;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataLoader {
    private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

    private static final int DEFAULT_BATCH_SIZE = 1000; // Default batch size if not provided

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
            return loadDataByFormat(filename, format, batchSize);
        } catch (IOException e) {
            logger.error("Error reading file: {}", filename, e);
            throw new IOException("Failed to load data from file: " + filename, e);
        }
    }

    private List<double[]> loadDataByFormat(String filename, String format, int batchSize) throws IOException {
        switch (format.toUpperCase()) {
            case "CSV":
                return loadCSV(filename, batchSize);
            case "JSON":
                return loadJSON(filename, batchSize);
            case "PARQUET":
                return loadParquet(filename, batchSize);
            case "FVECS":
                return loadFvecs(filename, batchSize);
            case "IVECS":
                return loadIvecs(filename, batchSize);
            case "NPZ":
                return loadNPZ(filename, batchSize);
            default:
                throw new UnsupportedOperationException("Format " + format + " is not supported.");
        }
    }

    public List<int[]> loadGroundTruth(String filename, int batchSize) throws IOException {
        List<int[]> groundTruth = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] dimBuffer = new byte[4];
            int vectorCount = 0;

            while (fis.available() > 0) {
                int bytesRead = fis.read(dimBuffer);
                if (bytesRead < 4) {
                    logger.warn("Incomplete dimension read: expected 4 bytes, got {}", bytesRead);
                    break;
                }
                ByteBuffer dimByteBuffer = ByteBuffer.wrap(dimBuffer).order(ByteOrder.LITTLE_ENDIAN);
                int dim = dimByteBuffer.getInt();

                byte[] vectorBuffer = new byte[dim * 4];
                int vectorBytesRead = fis.read(vectorBuffer);
                if (vectorBytesRead != dim * 4) {
                    logger.warn("Incomplete vector read: expected {} bytes, got {}", dim * 4, vectorBytesRead);
                    break;
                }

                int[] vector = new int[dim];
                ByteBuffer.wrap(vectorBuffer).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(vector);
                groundTruth.add(vector);

                vectorCount++;
                if (vectorCount >= batchSize) {
                    vectorCount = 0;
                }
            }
        } catch (IOException e) {
            logger.error("Error reading .ivecs file: {}", filename, e);
            throw new IOException("Failed to load ground truth from .ivecs file: " + filename, e);
        }
        return groundTruth;
    }

    private String detectFileFormat(String filename) {
        String extension = filename.substring(filename.lastIndexOf(".") + 1).toUpperCase();
        return extension;
    }

    private List<double[]> loadCSV(String filename, int batchSize) throws IOException {
        return loadFileData(filename, batchSize, this::parseCSVLine);
    }

    private List<double[]> loadJSON(String filename, int batchSize) throws IOException {
        return loadFileData(filename, batchSize, this::parseJSONLine);
    }

    private List<double[]> loadFileData(String filename, int batchSize, LineParser parser) throws IOException {
        List<double[]> data = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                double[] vector = parser.parseLine(line);
                data.add(vector);
                if (data.size() >= batchSize) {
                    processBatch(data);
                    data.clear();
                }
            }
        }
        if (!data.isEmpty()) {
            processBatch(data); // Process remaining data
        }
        return data;
    }

    private double[] parseCSVLine(String line) {
        String[] values = line.split(",");
        double[] vector = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            try {
                vector[i] = Double.parseDouble(values[i]);
            } catch (NumberFormatException e) {
                logger.warn("Skipping invalid number in line: " + line);
                return new double[0]; // Return an empty vector for invalid lines
            }
        }
        return vector;
    }

    private double[] parseJSONLine(String line) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(line);
            double[] vector = new double[rootNode.size()];
            for (int i = 0; i < rootNode.size(); i++) {
                vector[i] = rootNode.get(i).asDouble();
            }
            return vector;
        } catch (Exception e) {
            logger.warn("Skipping invalid JSON line: " + line);
            return new double[0]; // Return an empty vector for invalid JSON lines
        }
    }

    private List<double[]> loadParquet(String filename, int batchSize) throws IOException {
        logger.info("Loading Parquet data from: " + filename);
        return new ArrayList<>();
    }

    private List<double[]> loadFvecs(String filename, int batchSize) throws IOException {
        List<double[]> allVectors = new ArrayList<>();
        List<double[]> batch = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] dimBuffer = new byte[4];
            int vectorCount = 0;
            int expectedDim = -1;

            while (fis.available() > 0) {
                // Read the dimension of the vector (4 bytes)
                int bytesRead = fis.read(dimBuffer);
                if (bytesRead < 4) {
                    logger.warn("Incomplete dimension read: expected 4 bytes, got {}", bytesRead);
                    break;
                }

                ByteBuffer dimByteBuffer = ByteBuffer.wrap(dimBuffer).order(ByteOrder.LITTLE_ENDIAN);
                int dim = dimByteBuffer.getInt();

                if (expectedDim == -1) {
                    expectedDim = dim;
                } else if (dim != expectedDim) {
                    logger.warn("Vector with different dimension found: expected {}, got {}", expectedDim, dim);
                    continue;
                }

                byte[] vectorBuffer = new byte[dim * 4];
                int vectorBytesRead = fis.read(vectorBuffer);
                if (vectorBytesRead != dim * 4) {
                    logger.warn("Incomplete vector read: expected {} bytes, got {}", dim * 4, vectorBytesRead);
                    break;
                }

                float[] tempVector = new float[dim];
                ByteBuffer.wrap(vectorBuffer).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(tempVector);

                double[] vector = new double[dim];
                for (int i = 0; i < dim; i++) {
                    vector[i] = tempVector[i];
                }

                allVectors.add(vector);
                batch.add(vector);

                vectorCount++;
                if (vectorCount >= batchSize) {
                    processBatch(batch);
                    batch.clear();
                    vectorCount = 0;
                }
            }
        } catch (IOException e) {
            logger.error("Error reading .fvecs file: {}", filename, e);
            throw new IOException("Failed to load data from .fvecs file: " + filename, e);
        }

        if (!batch.isEmpty()) {
            processBatch(batch);
        }
        return allVectors;
    }

    private double[] parseFvecsLine(String line) {
        // Example placeholder parsing logic for FVECS format
        return new double[0]; // Implement actual parsing for FVECS format
    }

    private List<double[]> loadIvecs(String filename, int batchSize) throws IOException {
        return loadFileData(filename, batchSize, this::parseIvecsLine);
    }

    private double[] parseIvecsLine(String line) {
        return new double[0];
    }

    private List<double[]> loadNPZ(String filename, int batchSize) throws IOException {
        logger.info("Loading NPZ data from: " + filename);
        return new ArrayList<>();
    }

    private void processBatch(List<double[]> data) {
    }

    @FunctionalInterface
    private interface LineParser {
        double[] parseLine(String line);
    }
}
