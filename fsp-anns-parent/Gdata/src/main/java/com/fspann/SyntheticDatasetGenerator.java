package com.fspann;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.Random;

public class SyntheticDatasetGenerator {

    // Configurable constants
    private static final int NUM_VECTORS_STORAGE = 1_000_000;
    private static final int NUM_VECTORS_QUERY = 10_000;

    private static final double MIN_VALUE = -1.0;
    private static final double MAX_VALUE = 1.0;

    public static void main(String[] args) {
        // Target output folder
        String outputDir = "datasetsMillion";

        int[] dimensions = {128, 256, 512, 1024};

        try {
            // STORAGE
            String storageDir = outputDir + "/storage";
            Files.createDirectories(Paths.get(storageDir));

            for (int dim : dimensions) {
                generateUniformDataset(storageDir + "/synthetic_uniform_" + dim + "d.csv", dim, NUM_VECTORS_STORAGE);
                generateGaussianDataset(storageDir + "/synthetic_gaussian_" + dim + "d.csv", dim, NUM_VECTORS_STORAGE);
            }

            // QUERY
            String queryDir = outputDir + "/query";
            Files.createDirectories(Paths.get(queryDir));

            for (int dim : dimensions) {
                generateUniformDataset(queryDir + "/synthetic_uniform_" + dim + "d.csv", dim, NUM_VECTORS_QUERY);
                generateGaussianDataset(queryDir + "/synthetic_gaussian_" + dim + "d.csv", dim, NUM_VECTORS_QUERY);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to create output directories", e);
        }

        System.out.println("✅ All synthetic datasets created in '" + outputDir + "' folder.");
    }

    public static void generateUniformDataset(String filename, int dimension, int numVectors) {
        System.out.println("Generating UNIFORM dataset: " + filename + " with " + numVectors + " vectors of dimension " + dimension);
        Random rand = new Random();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (int i = 0; i < numVectors; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < dimension; j++) {
                    double value = MIN_VALUE + (MAX_VALUE - MIN_VALUE) * rand.nextDouble();
                    sb.append(value);
                    if (j < dimension - 1) sb.append(",");
                }
                writer.write(sb.toString());
                writer.newLine();

                if ((i + 1) % 100000 == 0) {
                    System.out.println("  [Uniform] ... written " + (i + 1) + " vectors");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write uniform dataset: " + filename, e);
        }
    }

    public static void generateGaussianDataset(String filename, int dimension, int numVectors) {
        System.out.println("Generating GAUSSIAN dataset: " + filename + " with " + numVectors + " vectors of dimension " + dimension);
        Random rand = new Random();

        // Gaussian parameters
        double mean = 0.0;
        double stddev = 1.0;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (int i = 0; i < numVectors; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < dimension; j++) {
                    double value = mean + stddev * rand.nextGaussian();
                    sb.append(value);
                    if (j < dimension - 1) sb.append(",");
                }
                writer.write(sb.toString());
                writer.newLine();

                if ((i + 1) % 100000 == 0) {
                    System.out.println("  [Gaussian] ... written " + (i + 1) + " vectors");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write gaussian dataset: " + filename, e);
        }
    }
}
