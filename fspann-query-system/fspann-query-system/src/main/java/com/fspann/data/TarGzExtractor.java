package com.fspann.data;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

/**
 * A Java program to extract a downloaded .tar.gz file.
 * This program extracts the SIFT dataset from sift.tar.gz
 */
public class TarGzExtractor {

    // Input file path (the downloaded .tar.gz file)
    private static final String INPUT_FILE = "sift.tar.gz";

    // Output directory where files will be extracted
    private static final String OUTPUT_DIR = "sift_dataset";

    // Buffer size for reading/writing data
    private static final int BUFFER_SIZE = 8192;

    public static void main(String[] args) {
        System.out.println("Starting extraction of " + INPUT_FILE);

        // Create output directory if it doesn't exist
        File outputDir = new File(OUTPUT_DIR);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
            System.out.println("Created output directory: " + OUTPUT_DIR);
        }

        try {
            // Create input streams for reading the .tar.gz file
            FileInputStream fileInputStream = new FileInputStream(INPUT_FILE);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            GZIPInputStream gzipInputStream = new GZIPInputStream(bufferedInputStream);
            TarArchiveInputStream tarInputStream = new TarArchiveInputStream(gzipInputStream);

            // Buffer for reading data
            byte[] buffer = new byte[BUFFER_SIZE];

            // Variables for tracking progress
            long totalBytesExtracted = 0;
            int fileCount = 0;
            long startTime = System.currentTimeMillis();

            System.out.println("Extraction in progress...");

            // Process each entry in the tar file
            TarArchiveEntry entry;
            while ((entry = tarInputStream.getNextTarEntry()) != null) {
                // Get the name of the entry
                String entryName = entry.getName();

                // Create the full path for the output file
                File outputFile = new File(outputDir, entryName);

                // If the entry is a directory, create it
                if (entry.isDirectory()) {
                    outputFile.mkdirs();
                    System.out.println("Created directory: " + outputFile.getPath());
                    continue;
                }

                // Ensure parent directories exist
                File parent = outputFile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }

                // Create output stream for the current file
                FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);

                // Read from tar input and write to output file
                int bytesRead;
                while ((bytesRead = tarInputStream.read(buffer)) != -1) {
                    bufferedOutputStream.write(buffer, 0, bytesRead);
                    totalBytesExtracted += bytesRead;
                }

                // Close output stream for current file
                bufferedOutputStream.close();
                fileOutputStream.close();

                // Update progress
                fileCount++;
                System.out.println("Extracted: " + outputFile.getPath() +
                        " (" + formatFileSize(outputFile.length()) + ")");
            }

            // Close all input streams
            tarInputStream.close();
            gzipInputStream.close();
            bufferedInputStream.close();
            fileInputStream.close();

            long endTime = System.currentTimeMillis();
            double timeInSeconds = (endTime - startTime) / 1000.0;

            System.out.println("\nExtraction completed successfully!");
            System.out.println("Files extracted: " + fileCount);
            System.out.println("Total data extracted: " + formatFileSize(totalBytesExtracted));
            System.out.println("Time taken: " + String.format("%.2f", timeInSeconds) + " seconds");
            System.out.println("Files extracted to: " + outputDir.getAbsolutePath());

        } catch (IOException e) {
            System.err.println("\nError during extraction: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Format file size in human-readable format
     * @param size File size in bytes
     * @return Formatted string
     */
    private static String formatFileSize(long size) {
        if (size < 1024) {
            return size + " B";
        } else if (size < 1024 * 1024) {
            return String.format("%.2f KB", size / 1024.0);
        } else if (size < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", size / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
        }
    }
}
