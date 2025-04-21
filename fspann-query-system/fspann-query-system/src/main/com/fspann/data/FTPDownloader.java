package com.fspann.data;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * A Java program to download a dataset from an FTP server.
 * This program downloads the SIFT dataset from ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
 */
public class FTPDownloader {

    // FTP URL to download the dataset
    private static final String FTP_URL = "ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz";

    // Local file path where the dataset will be saved
    private static final String OUTPUT_FILE = "gist.tar.gz";

    // Buffer size for reading/writing data
    private static final int BUFFER_SIZE = 8192;

    public static void main(String[] args) {
        System.out.println("Starting download of GIST dataset...");
        System.out.println("Source: " + FTP_URL);
        System.out.println("Destination: " + OUTPUT_FILE);

        try {
            // Create URL object
            URL url = new URL(FTP_URL);

            // Open connection to the URL
            URLConnection connection = url.openConnection();

            // Get input stream from the connection
            InputStream inputStream = connection.getInputStream();

            // Get the file size (if available)
            int fileSize = connection.getContentLength();
            if (fileSize > 0) {
                System.out.println("File size: " + formatFileSize(fileSize));
            } else {
                System.out.println("File size: Unknown");
            }

            // Create output stream to save the file
            BufferedOutputStream outputStream = new BufferedOutputStream(
                    new FileOutputStream(OUTPUT_FILE));

            // Buffer for reading data
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            long totalBytesRead = 0;
            long startTime = System.currentTimeMillis();

            // Read from input and write to output
            System.out.println("Download in progress...");
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;

                // Print progress if file size is known
                if (fileSize > 0) {
                    int percentComplete = (int) (totalBytesRead * 100 / fileSize);
                    System.out.print("\rProgress: " + percentComplete + "% (" +
                            formatFileSize(totalBytesRead) + " / " + formatFileSize(fileSize) + ")");
                } else {
                    // Print progress without percentage if file size is unknown
                    System.out.print("\rDownloaded: " + formatFileSize(totalBytesRead));
                }
            }

            // Close streams
            outputStream.close();
            inputStream.close();

            long endTime = System.currentTimeMillis();
            double timeInSeconds = (endTime - startTime) / 1000.0;

            System.out.println("\nDownload completed successfully!");
            System.out.println("Total downloaded: " + formatFileSize(totalBytesRead));
            System.out.println("Time taken: " + String.format("%.2f", timeInSeconds) + " seconds");
            System.out.println("Average speed: " + formatFileSize((long)(totalBytesRead / timeInSeconds)) + "/s");
            System.out.println("File saved as: " + OUTPUT_FILE);

        } catch (IOException e) {
            System.err.println("\nError during download: " + e.getMessage());
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
