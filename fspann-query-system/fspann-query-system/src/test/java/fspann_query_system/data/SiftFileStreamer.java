package fspann_query_system.data;

import java.io.*;

public class SiftFileStreamer {

    // Class to read .fvecs files incrementally
    static class FvecsReader implements AutoCloseable {
        private DataInputStream dis;
        private int dimension = -1;

        public FvecsReader(String filename) throws IOException {
            this.dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
        }

        // Read the next float vector; returns null if end of file is reached
        public float[] readNextVector() throws IOException {
            if (dis.available() <= 0) {
                return null; // End of file
            }

            // Read dimension (4 bytes, little-endian)
            byte[] dimBytes = new byte[4];
            int bytesRead = dis.read(dimBytes);
            if (bytesRead < 4) {
                return null; // Incomplete data
            }
            int dim = toLittleEndianInt(dimBytes);
            if (dim <= 0 || dim > 1_000_000) { // Sanity check
                throw new IOException("Invalid dimension: " + dim);
            }

            if (dimension == -1) {
                dimension = dim; // Set dimension from the first vector
            } else if (dim != dimension) {
                throw new IOException("Inconsistent dimension: expected " + dimension + ", got " + dim);
            }

            // Read the vector (dimension floats, 4 bytes each)
            float[] vector = new float[dimension];
            byte[] floatBytes = new byte[4];
            for (int i = 0; i < dimension; i++) {
                bytesRead = dis.read(floatBytes);
                if (bytesRead < 4) {
                    throw new IOException("Incomplete vector data at index " + i);
                }
                vector[i] = Float.intBitsToFloat(toLittleEndianInt(floatBytes));
            }
            return vector;
        }

        // Convert 4 bytes to little-endian int
        private int toLittleEndianInt(byte[] bytes) {
            return (bytes[0] & 0xFF) |
                    ((bytes[1] & 0xFF) << 8) |
                    ((bytes[2] & 0xFF) << 16) |
                    ((bytes[3] & 0xFF) << 24);
        }

        public int getDimension() {
            return dimension;
        }

        @Override
        public void close() throws IOException {
            dis.close();
        }
    }

    // Class to read .ivecs files incrementally
    static class IvecsReader implements AutoCloseable {
        private DataInputStream dis;
        private int dimension = -1;

        public IvecsReader(String filename) throws IOException {
            this.dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
        }

        // Read the next integer vector; returns null if end of file is reached
        public int[] readNextVector() throws IOException {
            if (dis.available() <= 0) {
                return null; // End of file
            }

            // Read dimension (4 bytes, little-endian)
            byte[] dimBytes = new byte[4];
            int bytesRead = dis.read(dimBytes);
            if (bytesRead < 4) {
                return null; // Incomplete data
            }
            int dim = toLittleEndianInt(dimBytes);
            if (dim <= 0 || dim > 1_000_000) { // Sanity check
                throw new IOException("Invalid dimension: " + dim);
            }

            if (dimension == -1) {
                dimension = dim; // Set dimension from the first vector
            } else if (dim != dimension) {
                throw new IOException("Inconsistent dimension: expected " + dimension + ", got " + dim);
            }

            // Read the vector (dimension ints, 4 bytes each)
            int[] vector = new int[dimension];
            byte[] intBytes = new byte[4];
            for (int i = 0; i < dimension; i++) {
                bytesRead = dis.read(intBytes);
                if (bytesRead < 4) {
                    throw new IOException("Incomplete vector data at index " + i);
                }
                vector[i] = toLittleEndianInt(intBytes);
            }
            return vector;
        }

        // Convert 4 bytes to little-endian int
        private int toLittleEndianInt(byte[] bytes) {
            return (bytes[0] & 0xFF) |
                    ((bytes[1] & 0xFF) << 8) |
                    ((bytes[2] & 0xFF) << 16) |
                    ((bytes[3] & 0xFF) << 24);
        }

        public int getDimension() {
            return dimension;
        }

        @Override
        public void close() throws IOException {
            dis.close();
        }
    }

    // Main method to demonstrate usage
    public static void main(String[] args) {
        String[] files = {
                "data/sift_dataset/sift/sift_base.fvecs",
                "data/sift_dataset/sift/sift_groundtruth.ivecs",
                "data/sift_dataset/sift/sift_learn.fvecs",
                "data/sift_dataset/sift/sift_query.fvecs"
        };

        // Process each file
        for (String file : files) {
            System.out.println("\nReading file: " + file);
            try {
                if (file.endsWith(".fvecs")) {
                    processFvecsFile(file);
                } else if (file.endsWith(".ivecs")) {
                    processIvecsFile(file);
                } else {
                    System.out.println("Unsupported file type: " + file);
                }
            } catch (IOException e) {
                System.err.println("Error reading " + file + ": " + e.getMessage());
            }
        }
    }

    // Process .fvecs file one vector at a time
    private static void processFvecsFile(String filename) throws IOException {
        try (FvecsReader reader = new FvecsReader(filename)) {
            int vectorCount = 0;
            float[] vector;
            while ((vector = reader.readNextVector()) != null) {
                vectorCount++;
                if (vectorCount == 1) {
                    System.out.println("Dimension: " + reader.getDimension());
                    System.out.print("First vector: [");
                    for (int i = 0; i < Math.min(5, vector.length); i++) {
                        System.out.print(vector[i] + (i < 4 ? ", " : ""));
                    }
                    System.out.println("...]");
                }
            }
            System.out.println("Total vectors: " + vectorCount);
        }
    }

    // Process .ivecs file one vector at a time
    private static void processIvecsFile(String filename) throws IOException {
        try (IvecsReader reader = new IvecsReader(filename)) {
            int vectorCount = 0;
            int[] vector;
            while ((vector = reader.readNextVector()) != null) {
                vectorCount++;
                if (vectorCount == 1) {
                    System.out.println("Dimension: " + reader.getDimension());
                    System.out.print("First vector: [");
                    for (int i = 0; i < Math.min(5, vector.length); i++) {
                        System.out.print(vector[i] + (i < 4 ? ", " : ""));
                    }
                    System.out.println("...]");
                }
            }
            System.out.println("Total vectors: " + vectorCount);
        }
    }
}