import java.io.*;
import java.nio.file.*;
import java.util.*;

public class SyntheticDatasetBatchGenerator {

    public static void main(String[] args) throws IOException {
        // Settings
        int[] dimensions = {128, 256, 512, 1024};
        int numBase = 1_000_000;
        int numQuery = 10_000;
        int topK = 10;
        String baseOutputDir = "E:\\Research Work\\Datasets\\synthetic_data";

        Random rnd = new Random(12345L); // fixed seed for reproducibility

        for (int dim : dimensions) {
            String folderName = "synthetic_" + dim;
            Path outDir = Paths.get(baseOutputDir, folderName);
            Files.createDirectories(outDir);

            System.out.printf("Generating synthetic dataset: %s (dim=%d)%n", folderName, dim);

            // Generate base vectors
            float[][] baseVectors = new float[numBase][dim];
            for (int i = 0; i < numBase; i++) {
                for (int j = 0; j < dim; j++) {
                    baseVectors[i][j] = (float) rnd.nextGaussian();
                }
            }
            Path baseFile = outDir.resolve("base.fvecs");
            writeFvecs(baseFile, baseVectors);
            System.out.println("Base vectors saved to: " + baseFile);

            // Generate query vectors
            float[][] queryVectors = new float[numQuery][dim];
            for (int i = 0; i < numQuery; i++) {
                for (int j = 0; j < dim; j++) {
                    queryVectors[i][j] = (float) rnd.nextGaussian();
                }
            }
            Path queryFile = outDir.resolve("query.fvecs");
            writeFvecs(queryFile, queryVectors);
            System.out.println("Query vectors saved to: " + queryFile);

            // Compute groundtruth top-K for each query
            Path gtFile = outDir.resolve("groundtruth.ivecs");
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(gtFile)))) {
                for (float[] query : queryVectors) {
                    int[] indices = topKNearest(query, baseVectors, topK);
                    dos.writeInt(Integer.reverseBytes(topK)); // .ivecs uses little-endian
                    for (int idx : indices) {
                        dos.writeInt(Integer.reverseBytes(idx));
                    }
                }
            }
            System.out.println("Groundtruth saved to: " + gtFile);
            System.out.println("Dataset " + folderName + " generation complete!\n");
        }

        System.out.println("All synthetic datasets generated successfully!");
    }

    private static void writeFvecs(Path path, float[][] vectors) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (float[] vec : vectors) {
                dos.writeInt(Integer.reverseBytes(vec.length)); // little-endian
                for (float v : vec) {
                    dos.writeFloat(v);
                }
            }
        }
    }

    private static int[] topKNearest(float[] query, float[][] base, int k) {
        PriorityQueue<int[]> pq = new PriorityQueue<>(Comparator.comparingDouble(a -> -a[1]));
        for (int i = 0; i < base.length; i++) {
            float dist = 0;
            for (int j = 0; j < query.length; j++) {
                float d = query[j] - base[i][j];
                dist += d * d;
            }
            int[] pair = new int[]{i, (int) dist}; // index and distance
            if (pq.size() < k) {
                pq.offer(pair);
            } else if (dist < pq.peek()[1]) {
                pq.poll();
                pq.offer(pair);
            }
        }
        int[] indices = new int[k];
        for (int i = k - 1; i >= 0; i--) {
            indices[i] = pq.poll()[0];
        }
        return indices;
    }
}
