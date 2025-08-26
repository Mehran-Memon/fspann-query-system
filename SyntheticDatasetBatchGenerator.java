import java.io.*;
import java.nio.file.*;
import java.util.*;

public class SyntheticDatasetBatchGenerator {

    public static void main(String[] args) throws IOException {
        int[] dimensions = {128, 256, 512, 1024};
        int numBase = 1_000_000;
        int numQuery = 10_000;
        int topK = 10;
        String baseOutputDir = "E:\\Research Work\\Datasets\\synthetic_data";

        Random rnd = new Random(12345L);

        for (int dim : dimensions) {
            String folderName = "synthetic_" + dim;
            Path outDir = Paths.get(baseOutputDir, folderName);
            Files.createDirectories(outDir);

            System.out.printf("Generating synthetic dataset: %s (dim=%d)%n", folderName, dim);

            // --- Base vectors ---
            Path baseFile = outDir.resolve("base.fvecs");
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(baseFile)))) {
                for (int i = 0; i < numBase; i++) {
                    dos.writeInt(Integer.reverseBytes(dim));
                    for (int j = 0; j < dim; j++) {
                        dos.writeFloat((float) rnd.nextGaussian());
                    }
                }
            }
            System.out.println("Base vectors saved to: " + baseFile);

            // --- Query vectors ---
            Path queryFile = outDir.resolve("query.fvecs");
            float[][] queryVectors = new float[numQuery][dim]; // queries kept in memory
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(queryFile)))) {
                for (int i = 0; i < numQuery; i++) {
                    dos.writeInt(Integer.reverseBytes(dim));
                    for (int j = 0; j < dim; j++) {
                        queryVectors[i][j] = (float) rnd.nextGaussian();
                        dos.writeFloat(queryVectors[i][j]);
                    }
                }
            }
            System.out.println("Query vectors saved to: " + queryFile);

            // --- Groundtruth (exact top-K, streaming base vectors) ---
            Path gtFile = outDir.resolve("groundtruth.ivecs");
            try (DataOutputStream dosGT = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(gtFile)));
                 DataInputStream disBase = new DataInputStream(new BufferedInputStream(Files.newInputStream(baseFile)))) {

                // Read base vectors one by one, compute distances for all queries
                List<PriorityQueue<int[]>> topKQueues = new ArrayList<>();
                for (int q = 0; q < numQuery; q++) {
                    // max-heap for top-K (store {index, distance})
                    topKQueues.add(new PriorityQueue<>(Comparator.comparingDouble(a -> -a[1])));
                }

                for (int i = 0; i < numBase; i++) {
                    int dimBase = Integer.reverseBytes(disBase.readInt());
                    if (dimBase != dim) throw new IllegalStateException("Dimension mismatch!");

                    float[] baseVec = new float[dim];
                    for (int d = 0; d < dim; d++) baseVec[d] = disBase.readFloat();

                    for (int q = 0; q < numQuery; q++) {
                        float[] query = queryVectors[q];
                        double dist = 0.0;
                        for (int d = 0; d < dim; d++) {
                            double diff = query[d] - baseVec[d];
                            dist += diff * diff;
                        }
                        PriorityQueue<int[]> pq = topKQueues.get(q);
                        if (pq.size() < topK) {
                            pq.offer(new int[]{i, (int) dist});
                        } else if (dist < pq.peek()[1]) {
                            pq.poll();
                            pq.offer(new int[]{i, (int) dist});
                        }
                    }

                    if ((i+1) % 100_000 == 0) System.out.printf("Processed %d/%d base vectors%n", i+1, numBase);
                }

                // Write .ivecs file
                for (PriorityQueue<int[]> pq : topKQueues) {
                    dosGT.writeInt(Integer.reverseBytes(topK));
                    int[] indices = new int[topK];
                    for (int k = topK - 1; k >= 0; k--) {
                        indices[k] = pq.poll()[0];
                    }
                    for (int idx : indices) dosGT.writeInt(Integer.reverseBytes(idx));
                }
            }
            System.out.println("Groundtruth saved to: " + gtFile);
            System.out.println("Dataset " + folderName + " generation complete!\n");
        }

        System.out.println("All synthetic datasets generated successfully!");
    }
}
