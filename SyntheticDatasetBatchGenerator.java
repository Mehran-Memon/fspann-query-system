import java.io.*;
import java.nio.file.*;
import java.util.*;

public class SyntheticDatasetBatchGenerator {

    // Record to store candidate base vector index and squared distance
    private record Candidate(int idx, double dist) {}

    public static void main(String[] args) throws IOException {
        int[] dimensions = {128, 256, 512, 1024};
        int numBase = 1_000_000;   // number of base vectors
        int numQuery = 10_000;     // number of query vectors
        int topK = 100;             // groundtruth size
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
            float[][] queryVectors = new float[numQuery][dim];
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

            // --- Groundtruth (exact top-K) ---
            Path gtFile = outDir.resolve("groundtruth.ivecs");
            try (DataOutputStream dosGT = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(gtFile)));
                 DataInputStream disBase = new DataInputStream(new BufferedInputStream(Files.newInputStream(baseFile)))) {

                List<PriorityQueue<Candidate>> topKQueues = new ArrayList<>();
                for (int q = 0; q < numQuery; q++) {
                    // Max-heap: largest distance at top
                    topKQueues.add(new PriorityQueue<>(Comparator.comparingDouble(c -> -c.dist)));
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
                        PriorityQueue<Candidate> pq = topKQueues.get(q);
                        if (pq.size() < topK) {
                            pq.offer(new Candidate(i, dist));
                        } else if (dist < pq.peek().dist) {
                            pq.poll();
                            pq.offer(new Candidate(i, dist));
                        }
                    }

                    if ((i + 1) % 100_000 == 0) {
                        System.out.printf("Processed %d/%d base vectors%n", i + 1, numBase);
                    }
                }

                // Write .ivecs rows
                for (PriorityQueue<Candidate> pq : topKQueues) {
                    dosGT.writeInt(Integer.reverseBytes(topK));
                    Candidate[] arr = pq.toArray(new Candidate[0]);
                    Arrays.sort(arr, Comparator.comparingDouble(c -> c.dist)); // ascending distance
                    for (Candidate c : arr) {
                        dosGT.writeInt(Integer.reverseBytes(c.idx));
                    }
                }
            }
            System.out.println("Groundtruth saved to: " + gtFile);
            System.out.println("Dataset " + folderName + " generation complete!\n");
        }

        System.out.println("All synthetic datasets generated successfully!");
    }
}
