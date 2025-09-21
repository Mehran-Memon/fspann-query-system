import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

/** Improved Synthetic Dataset Batch Generator for FAISS-style vectors. */
public class SyntheticDatasetBatchGenerator {

    // Max-heap element for top-K (largest dist at head)
    public static class Candidate {
        int idx;
        double dist;

        public Candidate(int idx, double dist) {
            this.idx = idx;
            this.dist = dist;
        }
    }

    // Configuration Constants
    static final int[] DIMS = {128, 256, 512, 1024};
    static final int NUM_BASE = 1_000_000;    // Number of base vectors
    static final int NUM_QUERY = 10_000;      // Number of query vectors
    static final int TOPK = 10;               // Top-K for ground truth
    static final Path ROOT = Paths.get("E:\\Research Work\\Datasets\\synthetic_data");  // Output path
    static final long SEED = 12345L;          // Random seed
    static final Format FORMAT = Format.LE_ALL; // Change to LEGACY_MIXED if needed for older formats

    public static void main(String[] args) throws Exception {
        Random rnd = new Random(SEED);

        // Iterate over dimensions
        for (int dim : DIMS) {
            String folder = "synthetic_" + dim;
            Path outDir = ROOT.resolve(folder);
            Files.createDirectories(outDir);
            System.out.printf("Generating %s (dim=%d, format=%s)%n", folder, dim, FORMAT);

            Path baseFile = outDir.resolve("base.fvecs");
            Path queryFile = outDir.resolve("query.fvecs");
            Path gtFile = outDir.resolve("groundtruth.ivecs");

            // --- BASE --- (stream-write base vectors)
            try (FvecsWriter w = new FvecsWriter(baseFile, dim, FORMAT)) {
                float[] vec = new float[dim];
                for (int i = 0; i < NUM_BASE; i++) {
                    gaussian(rnd, vec);  // Generate random vector
                    sanityCheck(vec, "base[" + i + "]");  // Ensure no NaN/Inf
                    w.write(vec);  // Write to file
                    if ((i + 1) % 100_000 == 0) {
                        System.out.printf("  wrote %d/%d base%n", i + 1, NUM_BASE);
                    }
                }
            }
            System.out.println("Base vectors saved to: " + baseFile);

            // --- QUERIES --- (in RAM + write queries to file)
            float[][] Q = new float[NUM_QUERY][dim];
            try (FvecsWriter w = new FvecsWriter(queryFile, dim, FORMAT)) {
                for (int i = 0; i < NUM_QUERY; i++) {
                    gaussian(rnd, Q[i]);  // Generate query vector
                    sanityCheck(Q[i], "query[" + i + "]");  // Ensure no NaN/Inf
                    w.write(Q[i]);  // Write to file
                }
            }
            System.out.println("Query vectors saved to: " + queryFile);

            // --- Sanity Check (first few vectors) ---
            try (FvecsReader r = new FvecsReader(baseFile, FORMAT)) {
                for (int i = 0; i < Math.min(3, NUM_BASE); i++) {
                    float[] v = r.read();
                    if (v == null || v.length != dim) {
                        throw new IllegalStateException("Spot-check failed: could not read base[" + i + "]");
                    }
                    sanityCheck(v, "spotCheck.base[" + i + "]");
                }
            }

            // --- GROUND TRUTH --- (calculate ground truth for each query)
            @SuppressWarnings("unchecked")
            PriorityQueue<Candidate>[] heaps = new PriorityQueue[NUM_QUERY];
            for (int q = 0; q < NUM_QUERY; q++) {
                heaps[q] = new PriorityQueue<>(Comparator.comparingDouble((Candidate c) -> -c.dist));
            }

            try (FvecsReader r = new FvecsReader(baseFile, FORMAT)) {
                int idx = 0;
                for (float[] bv; (bv = r.read()) != null; idx++) {
                    sanityCheck(bv, "base-read[" + idx + "]");
                    for (int q = 0; q < NUM_QUERY; q++) {
                        double dist = l2(Q[q], bv);  // Calculate L2 distance
                        PriorityQueue<Candidate> pq = heaps[q];
                        if (pq.size() < TOPK) {
                            pq.offer(new Candidate(idx, dist));  // Add to heap
                        } else if (dist < pq.peek().dist) {
                            pq.poll();
                            pq.offer(new Candidate(idx, dist));  // Replace if smaller
                        }
                    }
                    if ((idx + 1) % 100_000 == 0) {
                        System.out.printf("  GT processed %d/%d base%n", idx + 1, NUM_BASE);
                    }
                }
                if (idx != NUM_BASE) {
                    System.out.printf("WARN: read %d base vectors for GT (expected %d)%n", idx, NUM_BASE);
                }
            }

            // --- WRITE GROUND TRUTH --- (store indices in .ivecs format)
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(gtFile)))) {
                for (int q = 0; q < NUM_QUERY; q++) {
                    PriorityQueue<Candidate> pq = heaps[q];
                    int k = Math.min(TOPK, pq.size());
                    out.writeInt(Integer.reverseBytes(k));  // Write number of results
                    Candidate[] arr = pq.toArray(new Candidate[0]);
                    Arrays.sort(arr, Comparator.comparingDouble(c -> c.dist));  // Sort by distance
                    for (int i = 0; i < k; i++) {
                        out.writeInt(Integer.reverseBytes(arr[i].idx));  // Write indices in little-endian
                    }
                }
            }
            System.out.println("Groundtruth saved to: " + gtFile);
            System.out.println("DONE " + folder + "\n");
        }

        System.out.println("All synthetic datasets generated successfully!");
    }

    // --------- Data Generation & Helpers ---------

    // Gaussian vector generation (N(0,1) distribution)
    static void gaussian(Random rnd, float[] vec) {
        for (int i = 0; i < vec.length; i++) {
            float v = (float) rnd.nextGaussian();
            if (v != 0f && Math.abs(v) < 1e-38f) v = 0f;  // Avoid denormalized numbers
            vec[i] = v;
        }
    }

    // L2 distance between two vectors
    static double l2(float[] a, float[] b) {
        double s = 0.0;
        for (int i = 0; i < a.length; i++) {
            double d = (double) a[i] - (double) b[i];
            s += d * d;
        }
        return s;
    }

    // Sanity check: ensure no NaN/Inf in the vector
    static void sanityCheck(float[] v, String tag) {
        for (float x : v) {
            if (!Float.isFinite(x)) {
                throw new IllegalStateException("NaN/Inf in " + tag);
            }
        }
    }

    // Sanity check for 2D array of vectors
    static void sanityCheck(float[][] vv, String tag) {
        for (int i = 0; i < vv.length; i++) sanityCheck(vv[i], tag + "[" + i + "]");
    }

    // --------- fvecs Writers/Readers ---------

    /** Writer for fvecs (supports multiple formats: LE_ALL or LEGACY_MIXED). */
    static final class FvecsWriter implements Closeable {
        private final FileChannel ch;
        private final ByteBuffer  leHeader;
        private final int dim;
        private final Format fmt;

        // scratch buffers for payload in LE/BYTE_ENDIAN formats
        private final ByteBuffer payloadLE;
        private final ByteBuffer payloadBE;

        FvecsWriter(Path path, int dim, Format fmt) throws IOException {
            this.ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            this.leHeader = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
            this.dim = dim;
            this.fmt = fmt;
            this.payloadLE = ByteBuffer.allocateDirect(dim * 4).order(ByteOrder.LITTLE_ENDIAN);
            this.payloadBE = ByteBuffer.allocateDirect(dim * 4).order(ByteOrder.BIG_ENDIAN);
        }

        void write(float[] vec) throws IOException {
            if (vec.length != dim) throw new IllegalArgumentException("dim mismatch");
            // header: little-endian int32 dim
            leHeader.clear();
            leHeader.putInt(dim).flip();
            ch.write(leHeader);

            // payload: according to format
            ByteBuffer payload = (fmt == Format.LE_ALL) ? payloadLE : payloadBE;
            payload.clear();
            for (float f : vec) payload.putFloat(f);
            payload.flip();
            ch.write(payload);
        }

        public void close() throws IOException { ch.close(); }
    }

    /** Reader for fvecs with selectable endianness policy. */
    static final class FvecsReader implements Closeable {
        private final DataInputStream in;
        private final Format fmt;

        FvecsReader(Path path, Format fmt) throws IOException {
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
            this.fmt = fmt;
        }

        float[] read() throws IOException {
            try {
                int dimLE = Integer.reverseBytes(in.readInt());
                if (dimLE <= 0 || dimLE > 1_000_000) {
                    throw new IOException("Invalid dimension: " + dimLE);
                }

                byte[] buf = in.readNBytes(dimLE * 4);
                if (buf.length != dimLE * 4) return null;

                ByteOrder order = (fmt == Format.LE_ALL) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
                ByteBuffer bb = ByteBuffer.wrap(buf).order(order);
                float[] v = new float[dimLE];
                for (int i = 0; i < dimLE; i++) v[i] = bb.getFloat();

                sanityCheck(v, "reader");
                return v;
            } catch (EOFException eof) {
                return null;
            }
        }

        public void close() throws IOException { in.close(); }
    }

    /** Supported formats for writing and reading data. */
    enum Format {
        LE_ALL,      // Little-endian for both dim and payload (FAISS standard)
        LEGACY_MIXED // Mixed format with little-endian dim and big-endian payload
    }
}
