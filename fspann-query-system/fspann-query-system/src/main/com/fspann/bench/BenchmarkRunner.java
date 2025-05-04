package com.fspann.bench;

import com.fspann.ForwardSecureANNSystem;
import com.fspann.data.DataLoader;
import com.fspann.config.SystemConfig;
import com.fspann.utils.Profiler;

import java.util.List;
import java.util.Random;

public class BenchmarkRunner {
    private static final int BASE_N = 100_000;  // slice of SIFT base
    private static final int RUN_Q  = 1_000;    // queries
    private static final int RUN_I  = 1_000;    // inserts

    public static void main(String[] args) throws Exception {

        String base   = "data/sift_dataset/sift/sift_base.fvecs";
        String query  = "data/sift_dataset/sift/sift_query.fvecs";
        String gt     = "data/sift_dataset/sift/sift_groundtruth.ivecs";

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                base, query, gt, 3, 10, 1_000, 1_500, true, true);

        Profiler prof = new Profiler();
        Random   rnd  = new Random(42);

        System.out.println("\n--- Benchmark inserts ---");
        List<double[]> baseVec = sys.getBaseVectors().subList(0, BASE_N);
        for (int i = 0; i < RUN_I; i++) {
            double[] v = baseVec.get(rnd.nextInt(BASE_N));
            sys.insert("b_" + i, v);
        }

        System.out.println("\n--- Benchmark queries ---");
        List<double[]> qVec = sys.getQueryVectors();
        for (int i = 0; i < RUN_Q; i++) {
            sys.query(qVec.get(i % qVec.size()), 10);
        }

        /* force one shard rotation */
        sys.getANN().getKeyManager().rotateShard(0);

        // wait a bit so background ReEncryptor can run
        Thread.sleep(3_000);

        System.out.println("\n=== Profiler summary ===");
        prof.log("insert");
        prof.log("query");
        prof.log("reenc-batch");

        sys.shutdown();
    }
}
