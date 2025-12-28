package com.fspann.index;

import com.fspann.index.paper.Coding;
import com.fspann.index.paper.GreedyPartitioner;

import java.util.*;

/**
 * Verification test for MSANNP integer hash stability and prefix behavior.
 *
 * This test is aligned with:
 *  - Coding.H(...) → int[]
 *  - GreedyPartitioner → integer interval based
 *
 * Run BEFORE large-scale evaluation.
 */
public final class CodingVerificationTest {

    public static void main(String[] args) {

        System.out.println("=== MSANNP CODING VERIFICATION (INTEGER HASH) ===\n");

        int m = 24;
        int lambda = 2;
        int dim = 128;
        long seed = 42L;

        // ------------------------------------------------------------
        // 1. Create vectors
        // ------------------------------------------------------------
        double[] v1 = new double[dim];
        double[] v2 = new double[dim];
        double[] v3 = new double[dim];

        Random rnd = new Random(123);
        for (int i = 0; i < dim; i++) {
            v1[i] = rnd.nextGaussian();
            v2[i] = v1[i] + rnd.nextGaussian() * 0.1;   // similar
            v3[i] = rnd.nextGaussian() * 10.0;          // very different
        }

        // ------------------------------------------------------------
        // 2. Build GFunction
        // ------------------------------------------------------------
        Coding.GFunction G =
                Coding.buildRandomG(dim, m, lambda, 1.0, seed);

        int[] h1 = Coding.H(v1, G);
        int[] h2 = Coding.H(v2, G);
        int[] h3 = Coding.H(v3, G);

        System.out.println("Hash length (m): " + h1.length);
        System.out.println();

        // ------------------------------------------------------------
        // 3. Similar vectors → similar hashes
        // ------------------------------------------------------------
        int match12 = countEqual(h1, h2);
        int match13 = countEqual(h1, h3);

        System.out.println("Test 1: Hash similarity");
        System.out.println("  v1 vs v2 equal hashes: " + match12 + "/" + m);
        System.out.println("  v1 vs v3 equal hashes: " + match13 + "/" + m);

        if (match12 <= match13) {
            System.err.println("FAIL: Similar vectors not more similar than random!");
            System.exit(1);
        } else {
            System.out.println("  OK: Similarity preserved");
        }

        System.out.println();

        // ------------------------------------------------------------
        // 4. Prefix stability test (first k projections)
        // ------------------------------------------------------------
        int k = m / 2;
        int prefixMatch = countEqual(h1, h2, 0, k);

        System.out.println("Test 2: Prefix stability (first " + k + ")");
        System.out.println("  Prefix matches: " + prefixMatch + "/" + k);

        if (prefixMatch < k / 2) {
            System.err.println("FAIL: Prefix similarity too weak");
            System.exit(1);
        } else {
            System.out.println("  OK: Prefix similarity strong");
        }

        System.out.println();

        // ------------------------------------------------------------
        // 5. GreedyPartitioner sanity test
        // ------------------------------------------------------------
        System.out.println("Test 3: GreedyPartitioner correctness");

        List<GreedyPartitioner.Item> items = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            items.add(new GreedyPartitioner.Item("v1_" + i, h1[i]));
            items.add(new GreedyPartitioner.Item("v2_" + i, h2[i]));
        }

        GreedyPartitioner.BuildResult br =
                GreedyPartitioner.build(items, seed);

        System.out.println("  Intervals: " + br.indexI.size());
        System.out.println("  Max bucket size (w): " + br.w);

        if (br.indexI.isEmpty()) {
            System.err.println("FAIL: No partitions created");
            System.exit(1);
        }

        System.out.println("  OK: Partitioning produced valid intervals");

        System.out.println("\n=== VERIFICATION PASSED ===");
    }

    // ------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------
    private static int countEqual(int[] a, int[] b) {
        return countEqual(a, b, 0, a.length);
    }

    private static int countEqual(int[] a, int[] b, int start, int end) {
        int c = 0;
        for (int i = start; i < end; i++) {
            if (a[i] == b[i]) c++;
        }
        return c;
    }
}
