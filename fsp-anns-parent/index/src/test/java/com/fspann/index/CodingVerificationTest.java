package com.fspann.index;

import com.fspann.index.paper.Coding;
import com.fspann.index.paper.GreedyPartitioner;

import java.util.BitSet;

/**
 * Verification test for the Coding bit ordering fix.
 *
 * Run this BEFORE full evaluation to confirm the fix is correct.
 */
public class CodingVerificationTest {

    public static void main(String[] args) {
        System.out.println("=== CODING BIT ORDERING VERIFICATION ===\n");

        int m = 24;
        int lambda = 2;
        int dim = 128;
        long seed = 42L;

        // Create two similar vectors
        double[] v1 = new double[dim];
        double[] v2 = new double[dim];
        double[] v3 = new double[dim];  // very different

        java.util.Random rnd = new java.util.Random(123);
        for (int i = 0; i < dim; i++) {
            v1[i] = rnd.nextGaussian();
            v2[i] = v1[i] + rnd.nextGaussian() * 0.1;  // Small perturbation
            v3[i] = rnd.nextGaussian() * 10;           // Completely different
        }

        // Generate codes
        Coding.GFunction G = Coding.buildRandomG(dim, m, lambda, 1.0, seed);

        BitSet c1 = Coding.C(v1, G);
        BitSet c2 = Coding.C(v2, G);
        BitSet c3 = Coding.C(v3, G);

        int codeBits = m * lambda;  // 48

        System.out.println("Code bits: " + codeBits);
        System.out.println();

        // Test 1: Similar vectors should have similar MSBs
        int msbMatch = countMatchingBits(c1, c2, 0, m);  // First m bits (MSBs)
        int lsbMatch = countMatchingBits(c1, c2, m, codeBits);  // Last m bits (LSBs)

        System.out.println("Test 1: Similar vectors (v1 vs v2)");
        System.out.println("  MSB match (bits 0-" + (m-1) + "): " + msbMatch + "/" + m);
        System.out.println("  LSB match (bits " + m + "-" + (codeBits-1) + "): " + lsbMatch + "/" + m);

        // For similar vectors, MSB match should be HIGH
        if (msbMatch < m/2) {
            System.out.println("  WARNING: Low MSB match for similar vectors!");
        } else {
            System.out.println("  OK: Good MSB match for similar vectors");
        }
        System.out.println();

        // Test 2: Different vectors should have low MSB match
        int msbMatchDiff = countMatchingBits(c1, c3, 0, m);
        int lsbMatchDiff = countMatchingBits(c1, c3, m, codeBits);

        System.out.println("Test 2: Different vectors (v1 vs v3)");
        System.out.println("  MSB match (bits 0-" + (m-1) + "): " + msbMatchDiff + "/" + m);
        System.out.println("  LSB match (bits " + m + "-" + (codeBits-1) + "): " + lsbMatchDiff + "/" + m);
        System.out.println();

        // Test 3: Prefix matching simulation
        System.out.println("Test 3: Prefix matching simulation");

        GreedyPartitioner.CodeComparator fullCmp = new GreedyPartitioner.CodeComparator(codeBits);
        GreedyPartitioner.CodeComparator halfCmp = new GreedyPartitioner.CodeComparator(m);  // Half bits

        int fullCmpResult = fullCmp.compare(c1, c2);
        int halfCmpResult = halfCmp.compare(c1, c2);

        System.out.println("  Full 48-bit compare (c1 vs c2): " + fullCmpResult);
        System.out.println("  Half 24-bit compare (c1 vs c2): " + halfCmpResult);

        // With MSB-first ordering, half-bit compare should still capture similarity
        System.out.println();

        // Test 4: Verify bit layout
        System.out.println("Test 4: Bit layout verification");
        int[] H = Coding.H(v1, G);

        System.out.println("  First 3 h_j values: h_0=" + H[0] + ", h_1=" + H[1] + ", h_2=" + H[2]);
        System.out.println("  Expected MSBs at positions 0-" + (m-1));

        // Check if MSB of h_0 is at position 0
        boolean h0_msb = ((H[0] >>> (lambda - 1)) & 1) == 1;
        boolean bit0 = c1.get(0);

        System.out.println("  h_0 MSB = " + (h0_msb ? 1 : 0));
        System.out.println("  Code bit 0 = " + (bit0 ? 1 : 0));

        if (h0_msb == bit0) {
            System.out.println("  OK: MSB correctly placed at position 0");
        } else {
            System.out.println("  ERROR: MSB not at expected position!");
        }

        System.out.println();
        System.out.println("=== VERIFICATION COMPLETE ===");

        // Summary
        boolean success = (msbMatch >= m/2) && (h0_msb == bit0);
        System.out.println("\nResult: " + (success ? "PASS" : "FAIL"));

        if (!success) {
            System.exit(1);
        }
    }

    private static int countMatchingBits(BitSet a, BitSet b, int start, int end) {
        int count = 0;
        for (int i = start; i < end; i++) {
            if (a.get(i) == b.get(i)) count++;
        }
        return count;
    }
}