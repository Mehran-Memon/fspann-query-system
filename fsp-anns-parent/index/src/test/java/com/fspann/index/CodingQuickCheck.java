package com.fspann.index;

import com.fspann.index.paper.Coding;

import java.util.BitSet;

public class CodingQuickCheck {
    public static void main(String[] args) {
        // Create a simple vector
        double[] v = new double[128];
        for (int i = 0; i < 128; i++) v[i] = i * 0.01;

        // Generate code with known parameters
        int m = 24, lambda = 2;
        long seed = 12345L;

        Coding.GFunction G = Coding.buildRandomG(128, m, lambda, 1.0, seed);
        int[] H = Coding.H(v, G);
        BitSet code = Coding.C(v, G);

        // Check: For MSB-first, bit 0 should be MSB of h_0
        // MSB of h_0 = (H[0] >>> 1) & 1
        int expectedBit0 = (H[0] >>> (lambda - 1)) & 1;
        int actualBit0 = code.get(0) ? 1 : 0;

        System.out.println("=== CODING VERIFICATION ===");
        System.out.println("H[0] = " + H[0] + " (binary: " + Integer.toBinaryString(H[0]) + ")");
        System.out.println("Expected bit 0 (MSB of H[0]): " + expectedBit0);
        System.out.println("Actual bit 0 from code:      " + actualBit0);

        if (expectedBit0 == actualBit0) {
            System.out.println("\n✓ PASS: MSB-first ordering is CORRECT!");
            System.out.println("  The fix is properly applied.");
        } else {
            System.out.println("\n✗ FAIL: Bit ordering is WRONG!");
            System.out.println("  The fix was NOT applied correctly.");
            System.exit(1);
        }
    }
}