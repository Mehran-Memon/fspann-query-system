package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.PartitioningPolicy;
import com.fspann.index.core.SecureLSHIndex;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Quick integration: insert synthetic points, ensure candidate fanout
 * is non-decreasing with larger K and bounded by N.
 */
class SecureLSHIndexFanoutTest {

    private static EncryptedPoint ep(String id, int dim, List<Integer> buckets) {
        // shardId legacy: use first bucket; IV/ciphertext dummies for index-only test
        return new EncryptedPoint(
                id,
                buckets.get(0),
                new byte[12],
                new byte[16],
                1,          // version
                dim,
                buckets
        );
    }

    @Test
    void fanoutMonotonicWithK() {
        int dims = 64;
        int buckets = 64;
        int projections = 96;
        int numTables = 4;
        long seed = 424242L;
        int N = 1000;

        EvenLSH lsh = new EvenLSH(dims, buckets, projections, seed);
        SecureLSHIndex index = new SecureLSHIndex(numTables, buckets, lsh);

        Random r = new Random(123);
        for (int i = 0; i < N; i++) {
            double[] v = randVec(r, dims);
            List<Integer> perTable = PartitioningPolicy.bucketsForInsert(lsh, v, numTables);
            index.addPoint(ep("p" + i, dims, perTable));
        }
        assertEquals(N, index.getPointCount());

        double[] q = randVec(new Random(999), dims);
        int[] ks = {1, 20, 40, 60, 80, 100};

        int prev = -1;
        for (int k : ks) {
            var perTable = PartitioningPolicy.expansionsForQuery(lsh, q, numTables, k);
            int cand = index.candidateCount(perTable);
            assertTrue(cand >= 0 && cand <= N, "cand out of bounds");
            if (prev >= 0) assertTrue(cand >= prev, "non-monotonic fanout for K=" + k);
            prev = cand;
        }
    }

    private static double[] randVec(Random r, int n) {
        double[] x = new double[n];
        for (int i = 0; i < n; i++) x[i] = r.nextGaussian();
        return x;
    }
}
