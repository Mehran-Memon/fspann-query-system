package com.index;

import com.fspann.index.core.EvenLSH;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.*;

class EvenLSHDeterminismTest {

    @Test
    void sameSeedSameBucketsAcrossInstances() {
        int dims = 128;
        int buckets = 64;
        int projections = 96;
        long seed = 0x1234_5678_ABCDL;

        EvenLSH lshA = new EvenLSH(dims, buckets, projections, seed);
        EvenLSH lshB = new EvenLSH(dims, buckets, projections, seed);

        assertEquals(buckets, lshA.getNumBuckets());
        assertEquals(buckets, lshB.getNumBuckets());

        // generate a few random vectors, check per-table bucket equality
        SplittableRandom rnd = new SplittableRandom(42);
        int numTables = 4;
        for (int v = 0; v < 25; v++) {
            double[] x = randUnitVector(rnd, dims);
            for (int t = 0; t < numTables; t++) {
                int a = lshA.getBucketId(x, t);
                int b = lshB.getBucketId(x, t);
                assertEquals(a, b, "Bucket mismatch at vec=" + v + " table=" + t);
                assertTrue(a >= 0 && a < buckets);
            }
        }
    }

    @Test
    void expansionsMatchAcrossInstances() {
        int dims = 64, buckets = 32, projections = 64;
        long seed = 777;
        int numTables = 3;
        int topK = 40;

        EvenLSH l1 = new EvenLSH(dims, buckets, projections, seed);
        EvenLSH l2 = new EvenLSH(dims, buckets, projections, seed);

        double[] q = new double[dims];
        for (int i = 0; i < dims; i++) q[i] = Math.sin(i * 0.01);

        for (int t = 0; t < numTables; t++) {
            var a = l1.getBuckets(q, topK, t);
            var b = l2.getBuckets(q, topK, t);
            assertEquals(a, b, "Expansion differs for table " + t);
            for (int id : a) assertTrue(0 <= id && id < buckets);
        }
    }

    private static double[] randUnitVector(SplittableRandom r, int n) {
        double[] x = new double[n];
        double norm = 0.0;
        for (int i = 0; i < n; i++) {
            x[i] = r.nextDouble(-1.0, 1.0);
            norm += x[i] * x[i];
        }
        norm = Math.sqrt(Math.max(norm, 1e-12));
        for (int i = 0; i < n; i++) x[i] /= norm;
        return x;
    }
}
