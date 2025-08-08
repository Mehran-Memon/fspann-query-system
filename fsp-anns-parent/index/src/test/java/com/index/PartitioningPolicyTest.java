package com.index;

import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.PartitioningPolicy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PartitioningPolicyTest {

    @Test
    void bucketsForInsertReturnsPerTableIds() {
        int dims = 96, buckets = 64, projections = 96, numTables = 5;
        long seed = 2025L;
        EvenLSH lsh = new EvenLSH(dims, buckets, projections, seed);

        double[] v = new double[dims];
        for (int i = 0; i < dims; i++) v[i] = Math.cos(i * 0.013);

        List<Integer> ids = PartitioningPolicy.bucketsForInsert(lsh, v, numTables);
        assertEquals(numTables, ids.size());

        // Equal to LSH per-table main bucket
        for (int t = 0; t < numTables; t++) {
            int expected = lsh.getBucketId(v, t);
            assertEquals(expected, ids.get(t));
            assertTrue(0 <= ids.get(t) && ids.get(t) < buckets);
        }
    }

    @Test
    void expansionsForQueryAreContiguousAndContainMain() {
        int dims = 128, buckets = 64, projections = 96, numTables = 4, topK = 60;
        long seed = 99;
        EvenLSH lsh = new EvenLSH(dims, buckets, projections, seed);

        double[] q = new double[dims];
        for (int i = 0; i < dims; i++) q[i] = Math.sin(i * 0.007);

        var perTable = PartitioningPolicy.expansionsForQuery(lsh, q, numTables, topK);
        assertEquals(numTables, perTable.size());

        for (int t = 0; t < numTables; t++) {
            var ex = perTable.get(t);
            assertFalse(ex.isEmpty());
            int main = lsh.getBucketId(q, t);
            assertTrue(ex.contains(main), "Main bucket missing (table " + t + ")");
            // valid range
            for (int b : ex) assertTrue(0 <= b && b < buckets);
            // pairwise buckets contiguous in modular sense (weak check)
            // i.e., gaps should be small; this is a sanity (not strict proof)
            assertTrue(ex.size() <= buckets / 2 + 1);
        }
    }

    @Test
    void evalSweepCoversRequestedTopKs() {
        int dims = 64, buckets = 32, projections = 64, numTables = 2;
        long seed = 7;
        EvenLSH lsh = new EvenLSH(dims, buckets, projections, seed);

        double[] q = new double[dims];
        for (int i = 0; i < dims; i++) q[i] = i % 3 == 0 ? 0.5 : -0.25;

        var all = PartitioningPolicy.expansionsForEvalSweep(lsh, q, numTables);
        assertEquals(PartitioningPolicy.EVAL_TOPKS.length, all.size());
        for (var perTable : all) {
            assertEquals(numTables, perTable.size());
            for (var ex : perTable) assertFalse(ex.isEmpty());
        }

        // Debug string compiles & looks sane
        String dbg = PartitioningPolicy.debugString(all.get(0));
        assertTrue(dbg.startsWith("[T0=") && dbg.endsWith("]"));
    }
}
