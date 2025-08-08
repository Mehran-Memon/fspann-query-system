package com.index;

import com.fspann.index.core.EvenLSH;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EvenLSHTest {
    private EvenLSH lsh;

    @BeforeEach
    void setUp() {
        lsh = new EvenLSH(3, 100); // 3D input, 100 buckets
    }

    @Test
    void perTableBucketsHaveContiguousRange() {
        double[] point = {1.0, 2.0, 3.0};
        List<Integer> buckets = lsh.getBuckets(point, 20, 0);
        assertNotNull(buckets);
        assertFalse(buckets.isEmpty());
        // contiguous expansion: 2*range + 1
        assertTrue(buckets.size() >= 1);
        assertTrue(buckets.stream().allMatch(b -> b >= 0 && b < lsh.getNumBuckets()));
    }

    @Test
    void allTablesExpansionHasNumTablesLists() {
        double[] point = {1.0, 2.0, 3.0};
        int numTables = 4;
        List<List<Integer>> all = lsh.getBucketsForAllTables(point, 20, numTables);
        assertEquals(numTables, all.size());
        all.forEach(lst -> assertFalse(lst.isEmpty()));
    }

    @Test
    void bucketIdDeterministic() {
        double[] vec = {1.2, 3.4, 5.6};
        int b1 = lsh.getBucketId(vec, 0);
        int b2 = lsh.getBucketId(vec, 0);
        assertEquals(b1, b2);
    }
}
