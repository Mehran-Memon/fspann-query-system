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
        // deterministic seed for stable tests
        lsh = new EvenLSH(3, 100, 32, 1234L);
    }

    @Test
    void testGetBucketsWithinRangePerTable() {
        double[] point = {1.0, 2.0, 3.0};
        int topK = 2; // range = ceil(log2(3)) = 2 -> size = 5
        List<Integer> buckets = lsh.getBuckets(point, topK, 0);
        assertNotNull(buckets);
        assertEquals(5, buckets.size());
        // must be within [0, numBuckets)
        buckets.forEach(b -> assertTrue(b >= 0 && b < lsh.getNumBuckets()));
    }

    @Test
    void testProjectAndBucketIdRange() {
        double[] vector = {1.0, 2.0, 3.0};
        double projection = lsh.project(vector, 0);
        int bucketId = lsh.getBucketId(vector, 0);
        assertFalse(Double.isNaN(projection));
        assertTrue(bucketId >= 0 && bucketId < lsh.getNumBuckets());
    }

    @Test
    void testVectorDimensionMismatchThrows() {
        double[] badVec = {1.0, 2.0}; // lsh expects 3D
        assertThrows(IllegalArgumentException.class, () -> lsh.getBucketId(badVec, 0));
        assertThrows(IllegalArgumentException.class, () -> lsh.project(badVec, 0));
    }

    @Test
    void testDeterministicBucketAssignment() {
        double[] vec = {1.2, 3.4, 5.6};
        int first = lsh.getBucketId(vec, 0);
        int second = lsh.getBucketId(vec, 0);
        assertEquals(first, second);

        // same params + seed -> identical mapping across instances
        EvenLSH lsh2 = new EvenLSH(3, 100, 32, 1234L);
        assertEquals(first, lsh2.getBucketId(vec, 0));
    }
}
