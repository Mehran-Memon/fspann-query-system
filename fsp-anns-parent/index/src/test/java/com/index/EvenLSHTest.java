package com.index;

import com.fspann.index.core.EvenLSH;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EvenLSHTest {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSHTest.class);
    private EvenLSH lsh;

    @BeforeEach
    void setUp() {
        lsh = new EvenLSH(3, 100); // 3D input space, 100 buckets
    }

    @Test
    void testGetBucketsWithinRange() {
        double[] point = {1.0, 2.0, 3.0};
        List<Integer> buckets = lsh.getBuckets(point, 2);
        assertNotNull(buckets);
        assertFalse(buckets.isEmpty());
        assertTrue(buckets.size() >= 1 && buckets.size() <= 5);
        logger.debug("Expanded buckets for point {}: {}", point, buckets);
    }

    @Test
    void testGetBucketsDefaultRange() {
        double[] point = {1.0, 2.0, 3.0};
        List<Integer> buckets = lsh.getBuckets(point);
        assertNotNull(buckets);
        assertTrue(buckets.size() >= 1);
        logger.debug("Default range buckets for point {}: {}", point, buckets);
    }

    @Test
    void testProjectAndBucketIdRange() {
        double[] vector = {1.0, 2.0, 3.0};
        double projection = lsh.project(vector);
        int bucketId = lsh.getBucketId(vector);
        logger.debug("Projection: {}, BucketId: {}", projection, bucketId);
        assertTrue(bucketId >= 0 && bucketId < lsh.getNumBuckets());
    }

    @Test
    void testVectorDimensionMismatchThrows() {
        double[] badVec = {1.0, 2.0}; // lsh expects 3D vectors
        assertThrows(IllegalArgumentException.class, () -> lsh.getBucketId(badVec));
        assertThrows(IllegalArgumentException.class, () -> lsh.project(badVec));
    }

    @Test
    void testConsistencyOfProjection() {
        double[] vec = {0.5, 1.5, -2.0};
        double first = lsh.project(vec);
        double second = lsh.project(vec);
        assertEquals(first, second, 1e-9);
    }

    @Test
    void testDeterministicBucketAssignment() {
        double[] vec = {1.2, 3.4, 5.6};
        int first = lsh.getBucketId(vec);
        int second = lsh.getBucketId(vec);
        assertEquals(first, second, "Bucket ID should be deterministic");
    }
}
