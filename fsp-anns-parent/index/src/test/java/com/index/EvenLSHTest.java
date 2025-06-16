package com.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fspann.index.core.EvenLSH;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EvenLSHTest {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSHTest.class);
    private EvenLSH lsh;

    @BeforeEach
    void setUp() {
        lsh = new EvenLSH(3, 100); // 3 tables, 100 buckets
    }

    @Test
    void testGetBuckets() {
        double[] point = {1.0, 2.0, 3.0};
        List<Integer> buckets = lsh.getBuckets(point, 1);
        assertNotNull(buckets);
        assertTrue(buckets.size() >= 1 && buckets.size() <= 3);
        logger.debug("Buckets for point {}: {}", point, buckets);
    }

    @Test
    void testGetBucketsDefaultRange() {
        double[] point = {1.0, 2.0, 3.0};
        List<Integer> buckets = lsh.getBuckets(point);
        assertNotNull(buckets);
        logger.debug("Default range buckets for point {}: {}", point, buckets);
    }

    @Test
    void testProjectAndGetBucketId() {
        EvenLSH lsh = new EvenLSH(2, 10);
        double[] vector = {1.0, 2.0};
        double projection = lsh.project(vector);
        int bucketId = lsh.getBucketId(vector);
        System.out.println("Projection: " + projection + ", BucketId: " + bucketId);
        assertTrue(projection != 0.0);
        assertTrue(bucketId >= 0 && bucketId < 10);
    }
}