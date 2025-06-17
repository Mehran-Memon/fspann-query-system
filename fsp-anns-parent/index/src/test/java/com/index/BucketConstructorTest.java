package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.index.core.BucketConstructor;
import org.junit.jupiter.api.Test;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BucketConstructorTest {

    @Test
    void testApplyFakePadsBucketsCorrectly() {
        EncryptedPoint fakeTemplate = new EncryptedPoint(
                "real",
                0,
                new byte[12],
                new byte[32],
                1,
                128
        );

        List<List<EncryptedPoint>> buckets = new ArrayList<>();
        List<EncryptedPoint> bucket = new ArrayList<>();
        bucket.add(fakeTemplate);
        buckets.add(bucket);

        BucketConstructor.applyFake(buckets, 5, fakeTemplate);

        assertEquals(5, buckets.get(0).size());
        assertTrue(buckets.get(0).stream().anyMatch(p -> p.getId().startsWith("FAKE_")));
    }
}
