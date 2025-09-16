package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.index.core.BucketConstructor;
import org.junit.jupiter.api.Test;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BucketConstructorTest {

    @Test
    void testApplyFakeIsUnsupported() {
        EncryptedPoint fakeTemplate = new EncryptedPoint("real", 0, new byte[12], new byte[32], 1, 128, null);
        List<List<EncryptedPoint>> buckets = List.of(List.of(fakeTemplate));
        assertThrows(UnsupportedOperationException.class,
                () -> BucketConstructor.applyFake(buckets, 5, fakeTemplate));
    }

}
