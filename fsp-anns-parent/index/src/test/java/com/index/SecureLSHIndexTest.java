package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class SecureLSHIndexTest {
    private SecureLSHIndex index;

    @BeforeEach
    void setUp() {
        index = new SecureLSHIndex(2, 32, new EvenLSH(2, 10));
    }

    @Test
    void testConcurrentAdd() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        IntStream.range(0, 100).forEach(i -> executor.submit(() -> {
            EncryptedPoint pt = new EncryptedPoint("pt" + i, i % 32, new byte[12], new byte[16], 1, 2);
            index.addPoint(pt);
        }));
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        List<Integer> candidateBuckets = IntStream.range(0, 32).boxed().collect(Collectors.toList()); // Query all shards
        byte[] iv = new byte[12];
        byte[] encryptedQuery = new byte[32];
        double[] plaintextQuery = {1.0, 2.0};
        QueryToken token = new QueryToken(candidateBuckets, iv, encryptedQuery, plaintextQuery, 100, 2, "epoch_0", 2 ,0 , 7);
        assertEquals(100, index.queryEncrypted(token).size()); // Match all points
    }
}