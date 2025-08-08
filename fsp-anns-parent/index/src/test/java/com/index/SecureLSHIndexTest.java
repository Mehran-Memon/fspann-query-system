package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class SecureLSHIndexTest {
    private SecureLSHIndex index;
    private EvenLSH lsh;

    @BeforeEach
    void setUp() {
        int numTables = 2;
        int numBuckets = 32;
        lsh = new EvenLSH(2, numBuckets);
        index = new SecureLSHIndex(numTables, numBuckets, lsh);
    }

    @Test
    void testConcurrentAddAndQueryAll() throws InterruptedException {
        int numTables = index.getNumHashTables();
        int numBuckets = lsh.getNumBuckets();

        ExecutorService executor = Executors.newFixedThreadPool(4);
        IntStream.range(0, 100).forEach(i -> executor.submit(() -> {
            double[] dummy = {i, i}; // 2D
            List<Integer> perTableBuckets = new ArrayList<>(numTables);
            for (int t = 0; t < numTables; t++) perTableBuckets.add(lsh.getBucketId(dummy, t));
            EncryptedPoint pt = new EncryptedPoint("pt" + i, perTableBuckets.get(0),
                    new byte[12], new byte[16], 1, 2, perTableBuckets);
            index.addPoint(pt);
        }));
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Build a token that scans ALL buckets in each table -> should fetch all 100
        List<List<Integer>> tableBuckets = new ArrayList<>(index.getNumHashTables());
        List<Integer> all = IntStream.range(0, numBuckets).boxed().collect(Collectors.toList());
        for (int t = 0; t < index.getNumHashTables(); t++) tableBuckets.add(all);

        QueryToken token = new QueryToken(
                tableBuckets,
                new byte[12],
                new byte[16],
                new double[]{1.0, 2.0},
                100,
                index.getNumHashTables(),
                "epoch_0_dim_2",
                2,
                0
        );

        assertEquals(100, index.queryEncrypted(token).size());
    }
}
