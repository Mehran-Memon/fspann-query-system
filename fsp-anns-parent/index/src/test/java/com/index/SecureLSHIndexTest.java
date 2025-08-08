package com.index;

import com.fspann.common.EncryptedPoint;
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
    private int numTables = 2;
    private int numBuckets = 32;

    @BeforeEach
    void setUp() {
        // keep LSH buckets consistent with index
        EvenLSH lsh = new EvenLSH(2, numBuckets, 32, 42L);
        index = new SecureLSHIndex(numTables, numBuckets, lsh);
    }

    @Test
    void testConcurrentAddAndCandidateCount() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        int total = 100;

        IntStream.range(0, total).forEach(i -> executor.submit(() -> {
            int b = i % numBuckets;
            List<Integer> buckets = new ArrayList<>(numTables);
            for (int t = 0; t < numTables; t++) buckets.add(b);
            EncryptedPoint pt = new EncryptedPoint(
                    "pt" + i,
                    buckets.get(0),              // legacy field kept but unused
                    new byte[12],
                    new byte[16],
                    1,
                    2,
                    buckets
            );
            index.addPoint(pt);
        }));
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Build per-table expansions that include all buckets
        List<List<Integer>> allBucketsPerTable = new ArrayList<>(numTables);
        List<Integer> all = IntStream.range(0, numBuckets).boxed().collect(Collectors.toList());
        for (int t = 0; t < numTables; t++) allBucketsPerTable.add(all);

        assertEquals(total, index.candidateCount(allBucketsPerTable));
    }
}
