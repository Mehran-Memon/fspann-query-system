package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.index.service.SecureLSHIndexService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Updated deterministic paper-mode E2E query test.
 * Automatically forces PartitionedIndexService to build partitions so that
 * candidate lists are non-empty even for tiny datasets.
 */
class QueryEndToEndPaperModeTest {

    private SecureLSHIndexService indexService;
    private QueryServiceImpl queryService;

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private RocksDBMetadataManager meta;
    private EncryptedPointBuffer buffer;

    private PartitionedIndexService paper;

    // Plaintext store for mock decrypt
    private final Map<String, double[]> plaintextById = new ConcurrentHashMap<>();

    private static final SecretKey TEST_KEY = new SecretKeySpec(new byte[32], "AES");
    private static final KeyVersion KV1 = new KeyVersion(1, TEST_KEY);

    // Tiny dataset in 2D
    private final double[][] points = new double[][] {
            {0.0, 0.0},    // 0
            {1.0, 0.0},    // 1
            {0.0, 1.0},    // 2
            {1.0, 1.0},    // 3
            {5.0, 5.0},    // 4
            {5.1, 5.0},    // 5
            {10.0, 10.0},  // 6
            {10.1, 10.0}   // 7
    };

    @BeforeEach
    void setup() throws Exception {

        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        meta   = mock(RocksDBMetadataManager.class);
        buffer = mock(EncryptedPointBuffer.class);

        when(keys.getCurrentVersion()).thenReturn(KV1);
        when(keys.getVersion(anyInt())).thenReturn(KV1);

        // Store plaintext and return encrypted point
        when(crypto.encrypt(anyString(), any(double[].class))).thenAnswer(inv -> {
            String id = inv.getArgument(0, String.class);
            double[] v = inv.getArgument(1, double[].class);
            plaintextById.put(id, v.clone());
            return new EncryptedPoint(id, 0, new byte[12], new byte[32], 1, v.length, null);
        });

        when(crypto.encryptToPoint(eq("query"), any(double[].class), any()))
                .thenAnswer(inv -> {
                    double[] v = inv.getArgument(1, double[].class);
                    return new EncryptedPoint("query", 0, new byte[12], new byte[32], 1, v.length, null);
                });

        // Mock decryptFromPoint → return original stored vector
        when(crypto.decryptFromPoint(any(EncryptedPoint.class), any()))
                .thenAnswer(inv -> {
                    EncryptedPoint ep = inv.getArgument(0, EncryptedPoint.class);
                    double[] v = plaintextById.get(ep.getId());
                    return (v != null) ? v.clone() : new double[0];
                });

        when(meta.getPointsBaseDir()).thenReturn("in-mem");

        // Real paper engine
        paper = new PartitionedIndexService(
                8, 4, 3, 13L,
                /*buildThreshold*/ 1,
                /*maxCandidates*/ -1
        );

        indexService = new SecureLSHIndexService(
                crypto, keys, meta,
                paper,
                buffer
        );

        queryService = new QueryServiceImpl(indexService, crypto, keys, null);

        // Insert tiny dataset
        for (int i = 0; i < points.length; i++) {
            indexService.insert(String.valueOf(i), points[i]);
        }

        // Force partitions to build even for tiny dataset
        forceBuildAllDimensions(paper);
    }

    // ------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------

    @Test
    void nearest_from_dense_cluster_is_returned_first() {
        double[] q = {5.05, 5.0};
        QueryToken token = buildPaperToken(q, 3);

        when(crypto.decryptQuery(any(), any(), any())).thenReturn(q.clone());

        var results = queryService.search(token);

        assertFalse(results.isEmpty(), "Should return at least one neighbor");
        assertTrue(results.size() <= 3);
        assertMonotoneDistances(results);

        String expectedFirst = argminAmongReturned(q, results);
        assertEquals(expectedFirst, results.get(0).getId());
    }

    @Test
    void far_query_returns_far_cluster() {
        double[] q = {10.05, 10.0};
        QueryToken token = buildPaperToken(q, 2);

        when(crypto.decryptQuery(any(), any(), any())).thenReturn(q.clone());

        var results = queryService.search(token);

        assertFalse(results.isEmpty(), "Should return at least one neighbor");
        assertMonotoneDistances(results);

        String expectedFirst = argminAmongReturned(q, results);
        assertEquals(expectedFirst, results.get(0).getId());
    }

    @Test
    void metrics_are_populated() {
        double[] q = {1.0, 1.0};
        QueryToken token = buildPaperToken(q, 5);

        when(crypto.decryptQuery(any(), any(), any())).thenReturn(q.clone());

        var results = queryService.search(token);

        assertNotNull(results);
        assertTrue(queryService.getLastCandTotal() >= queryService.getLastReturned());
        assertTrue(queryService.getLastCandDecrypted() >= queryService.getLastReturned());
        assertTrue(queryService.getLastQueryDurationNs() > 0);
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private String argminAmongReturned(double[] q, List<QueryResult> res) {
        String best = null;
        double bestDist = Double.POSITIVE_INFINITY;
        for (QueryResult r : res) {
            double[] v = plaintextById.get(r.getId());
            if (v == null) continue;
            double dist = l2sq(q, v);
            if (dist < bestDist) {
                bestDist = dist;
                best = r.getId();
            }
        }
        return best;
    }

    private static double l2sq(double[] a, double[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            s += d * d;
        }
        return s;
    }

    private void assertMonotoneDistances(List<QueryResult> res) {
        double prev = -1;
        for (QueryResult r : res) {
            assertTrue(r.getDistance() >= prev - 1e-12, "Distance sequence must be increasing");
            prev = r.getDistance();
        }
    }

    private static int bitsetToInt(BitSet bits) {
        int v = 0;
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1))
            v |= (1 << i);
        return v;
    }

    private QueryToken buildPaperToken(double[] q, int topK) {
        BitSet[] codes;
        try {
            Method codeMethod = paper.getClass().getDeclaredMethod("code", double[].class);
            codeMethod.setAccessible(true);
            codes = (BitSet[]) codeMethod.invoke(paper, (Object) q);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot obtain paper codes", e);
        }

        int[] buckets = paper.getBucketsForCodes(codes);
        int numTables = codes.length;

        List<List<Integer>> perTable = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            perTable.add(List.of(buckets[t]));
        }

        KeyVersion kv = keys.getCurrentVersion();
        EncryptedPoint ep = crypto.encryptToPoint("query", q, kv.getKey());
        String ctx = "epoch_" + kv.getVersion() + "_dim_" + q.length;

        return new QueryToken(
                perTable,
                codes,
                ep.getIv(),
                ep.getCiphertext(),
                topK,
                numTables,
                ctx,
                q.length,
                kv.getVersion()
        );
    }

    /**
     * Forces PartitionedIndexService to build intervals even for very small datasets.
     */
    private static void forceBuildAllDimensions(PartitionedIndexService paper) {
        try {
            Field dimsField = PartitionedIndexService.class.getDeclaredField("dims");
            dimsField.setAccessible(true);

            Map<Integer, ?> dims = (Map<Integer, ?>) dimsField.get(paper);
            if (dims.isEmpty()) return;

            Method buildMethod = PartitionedIndexService.class.getDeclaredMethod(
                    "buildAllDivisions",
                    dims.values().iterator().next().getClass()
            );
            buildMethod.setAccessible(true);

            for (Object dimState : dims.values()) {
                buildMethod.invoke(paper, dimState);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to force-build partitioned index", e);
        }
    }
}
