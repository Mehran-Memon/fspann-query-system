package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.index.paper.PartitionedIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Fast, deterministic paper-mode query test.
 * Indexes a tiny in-memory dataset and verifies query search/fetch works.
 */
class QueryEndToEndPaperModeTest {

    // Core deps (some real, some mocked)
    private SecureLSHIndexService indexService;
    private QueryServiceImpl queryService;

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private RocksDBMetadataManager meta;
    private EncryptedPointBuffer buffer;

    // Small, deterministic paper engine
    private PartitionedIndexService paper;

    // Test-time vector store so decryptFromPoint can return originals
    private final Map<String, double[]> plaintextById = new ConcurrentHashMap<>();

    private static final SecretKey TEST_KEY = new SecretKeySpec(new byte[32], "AES");
    private static final KeyVersion KV1 = new KeyVersion(1, TEST_KEY);

    // Tiny toy dataset in 2D
    private final double[][] points = new double[][]{
            {0.0, 0.0},    // id "0"
            {1.0, 0.0},    // id "1"
            {0.0, 1.0},    // id "2"
            {1.0, 1.0},    // id "3"
            {5.0, 5.0},    // id "4"
            {5.1, 5.0},    // id "5"
            {10.0, 10.0},  // id "6"
            {10.1, 10.0}   // id "7"
    };

    @BeforeEach
    void setup() throws Exception {
        // ---- Mocks for system services we don't want to hit on disk/network
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        meta   = mock(RocksDBMetadataManager.class);
        buffer = mock(EncryptedPointBuffer.class);

        when(keys.getCurrentVersion()).thenReturn(KV1);
        when(keys.getVersion(anyInt())).thenReturn(KV1);

        // encrypt(id, vec) → EncryptedPoint and remember the plaintext for decryptFromPoint
        when(crypto.encrypt(anyString(), any(double[].class))).thenAnswer(inv -> {
            String id = inv.getArgument(0, String.class);
            double[] v = inv.getArgument(1, double[].class);
            plaintextById.put(id, v.clone());
            // dummy iv/ciphertext; version=1; perTable=null
            return new EncryptedPoint(id, /*shard*/0, new byte[12], new byte[32], 1, v.length, null);
        });

        // encryptToPoint("query", vec, key) for token build
        when(crypto.encryptToPoint(eq("query"), any(double[].class), any()))
                .thenAnswer(inv -> {
                    double[] v = inv.getArgument(1, double[].class);
                    return new EncryptedPoint("query", 0, new byte[12], new byte[32], 1, v.length, null);
                });

        // decryptFromPoint → return vector from our map
        when(crypto.decryptFromPoint(any(EncryptedPoint.class), any()))
                .thenAnswer(inv -> {
                    EncryptedPoint ep = inv.getArgument(0, EncryptedPoint.class);
                    double[] v = plaintextById.get(ep.getId());
                    return (v != null) ? v.clone() : new double[0];
                });

        // metadata/buffer just need to be callable
        when(meta.getPointsBaseDir()).thenReturn("in-mem");

        // ---- Real paper engine: tiny params; build immediately
        // m=8 projections, λ=4 bits/proj, ℓ=3 divisions, seed=13, buildThreshold=1 (build as we insert)
        paper = new PartitionedIndexService(8, 4, 3, 13L, /*buildThreshold*/1, /*maxCandidates*/-1);

        // ---- Index service in 'partitioned' mode (paper engine present)
        // One shard; let paper tables be the only "tables" notion here
                indexService = new SecureLSHIndexService(
                        crypto, keys, meta, paper,
                /*legacyIndex*/ null, /*legacyLsh*/ null,
                buffer, /*numTables (legacy path, unused)*/ 3, /*numShards*/ 1
        );

        // ---- Query service (uses same index + crypto + keys)
        queryService = new QueryServiceImpl(indexService, crypto, keys, /*tokenFactory*/ null);

        // ---- Insert tiny dataset
        for (int i = 0; i < points.length; i++) {
            String id = String.valueOf(i);
            indexService.insert(id, points[i]);
        }
    }

    @Test
    void nearest_from_dense_cluster_is_returned_first() {
        // Query near (5.0, 5.0) — expect ids 4/5 to be top-2 in any order, with 4 most likely first
        double[] q = {5.05, 5.0};
        // Our decryptQuery stub returns its first arg; pass q via that "channel"
        QueryToken token = buildPaperToken(q, /*topK*/3);

        // Perform search
        when(crypto.decryptQuery(any(), any(), any())).thenReturn(q.clone());
        var results = queryService.search(token);

        assertFalse(results.isEmpty(), "Should return at least one neighbor");
        assertTrue(results.size() <= 3);

        // Pull just IDs in order
        List<String> ids = new ArrayList<>();
        for (QueryResult r : results) ids.add(r.getId());

        assertTrue(ids.contains("4"), "cluster neighbor id '4' should be present");
        assertTrue(ids.contains("5"), "cluster neighbor id '5' should be present");

        // Ensure best is one of the cluster
        assertTrue(ids.get(0).equals("4") || ids.get(0).equals("5"),
                "nearest should come from the (5,5) cluster");
    }

    @Test
    void far_query_returns_far_cluster() {
        // Query near (10, 10) — expect ids 6/7 to lead
        double[] q = {10.05, 10.0};
        QueryToken token = buildPaperToken(q, /*topK*/2);
        when(crypto.decryptQuery(any(), any(), any())).thenReturn(q.clone());

        var results = queryService.search(token);

        assertFalse(results.isEmpty());
        List<String> ids = new ArrayList<>();
        for (QueryResult r : results) ids.add(r.getId());

        assertTrue(ids.contains("6") || ids.contains("7"));
        assertTrue(ids.get(0).equals("6") || ids.get(0).equals("7"),
                "nearest should come from the (10,10) cluster");
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

    // ---------- helpers ----------

    private static int bitsetToInt(BitSet bits) {
        int v = 0;
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
            v |= (1 << i);
        }
        return v;
    }

    private QueryToken buildPaperToken(double[] q, int topK) {
        BitSet[] codes;
        try {
            var m = paper.getClass().getDeclaredMethod("code", double[].class);
            m.setAccessible(true);
            codes = (BitSet[]) m.invoke(paper, (Object) q);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot obtain paper codes from PartitionedIndexService", e);
        }
        if (codes == null || codes.length == 0) {
            throw new IllegalStateException("Paper codes are empty");
        }

        // perTable must align with codes.length and contain correct bucket ids
        int numTables = codes.length;
        int lambda = 4;
        List<List<Integer>> perTable = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            int bucket = bitsetToInt(codes[t]) & ((1 << lambda) - 1);
            perTable.add(List.of(bucket));
        }

        KeyVersion kv = keys.getCurrentVersion();
        EncryptedPoint ep = crypto.encryptToPoint("query", q, kv.getKey());
        String ctx = "epoch_" + kv.getVersion() + "_dim_" + q.length; // informational

        return new QueryToken(
                perTable,
                codes,
                ep.getIv(),
                ep.getCiphertext(),
                null,
                topK,
                numTables,
                ctx,
                q.length,
                kv.getVersion()
        );
    }


}
