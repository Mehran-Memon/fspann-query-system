package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;
import com.fspann.common.IndexService.LookupWithDiagnostics;
import com.fspann.common.IndexService.SearchDiagnostics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QueryServiceImplTest {
    @Mock private IndexService indexService;
    @Mock private CryptoService cryptoService;
    @Mock private KeyLifeCycleService keyService;
    @Mock private GroundtruthManager groundtruthManager;

    private QueryServiceImpl service;

    @Mock private EncryptedPointBuffer pointBuffer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Provide a fake buffer with dummy metrics
        when(indexService.getPointBuffer()).thenReturn(pointBuffer);
        when(pointBuffer.getLastBatchInsertTimeMs()).thenReturn(5L);
        when(pointBuffer.getTotalFlushedPoints()).thenReturn(100);
        when(pointBuffer.getFlushThreshold()).thenReturn(1000);

        service = new QueryServiceImpl(indexService, cryptoService, keyService);
    }

    @AfterEach
    void tearDown() {
        clearInvocations(indexService, cryptoService, keyService, groundtruthManager);
    }

    private QueryToken tokenWithPerTable(int dim, int topK, int numTables, String ctx, int version) {
        List<List<Integer>> tableBuckets = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) tableBuckets.add(List.of(1));
        return new QueryToken(
                tableBuckets,
                new byte[12],
                new byte[32],
                new double[]{1.0, 2.0}, // test vectors use dim=2
                topK,
                numTables,
                ctx,
                dim,
                version
        );
    }

    @Test
    void testPartialCandidateSorting() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{1.0, 2.0};
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(7, key);
        String ctx = "epoch_7_dim_2";

        QueryToken token = new QueryToken(
                List.of(List.of(1)), iv, encQuery, query,
                2, 1, ctx, 2, 7
        );

        when(keyService.getVersion(7)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(encQuery, iv, key)).thenReturn(query);

        EncryptedPoint good = new EncryptedPoint("good", 0, iv, encQuery, 7, 2, List.of(1));
        EncryptedPoint bad  = new EncryptedPoint("bad",  0, iv, encQuery, 7, 2, List.of(1));

        when(indexService.lookup(token)).thenReturn(List.of(good, bad));
        when(cryptoService.decryptFromPoint(good, key)).thenReturn(new double[]{1.0, 2.0});     // dist=0
        when(cryptoService.decryptFromPoint(bad,  key)).thenReturn(new double[]{100.0, 100.0}); // far

        List<QueryResult> results = service.search(token);
        assertEquals("good", results.get(0).getId());

        assertEquals(2, service.getLastCandTotal());
        assertEquals(2, service.getLastCandKeptVersion());
        assertEquals(2, service.getLastCandDecrypted());
        assertEquals(2, service.getLastReturned());

    }

    @Test
    void testKeyVersionFallbackNotUsed() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{1.0, 2.0};
        QueryToken token = new QueryToken(
                List.of(List.of(1)), iv, encQuery, query,
                1, 1, "epoch_999_dim_2", 2, 999
        );

        SecretKey fallbackKey = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(999)).thenThrow(new IllegalArgumentException("no such version"));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, fallbackKey));
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(fallbackKey))).thenReturn(query);

        EncryptedPoint p = new EncryptedPoint("id", 0, iv, encQuery, 1, 2, List.of(1));
        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of(p));
        when(cryptoService.decryptFromPoint(eq(p), eq(fallbackKey))).thenReturn(new double[]{1.0, 2.0});

        assertDoesNotThrow(() -> service.search(token));

        verify(keyService).getVersion(999);
        verify(keyService, atLeastOnce()).getCurrentVersion();
        verify(cryptoService).decryptQuery(eq(encQuery), eq(iv), eq(fallbackKey));

        // Ensure a lookup happened
        verify(indexService, atLeastOnce()).lookup(any(QueryToken.class));
    }

    @Test
    void testRatioAndPrecisionWhenGroundtruthEmpty() throws Exception {
        // In this layer (service), "ratio" is not computed -> NaN.
        // Precision@K (stored in 'precision' field) should be 0.0 when GT is empty.
        double[] query = new double[]{0.5, 0.6};
        String ctx = "epoch_1_dim_2";
        QueryToken token = new QueryToken(
                List.of(List.of(5)), new byte[12], new byte[32], query,
                10, 1, ctx, 2, 1
        );

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(any(), any(), eq(key))).thenReturn(query);

        List<EncryptedPoint> ret = Arrays.asList(
                new EncryptedPoint("id1", 0, new byte[12], new byte[32], 1, 2, List.of(5)),
                new EncryptedPoint("id2", 0, new byte[12], new byte[32], 1, 2, List.of(5))
        );
        when(indexService.lookup(any(QueryToken.class))).thenReturn(ret);

        when(cryptoService.decryptFromPoint(any(), eq(key))).thenReturn(new double[]{0.5, 0.6});
        when(groundtruthManager.getGroundtruth(eq(1), anyInt())).thenReturn(new int[]{}); // empty GT

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 1, groundtruthManager);
        QueryEvaluationResult r = results.get(0); // k=1

        assertTrue(Double.isNaN(r.getRatio()), "ratio should be NaN in QueryServiceImpl layer");
        assertEquals(0.0, r.getPrecision(), 1e-9);
    }

    @Test
    void testPrecisionWithGroundtruth_MatchAtTop1_RatioDeferred() throws Exception {
        // Ratio remains NaN here; precision@1 should be 1.0 when the retrieved id matches GT.
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] queryVec = new double[]{1.0, 1.0};
        String ctx = "epoch_1_dim_2";

        QueryToken token = new QueryToken(
                List.of(List.of(5)),
                iv, encQuery, queryVec,
                1, 1, ctx, 2, 1
        );

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(queryVec);

        EncryptedPoint top1 = new EncryptedPoint("42", 0, iv, encQuery, 1, 2, List.of(5));

        when(indexService.lookup(any())).thenReturn(List.of(top1));
        when(cryptoService.decryptFromPoint(eq(top1), eq(key))).thenReturn(new double[]{0.0, 0.0});
        when(groundtruthManager.getGroundtruth(eq(0), anyInt())).thenReturn(new int[]{42});

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 0, groundtruthManager);
        QueryEvaluationResult top1Result = results.stream()
                .filter(r -> r.getTopKRequested() == 1)
                .findFirst().orElseThrow();

        assertTrue(Double.isNaN(top1Result.getRatio()), "ratio is computed in ForwardSecureANNSystem, not here");
        assertEquals(1.0, top1Result.getPrecision(), 1e-6);
    }

    @Test
    void testVersionFilterSkipsMismatchedCandidates() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.0, 0.0};

        // Query token says epoch_2, version=2
        String ctx = "epoch_2_dim_2";
        SecretKey k2 = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(2)).thenReturn(new KeyVersion(2, k2));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(2, k2));
        when(cryptoService.decryptQuery(encQuery, iv, k2)).thenReturn(query);

        QueryToken token = new QueryToken(
                List.of(List.of(1)), iv, encQuery, query,
                5, 1, ctx, 2, 2
        );

        // One matching version (2), one mismatched (1)
        EncryptedPoint v2 = new EncryptedPoint("ok",   0, iv, encQuery, 2, 2, List.of(1));
        EncryptedPoint v1 = new EncryptedPoint("skip", 0, iv, encQuery, 1, 2, List.of(1));

        when(indexService.lookup(token)).thenReturn(List.of(v2, v1));
        when(cryptoService.decryptFromPoint(eq(v2), eq(k2))).thenReturn(new double[]{0.0, 0.0});

        List<QueryResult> results = service.search(token);

        assertEquals(1, results.size(), "Only same-version candidate should be considered");
        assertEquals("ok", results.get(0).getId());
        verify(cryptoService, never()).decryptFromPoint(eq(v1), any());
    }

    @Test
    void testNoCandidatesPathCounters() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.0, 0.0};

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(1)).thenReturn(new KeyVersion(1, key));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key));
        when(cryptoService.decryptQuery(encQuery, iv, key)).thenReturn(query);

        QueryToken token = new QueryToken(
                List.of(List.of(1)), iv, encQuery, query,
                3, 1, "epoch_1_dim_2", 2, 1
        );

        when(indexService.lookup(token)).thenReturn(List.of()); // no candidates

        List<QueryResult> out = service.search(token);
        assertTrue(out.isEmpty());
        assertEquals(0, service.getLastCandTotal());
        assertEquals(0, service.getLastCandKeptVersion());
        assertEquals(0, service.getLastCandDecrypted());
        assertEquals(0, service.getLastReturned());
    }

    @Test
    void countersReflectFilteringAndReturnSizes() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.0, 0.0};

        // Query token targets version 2
        String ctx = "epoch_2_dim_2";
        SecretKey k2 = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(2)).thenReturn(new KeyVersion(2, k2));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(2, k2));
        when(cryptoService.decryptQuery(encQuery, iv, k2)).thenReturn(query);

        QueryToken token = new QueryToken(
                List.of(List.of(1)), iv, encQuery, query,
                5, 1, ctx, 2, 2
        );

        // candidates: one good (v2), one filtered (v1)
        EncryptedPoint v2 = new EncryptedPoint("ok",   0, iv, encQuery, 2, 2, List.of(1));
        EncryptedPoint v1 = new EncryptedPoint("skip", 0, iv, encQuery, 1, 2, List.of(1));

        when(indexService.lookup(token)).thenReturn(List.of(v2, v1));
        when(cryptoService.decryptFromPoint(eq(v2), eq(k2))).thenReturn(new double[]{0.0, 0.0});

        List<QueryResult> out = service.search(token);
        assertEquals(1, out.size());
        assertEquals("ok", out.get(0).getId());

        assertEquals(2, service.getLastCandTotal(),        "ScannedCandidates");
        assertEquals(1, service.getLastCandKeptVersion(),  "KeptVersion");
        assertEquals(1, service.getLastCandDecrypted(),    "Decrypted");
        assertEquals(1, service.getLastReturned(),         "Returned");
    }

}
