package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

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
        // trivial single-bucket expansion per table for tests
        List<List<Integer>> tableBuckets = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) tableBuckets.add(List.of(1));
        return new QueryToken(
                tableBuckets,
                new byte[12],
                new byte[32],
                new double[]{1.0, 2.0}, // dim must match caller
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
        when(cryptoService.decryptFromPoint(good, key)).thenReturn(new double[]{1.0, 2.0});
        when(cryptoService.decryptFromPoint(bad,  key)).thenReturn(new double[]{100.0, 100.0});

        List<QueryResult> results = service.search(token);
        assertEquals("good", results.get(0).getId());
    }

    @Test
    void testKeyVersionFallbackNotUsed() {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{1.0, 2.0};
        QueryToken token = new QueryToken(
                List.of(List.of(1)),
                iv, encQuery, query,
                1, 1,
                "epoch_999_dim_2",
                2,
                999
        );

        SecretKey fallbackKey = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, fallbackKey));
        when(keyService.getVersion(999)).thenThrow(new IllegalArgumentException("no such version"));

        assertThrows(IllegalArgumentException.class, () -> service.search(token));
    }

    @Test
    void testRatioAndRecallWhenGroundtruthEmpty() throws Exception {
        // ratio = 0.0 when GT empty, recall stays 1.0 (by design)
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

        when(indexService.lookup(any(QueryToken.class))).thenReturn(Arrays.asList(
                new EncryptedPoint("id1", 0, new byte[12], new byte[32], 1, 2, List.of(5)),
                new EncryptedPoint("id2", 0, new byte[12], new byte[32], 1, 2, List.of(5))
        ));
        when(cryptoService.decryptFromPoint(any(), eq(key))).thenReturn(new double[]{0.5, 0.6});

        when(groundtruthManager.getGroundtruth(eq(1), anyInt())).thenReturn(new int[]{}); // empty GT

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 1, groundtruthManager);
        QueryEvaluationResult r = results.get(0); // k=1

        assertEquals(0.0, r.getRatio(), 1e-9);
        assertEquals(1.0, r.getRecall(), 1e-9);
    }

    @Test
    void testRatioCalculationWithGroundtruth_MatchAtTop1() throws Exception {
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

        assertEquals(1.0, top1Result.getRatio(), 1e-6);
        assertEquals(1.0, top1Result.getRecall(), 1e-6);
    }
}
