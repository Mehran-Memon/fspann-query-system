package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QueryServiceImplPerKRecomputeTest {

    private IndexService indexService;
    private CryptoService cryptoService;
    private KeyLifeCycleService keyService;
    private GroundtruthManager groundtruth;
    private QueryTokenFactory factory;

    private QueryServiceImpl service;

    @BeforeEach
    void setUp() {
        indexService   = mock(IndexService.class);
        cryptoService  = mock(CryptoService.class);
        keyService     = mock(KeyLifeCycleService.class);
        groundtruth    = mock(GroundtruthManager.class);
        factory        = mock(QueryTokenFactory.class);

        EncryptedPointBuffer buffer = mock(EncryptedPointBuffer.class);
        when(buffer.getLastBatchInsertTimeMs()).thenReturn(5L);
        when(buffer.getTotalFlushedPoints()).thenReturn(100);
        when(buffer.getFlushThreshold()).thenReturn(1000);
        when(indexService.getPointBuffer()).thenReturn(buffer);

        service = new QueryServiceImpl(indexService, cryptoService, keyService, factory);

        SecretKey testKey = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(anyInt()))
                .thenAnswer(inv -> new KeyVersion(inv.getArgument(0, Integer.class), testKey));
        when(keyService.getCurrentVersion())
                .thenReturn(new KeyVersion(1, testKey));

        when(cryptoService.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{1.0, 2.0});

        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of());
        when(groundtruth.getGroundtruth(anyInt(), anyInt())).thenReturn(new int[]{});
    }

    @Test
    void singleScanPrefixEvaluation_doesNotRecomputePerK() {
        List<List<Integer>> baseExp = List.of(List.of(10), List.of(20));
        byte[] iv = new byte[12];
        byte[] enc = new byte[32];

        QueryToken base = new QueryToken(
                baseExp,
                /*codes*/ null,
                iv,
                enc,
                100,
                2,
                "epoch_1_dim_2",
                2,
                1
        );

        service.searchWithTopKVariants(base, 0, groundtruth);

        ArgumentCaptor<QueryToken> captor = ArgumentCaptor.forClass(QueryToken.class);
        verify(indexService, atLeastOnce()).lookup(captor.capture());

        for (QueryToken lookedUp : captor.getAllValues()) {
            assertEquals(100, lookedUp.getTopK(), "Should evaluate at max-K only");
            assertEquals(base.getTableBuckets(), lookedUp.getTableBuckets(), "No per-K bucket recompute");
        }

        verify(factory, never()).derive(any(QueryToken.class), anyInt());
    }

    @Test
    void searchWithDynamicTopK() {
        byte[] iv = new byte[12];
        byte[] enc = new byte[32];

        QueryToken token = new QueryToken(
                List.of(List.of(10), List.of(20)),
                null,
                iv,
                enc,
                100,
                2,
                "epoch_1_dim_2",
                2,
                1
        );

        EncryptedPoint good = new EncryptedPoint("good", 0, iv, enc, 1, 2, List.of(1));
        EncryptedPoint bad  = new EncryptedPoint("bad",  0, iv, enc, 1, 2, List.of(1));

        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of(good, bad));
        when(cryptoService.decryptFromPoint(eq(good), any())).thenReturn(new double[]{1.0, 2.0});
        when(cryptoService.decryptFromPoint(eq(bad),  any())).thenReturn(new double[]{100.0, 100.0});

        List<QueryResult> results = service.search(token);

        assertFalse(results.isEmpty());
        assertEquals("good", results.get(0).getId());
        assertEquals(2, service.getLastCandTotal());
        assertEquals(2, service.getLastCandKeptVersion());
        assertEquals(2, service.getLastCandDecrypted());
        assertEquals(2, service.getLastReturned());
    }

    @Test
    void searchWithGroundtruthAndPrecision() {
        byte[] iv = new byte[12];
        byte[] enc = new byte[32];
        double[] query = new double[]{1.0, 2.0};
        String ctx = "epoch_1_dim_2";

        QueryToken token = new QueryToken(
                List.of(List.of(5)),
                null,
                iv,
                enc,
                10,
                1,
                ctx,
                2,
                1
        );

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);
        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(enc, iv, key)).thenReturn(query);

        EncryptedPoint top1 = new EncryptedPoint("42", 0, iv, enc, 1, 2, List.of(5));
        when(indexService.lookup(any())).thenReturn(List.of(top1));
        when(cryptoService.decryptFromPoint(eq(top1), eq(key))).thenReturn(new double[]{0.0, 0.0});

        when(groundtruth.getGroundtruth(eq(1), anyInt())).thenReturn(new int[]{});

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 0, groundtruth);
        QueryEvaluationResult top1Result = results.stream()
                .filter(r -> r.getTopKRequested() == 1)
                .findFirst().orElseThrow();

        assertTrue(Double.isNaN(top1Result.getRatio()), "Ratio should be NaN when no groundtruth");
        assertEquals(0.0, top1Result.getPrecision(), 1e-6);
    }
}
