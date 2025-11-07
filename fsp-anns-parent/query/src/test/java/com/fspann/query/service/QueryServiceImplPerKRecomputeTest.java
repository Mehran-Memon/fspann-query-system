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
import com.fspann.common.IndexService.LookupWithDiagnostics;
import com.fspann.common.IndexService.SearchDiagnostics;

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

        // 🔧 Use a single test key and return a KeyVersion for ANY version requested.
        SecretKey testKey = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(anyInt()))
                .thenAnswer(inv -> new KeyVersion(inv.getArgument(0, Integer.class), testKey));
        when(keyService.getCurrentVersion())
                .thenReturn(new KeyVersion(1, testKey));

        // 🔧 Keep decryptQuery simple and version-agnostic.
        when(cryptoService.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{1.0, 2.0});

        // Default: empty lookup unless a test overrides it
        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of());

        when(groundtruth.getGroundtruth(anyInt(), anyInt())).thenReturn(new int[]{});
    }


    @Test
    void singleScanPrefixEvaluation_doesNotRecomputePerK() {
        // Prepare a QueryToken for testing
        List<List<Integer>> baseExp = List.of(List.of(10), List.of(20));
        QueryToken base = new QueryToken(
                baseExp, new byte[12], new byte[32], new double[]{1.0, 2.0},
                100, 2, "epoch_1_dim_2", 2, 1
        );

        // Diagnostics stub with empty candidates
        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of());

        // Execute search with topK variants
        service.searchWithTopKVariants(base, 0, groundtruth);

        // Capture the lookups that happen during the search process
        var captor = ArgumentCaptor.forClass(QueryToken.class);
        verify(indexService, atLeastOnce()).lookup(captor.capture());

        // Verify that only max-K is evaluated
        for (QueryToken lookedUp : captor.getAllValues()) {
            assertEquals(100, lookedUp.getTopK(), "Should evaluate at max-K only");
            assertEquals(base.getTableBuckets(), lookedUp.getTableBuckets(), "No per-K derivation");
        }

        // Ensure no per-K token derivations were done
        verify(factory, never()).derive(any(QueryToken.class), anyInt());

        // DO NOT call verifyNoMoreInteractions(ignoreStubs(indexService)) here — diagnostics are valid interactions.
    }

    @Test
    void searchWithDynamicTopK() {
        double[] query = new double[]{1.0, 2.0};
        QueryToken token = new QueryToken(
                List.of(List.of(10), List.of(20)),
                new byte[12], new byte[32], query,
                100, 2, "epoch_1_dim_2", 2, 1 // 🔧 version=1
        );

        // 🔧 Make candidates version=1 to match the token
        EncryptedPoint good = new EncryptedPoint("good", 0, new byte[12], new byte[32], 1, 2, List.of(1));
        EncryptedPoint bad  = new EncryptedPoint("bad",  0, new byte[12], new byte[32], 1, 2, List.of(1));

        // 🔧 Don’t over-match on the exact token instance
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
        // Prepare a QueryToken and simulate results
        double[] query = new double[]{1.0, 2.0};
        String ctx = "epoch_1_dim_2";
        QueryToken token = new QueryToken(
                List.of(List.of(5)), new byte[12], new byte[32], query,
                10, 1, ctx, 2, 1
        );

        // Simulate key and decryption
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);
        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(new byte[32]), eq(new byte[12]), eq(key))).thenReturn(query);

        // Simulate the index service returning results
        EncryptedPoint top1 = new EncryptedPoint("42", 0, new byte[12], new byte[32], 1, 2, List.of(5));
        when(indexService.lookup(any())).thenReturn(List.of(top1));
        when(cryptoService.decryptFromPoint(eq(top1), eq(key))).thenReturn(new double[]{0.0, 0.0});

        // Simulate empty groundtruth
        when(groundtruth.getGroundtruth(eq(1), anyInt())).thenReturn(new int[]{});

        // Execute search and verify precision and ratio
        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 0, groundtruth);
        QueryEvaluationResult top1Result = results.stream()
                .filter(r -> r.getTopKRequested() == 1)
                .findFirst().orElseThrow();

        // Validate that ratio is NaN and precision is 0 when groundtruth is empty
        assertTrue(Double.isNaN(top1Result.getRatio()), "Ratio should be NaN when no groundtruth");
        assertEquals(0.0, top1Result.getPrecision(), 1e-6);
    }
}
