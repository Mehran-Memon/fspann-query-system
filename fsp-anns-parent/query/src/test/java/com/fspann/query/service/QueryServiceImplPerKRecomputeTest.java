package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
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

        // Mock buffer stats used by the service
        EncryptedPointBuffer buffer = mock(EncryptedPointBuffer.class);
        when(buffer.getLastBatchInsertTimeMs()).thenReturn(5L);
        when(buffer.getTotalFlushedPoints()).thenReturn(100);
        when(buffer.getFlushThreshold()).thenReturn(1000);
        when(indexService.getPointBuffer()).thenReturn(buffer);

        service = new QueryServiceImpl(indexService, cryptoService, keyService, factory);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(anyInt())).thenReturn(new KeyVersion(1, key));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key));
        when(cryptoService.decryptQuery(any(), any(), eq(key))).thenReturn(new double[]{1.0, 2.0});

        // Index lookup returns empty: we only care about interaction behavior
        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of());

        // Ground truth empty to avoid extra logic
        when(groundtruth.getGroundtruth(anyInt(), anyInt())).thenReturn(new int[]{});
    }

    @Test
    void singleScanPrefixEvaluation_doesNotRecomputePerK() {
        // Build a "max-K" base token (single scan contract)
        // 2 tables, 1 bucket per table; K=100 (covers 1,20,40,60,80,100 by prefix)
        List<List<Integer>> baseExp = List.of(List.of(10), List.of(20));
        QueryToken base = new QueryToken(
                baseExp,
                new byte[12],
                new byte[32],
                new double[]{1.0, 2.0},
                100,              // topK: single scan at max K
                2,                // numTables
                "epoch_1_dim_2",
                2,                // dimension
                1                 // version
        );

        service.searchWithTopKVariants(base, 0, groundtruth);

        // Verify exactly ONE index lookup (single scan), not per-K recomputations
        ArgumentCaptor<QueryToken> captor = ArgumentCaptor.forClass(QueryToken.class);
        verify(indexService, times(1)).lookup(captor.capture());
        QueryToken lookedUp = captor.getValue();

        // The looked-up token should be the provided base token (same K, same expansions)
        assertEquals(100, lookedUp.getTopK());
        assertEquals(base.getTableBuckets(), lookedUp.getTableBuckets());

        // And we should not derive per-K tokens inside this flow
        verify(factory, never()).derive(any(QueryToken.class), anyInt());
        verifyNoMoreInteractions(ignoreStubs(indexService));
    }
}
