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
        List<List<Integer>> baseExp = List.of(List.of(10), List.of(20));
        QueryToken base = new QueryToken(
                baseExp, new byte[12], new byte[32], new double[]{1.0, 2.0},
                100, 2, "epoch_1_dim_2", 2, 1
        );

        // diagnostics stub with empty candidates (we only care about interaction pattern)
        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of());
        service.searchWithTopKVariants(base, 0, groundtruth);

        // Capture the lookups that actually happen
        var captor = ArgumentCaptor.forClass(QueryToken.class);
        verify(indexService, atLeastOnce()).lookup(captor.capture());

        for (QueryToken lookedUp : captor.getAllValues()) {
            assertEquals(100, lookedUp.getTopK(), "Should evaluate at max-K only");
            assertEquals(base.getTableBuckets(), lookedUp.getTableBuckets(), "No per-K derivation");
        }

        // no per-K token derivations
        verify(factory, never()).derive(any(QueryToken.class), anyInt());

        // DO NOT call verifyNoMoreInteractions(ignoreStubs(indexService)) here — diagnostics are valid interactions.
    }

}
