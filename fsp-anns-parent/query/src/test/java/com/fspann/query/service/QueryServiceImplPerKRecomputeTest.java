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
import java.util.ArrayList;
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
        indexService = mock(IndexService.class);
        cryptoService = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        groundtruth = mock(GroundtruthManager.class);
        factory = mock(QueryTokenFactory.class);

        // 1️⃣ Add a mock EncryptedPointBuffer
        EncryptedPointBuffer buffer = mock(EncryptedPointBuffer.class);
        when(buffer.getLastBatchInsertTimeMs()).thenReturn(5L);
        when(buffer.getTotalFlushedPoints()).thenReturn(100);
        when(buffer.getFlushThreshold()).thenReturn(1000);

        // 2️⃣ Make indexService return this buffer
        when(indexService.getPointBuffer()).thenReturn(buffer);

        service = new QueryServiceImpl(indexService, cryptoService, keyService, factory);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getVersion(anyInt())).thenReturn(new KeyVersion(1, key));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key));
        when(cryptoService.decryptQuery(any(), any(), eq(key))).thenReturn(new double[]{1.0, 2.0});

        // index lookup returns empty (we only care about token derivation)
        when(indexService.lookup(any(QueryToken.class))).thenReturn(List.of());

        // groundtruth: empty to avoid extra logic
        when(groundtruth.getGroundtruth(anyInt(), anyInt())).thenReturn(new int[]{});
    }

    @Test
    void perKRecomputeChangesExpansions() {
        // base token: 2 tables, each with 1 bucket (e.g., [ [10], [20] ])
        List<List<Integer>> baseExp = List.of(List.of(10), List.of(20));
        QueryToken base = new QueryToken(
                baseExp,
                new byte[12],
                new byte[32],
                new double[]{1.0, 2.0},
                1, // topK
                2, // numTables
                "epoch_1_dim_2",
                2,
                1
        );

        // For k=1 -> return base as-is
        when(factory.derive(eq(base), eq(1))).thenReturn(base);

        // For k>=20 -> fabricate expansions with 2 buckets per table
        for (int k : new int[]{20, 40, 60, 80, 100}) {
            List<List<Integer>> exp = new ArrayList<>();
            exp.add(List.of(10, 11));
            exp.add(List.of(20, 21));
            QueryToken derived = new QueryToken(
                    exp,
                    base.getIv(),
                    base.getEncryptedQuery(),
                    base.getQueryVector(),
                    k,
                    base.getNumTables(),
                    base.getEncryptionContext(),
                    base.getDimension(),
                    base.getVersion()
            );
            when(factory.derive(eq(base), eq(k))).thenReturn(derived);
        }

        service.searchWithTopKVariants(base, 0, groundtruth);

        ArgumentCaptor<QueryToken> captor = ArgumentCaptor.forClass(QueryToken.class);
        verify(indexService, times(6)).lookup(captor.capture()); // 6 ks: 1,20,40,60,80,100

        // Find tokens for k=1 and k=20
        QueryToken k1 = captor.getAllValues().stream().filter(t -> t.getTopK() == 1).findFirst().orElseThrow();
        QueryToken k20 = captor.getAllValues().stream().filter(t -> t.getTopK() == 20).findFirst().orElseThrow();

        // k=1 uses base expansions: 1 bucket per table
        assertEquals(2, k1.getTableBuckets().size());
        assertEquals(1, k1.getTableBuckets().get(0).size());
        assertEquals(1, k1.getTableBuckets().get(1).size());

        // k=20 uses derived expansions: 2 buckets per table
        assertEquals(2, k20.getTableBuckets().size());
        assertEquals(2, k20.getTableBuckets().get(0).size());
        assertEquals(2, k20.getTableBuckets().get(1).size());
    }
}
