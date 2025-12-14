package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.service.QueryServiceImpl;
import com.fspann.query.core.QueryTokenFactory;

import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryServiceImplStabilizationIT {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private PartitionedIndexService index;
    private QueryServiceImpl queryService;

    private SecretKey k;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = mock(PartitionedIndexService.class);

        k = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, k));

        SystemConfig config = mock(SystemConfig.class);
        SystemConfig.StabilizationConfig stab = mock(SystemConfig.StabilizationConfig.class);
        when(config.getStabilization()).thenReturn(stab);
        when(stab.isEnabled()).thenReturn(true);
        when(stab.getAlpha()).thenReturn(0.2);
        when(stab.getMinCandidates()).thenReturn(5);

        QueryTokenFactory tf = mock(QueryTokenFactory.class);
        queryService = new QueryServiceImpl(index, crypto, keys, tf, config);
    }

    @Test
    void testStabilizationAppliesAlphaAndMinCandidates() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[32];

        List<EncryptedPoint> raw = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            raw.add(new EncryptedPoint(
                    String.valueOf(i),
                    1,
                    iv,
                    ct,
                    1,
                    2,
                    0,
                    List.of(),
                    List.of()
            ));
        }

        QueryToken token = mock(QueryToken.class);
        when(token.getVersion()).thenReturn(1);
        when(token.getTopK()).thenReturn(100);
        when(token.getEncryptedQuery()).thenReturn(new byte[32]);
        when(token.getIv()).thenReturn(iv);

        when(crypto.decryptQuery(any(), any(), any())).thenReturn(new double[]{0.5,0.5});
        when(index.lookup(token)).thenReturn(raw);

        for (EncryptedPoint p : raw)
            when(crypto.decryptFromPoint(eq(p), isNull()))
                    .thenReturn(new double[]{1.0,1.0});

        List<QueryResult> results = queryService.search(token);

        assertNotNull(results);
        assertEquals(5, results.size()); // minCandidates = 5
    }

    @Test
    void testStabilizationDisabledKeepsAllCandidates() {
        SystemConfig config = mock(SystemConfig.class);
        SystemConfig.StabilizationConfig stab = mock(SystemConfig.StabilizationConfig.class);
        when(config.getStabilization()).thenReturn(stab);
        when(stab.isEnabled()).thenReturn(false);

        QueryTokenFactory tf = mock(QueryTokenFactory.class);
        queryService = new QueryServiceImpl(index, crypto, keys, tf, config);

        byte[] iv = new byte[12];
        byte[] ct = new byte[32];

        List<EncryptedPoint> raw = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            raw.add(new EncryptedPoint(
                    String.valueOf(i),
                    1,
                    iv,
                    ct,
                    1,
                    2,
                    0,
                    List.of(),
                    List.of()
            ));
        }

        QueryToken token = mock(QueryToken.class);
        when(token.getVersion()).thenReturn(1);
        when(token.getTopK()).thenReturn(100);
        when(token.getEncryptedQuery()).thenReturn(new byte[32]);
        when(token.getIv()).thenReturn(iv);

        when(crypto.decryptQuery(any(), any(), any())).thenReturn(new double[]{0.5,0.5});
        when(index.lookup(token)).thenReturn(raw);

        for (EncryptedPoint p : raw)
            when(crypto.decryptFromPoint(eq(p), isNull()))
                    .thenReturn(new double[]{1.0,1.0});

        List<QueryResult> results = queryService.search(token);

        assertEquals(6, results.size());
    }
}
