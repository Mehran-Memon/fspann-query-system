package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MQueryServiceImplTest {

    private QueryServiceImpl queryService;
    private PartitionedIndexService index;
    private CryptoService crypto;
    private KeyLifeCycleService keyService;
    private QueryTokenFactory tokenFactory;
    private SystemConfig cfg;

    @BeforeEach
    void setUp() {

        index = mock(PartitionedIndexService.class);
        crypto = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        tokenFactory = mock(QueryTokenFactory.class);
        cfg = mock(SystemConfig.class);

        // ---- Runtime config (CRITICAL) ----
        SystemConfig.RuntimeConfig runtime = mock(SystemConfig.RuntimeConfig.class);
        when(runtime.getRefinementLimit()).thenReturn(10);
        when(runtime.getMaxRefinementFactor()).thenReturn(5);
        when(cfg.getRuntime()).thenReturn(runtime);

        // ---- Stabilization config ----
        SystemConfig.StabilizationConfig stab = mock(SystemConfig.StabilizationConfig.class);
        when(stab.isEnabled()).thenReturn(false);
        when(cfg.getStabilization()).thenReturn(stab);

        queryService = new QueryServiceImpl(
                index,
                crypto,
                keyService,
                tokenFactory,
                cfg
        );
    }

    @Test
    void testSearchWithValidQuery() {

        QueryToken token = mock(QueryToken.class);
        when(token.getVersion()).thenReturn(1);
        when(token.getTopK()).thenReturn(10);
        when(token.getEncryptedQuery()).thenReturn(new byte[]{1, 2, 3});
        when(token.getIv()).thenReturn(new byte[]{4, 5, 6});

        SecretKey key = mock(SecretKey.class);
        when(keyService.getVersion(1))
                .thenReturn(new KeyVersion(1, key));
        when(keyService.getCurrentVersion())
                .thenReturn(new KeyVersion(1, key));

        when(crypto.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{1.0, 2.0, 3.0});

        // ---- Candidate IDs ----
        when(index.lookupCandidateIds(token))
                .thenReturn(List.of("id1", "id2"));

        EncryptedPoint p1 = mock(EncryptedPoint.class);
        when(p1.getId()).thenReturn("id1");
        when(p1.getKeyVersion()).thenReturn(1);

        EncryptedPoint p2 = mock(EncryptedPoint.class);
        when(p2.getId()).thenReturn("id2");
        when(p2.getKeyVersion()).thenReturn(1);

        when(index.loadPointIfActive("id1")).thenReturn(p1);
        when(index.loadPointIfActive("id2")).thenReturn(p2);

        when(crypto.decryptFromPoint(eq(p1), any()))
                .thenReturn(new double[]{1.1, 2.1, 3.1});
        when(crypto.decryptFromPoint(eq(p2), any()))
                .thenReturn(new double[]{1.2, 2.2, 3.2});

        List<QueryResult> results = queryService.search(token);

        assertNotNull(results);
        assertEquals(2, results.size());

        assertTrue(results.stream().anyMatch(r -> r.getId().equals("id1")));
        assertTrue(results.stream().anyMatch(r -> r.getId().equals("id2")));
    }

    @Test
    void testSearchWithEmptyCandidates() {

        QueryToken token = mock(QueryToken.class);
        when(token.getVersion()).thenReturn(1);
        when(token.getTopK()).thenReturn(10);
        when(token.getEncryptedQuery()).thenReturn(new byte[]{1, 2, 3});
        when(token.getIv()).thenReturn(new byte[]{4, 5, 6});

        SecretKey key = mock(SecretKey.class);
        when(keyService.getVersion(1))
                .thenReturn(new KeyVersion(1, key));

        when(crypto.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{1.0, 2.0, 3.0});

        when(index.lookupCandidateIds(token))
                .thenReturn(Collections.emptyList());

        List<QueryResult> results = queryService.search(token);

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }
}
