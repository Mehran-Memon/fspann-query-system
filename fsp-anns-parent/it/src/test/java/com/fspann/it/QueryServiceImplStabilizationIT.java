package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.service.QueryServiceImpl;
import com.fspann.query.core.QueryTokenFactory;

import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Verifies STABILIZATION BEHAVIOR by counting how many candidates are DECRYPTED,
 * not how many are returned (return count is always topK).
 */
class QueryServiceImplStabilizationIT {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private PartitionedIndexService index;
    private QueryServiceImpl queryService;
    private SecretKey key;

    private SystemConfig config;
    private SystemConfig.StabilizationConfig stab;

    @BeforeEach
    void init() {

        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = mock(PartitionedIndexService.class);

        key = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, key));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, key));

        // ---------------- CONFIG ----------------
        config = mock(SystemConfig.class);
        stab   = mock(SystemConfig.StabilizationConfig.class);

        // Runtime config
        SystemConfig.RuntimeConfig runtime = mock(SystemConfig.RuntimeConfig.class);
        when(runtime.getMaxRefinementFactor()).thenReturn(1_000);
        when(runtime.getMaxCandidateFactor()).thenReturn(1_000);
        when(runtime.getEarlyStopCandidates()).thenReturn(Integer.MAX_VALUE);
        when(runtime.getMaxRelaxationDepth()).thenReturn(10);
        when(config.getRuntime()).thenReturn(runtime);

        // Paper config (mocked getters ONLY)
        SystemConfig.PaperConfig paper = mock(SystemConfig.PaperConfig.class);
        when(paper.getLambda()).thenReturn(1);
        when(paper.getM()).thenReturn(4);
        when(paper.getDivisions()).thenReturn(2);
        when(paper.getTables()).thenReturn(1);
        when(paper.getSeed()).thenReturn(13L);
        when(config.getPaper()).thenReturn(paper);

        // Stabilization
        when(config.getStabilization()).thenReturn(stab);
        when(stab.isEnabled()).thenReturn(true);
        when(stab.getAlpha()).thenReturn(0.02);
        when(stab.getMinCandidatesRatio()).thenReturn(1.25);

        queryService = new QueryServiceImpl(
                index,
                crypto,
                keys,
                mock(QueryTokenFactory.class),
                config
        );
    }

    @Test
    void stabilizationLimitsDecryptionCount() {

        List<EncryptedPoint> raw = makePoints(20);

        QueryToken token = mock(QueryToken.class);
        when(token.getVersion()).thenReturn(1);
        when(token.getTopK()).thenReturn(10);
        when(token.getEncryptedQuery()).thenReturn(new byte[32]);
        when(token.getIv()).thenReturn(new byte[12]);
        when(token.getDimension()).thenReturn(2);
        when(token.getHashesByTable()).thenReturn(new int[1][2]);

        when(crypto.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{0.5, 0.5});

        when(index.lookupCandidateIds(token))
                .thenReturn(raw.stream().map(EncryptedPoint::getId).toList());

        when(index.loadPointIfActive(anyString()))
                .thenAnswer(inv ->
                        raw.stream()
                                .filter(p -> p.getId().equals(inv.getArgument(0)))
                                .findFirst().orElse(null));

        for (EncryptedPoint p : raw) {
            when(crypto.decryptFromPoint(eq(p), any()))
                    .thenReturn(new double[]{1.0, 1.0});
        }

        List<QueryResult> results = queryService.search(token);

        assertEquals(10, results.size(), "Result count must be topK");

        ArgumentCaptor<EncryptedPoint> cap =
                ArgumentCaptor.forClass(EncryptedPoint.class);
        verify(crypto, atLeastOnce()).decryptFromPoint(cap.capture(), any());

        int decrypted = cap.getAllValues().size();

        // K=10, ratio=1.25 → target ≈ 13
        assertTrue(decrypted >= 10);
        assertTrue(decrypted <= 15);
    }

    @Test
    void stabilizationDisabledDecryptsAll() {

        when(stab.isEnabled()).thenReturn(false);

        List<EncryptedPoint> raw = makePoints(20);

        QueryToken token = mock(QueryToken.class);
        when(token.getVersion()).thenReturn(1);
        when(token.getTopK()).thenReturn(10);
        when(token.getEncryptedQuery()).thenReturn(new byte[32]);
        when(token.getIv()).thenReturn(new byte[12]);
        when(token.getDimension()).thenReturn(2);
        when(token.getHashesByTable()).thenReturn(new int[1][2]);

        when(crypto.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{0.5, 0.5});

        when(index.lookupCandidateIds(token))
                .thenReturn(raw.stream().map(EncryptedPoint::getId).toList());

        when(index.loadPointIfActive(anyString()))
                .thenAnswer(inv ->
                        raw.stream()
                                .filter(p -> p.getId().equals(inv.getArgument(0)))
                                .findFirst().orElse(null));

        for (EncryptedPoint p : raw) {
            when(crypto.decryptFromPoint(eq(p), any()))
                    .thenReturn(new double[]{1.0, 1.0});
        }

        queryService.search(token);

        ArgumentCaptor<EncryptedPoint> cap =
                ArgumentCaptor.forClass(EncryptedPoint.class);
        verify(crypto, atLeastOnce()).decryptFromPoint(cap.capture(), any());

        assertEquals(20, cap.getAllValues().size());
    }

    // ---------------- helpers ----------------

    private static List<EncryptedPoint> makePoints(int n) {
        byte[] iv = new byte[12];
        byte[] ct = new byte[32];
        List<EncryptedPoint> out = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            out.add(new EncryptedPoint(
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
        return out;
    }
}