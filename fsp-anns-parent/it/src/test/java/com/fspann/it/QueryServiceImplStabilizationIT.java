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
 * CORRECTED: Tests verify HOW MANY CANDIDATES ARE PROCESSED (decrypted),
 * not the final result count (which is always limited to topK).
 *
 * Query Service Flow:
 * 1. Index lookup → raw candidates
 * 2. Stabilization → limit processing
 * 3. Decrypt candidates ← WE VERIFY THIS COUNT
 * 4. Compute distances
 * 5. Final refinement → return topK ← THIS always limits to K
 */
class QueryServiceImplStabilizationIT {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private PartitionedIndexService index;
    private QueryServiceImpl queryService;
    private SecretKey k;

    private SystemConfig config;
    private SystemConfig.StabilizationConfig stab;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = mock(PartitionedIndexService.class);

        k = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, k));

        // -------- Mock SystemConfig FIRST --------
        config = mock(SystemConfig.class);
        stab   = mock(SystemConfig.StabilizationConfig.class);

        // -------- RuntimeConfig (must exist BEFORE search) --------
        SystemConfig.RuntimeConfig runtime = mock(SystemConfig.RuntimeConfig.class);
        when(runtime.getMaxRefinementFactor()).thenReturn(100); // do not bind refinement
        when(config.getRuntime()).thenReturn(runtime);

        // -------- REAL PaperConfig (NOT mocked) --------
        SystemConfig.PaperConfig paper = new SystemConfig.PaperConfig();
        paper.probeLimit = 10;
        paper.lambda = 1;
        paper.m = 4;
        paper.divisions = 2;
        paper.seed = 13;

        when(config.getPaper()).thenReturn(paper);
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

    /**
     * Test: K-aware stabilization limits decryption count
     *
     * With K=10, target=13, we should decrypt ~13 candidates
     * (even though final result is limited to K=10)
     */
    @Test
    void testStabilizationLimitsDecryptionCount() {

        byte[] iv = new byte[12];
        byte[] ct = new byte[32];

        // Create 20 raw candidates
        List<EncryptedPoint> raw = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
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
        when(token.getTopK()).thenReturn(10);
        when(token.getEncryptedQuery()).thenReturn(new byte[32]);
        when(token.getIv()).thenReturn(iv);
        when(token.getDimension()).thenReturn(2);
        when(token.getCodes()).thenReturn(new BitSet[]{new BitSet(), new BitSet()});

        when(crypto.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{0.5, 0.5});
        when(index.lookupCandidateIds(token))
                .thenReturn(
                        raw.stream().map(EncryptedPoint::getId).toList()
                );
        when(index.loadPointIfActive(anyString()))
                .thenAnswer(inv -> {
                    String id = inv.getArgument(0);
                    return raw.stream()
                            .filter(p -> p.getId().equals(id))
                            .findFirst()
                            .orElse(null);
                });

        for (EncryptedPoint p : raw) {
            when(crypto.decryptFromPoint(eq(p), any()))
                    .thenReturn(new double[]{1.0, 1.0});
        }

        List<QueryResult> results = queryService.search(token);

        // VERIFY: Final result is topK (always)
        assertEquals(10, results.size(),
                "Final result should be limited to topK=10");

        // VERIFY: Decryption count matches stabilization target (~13)
        ArgumentCaptor<EncryptedPoint> captor = ArgumentCaptor.forClass(EncryptedPoint.class);
        verify(crypto, atLeastOnce()).decryptFromPoint(captor.capture(), any());

        int decryptionCount = captor.getAllValues().size();

        // K=10, target=10*1.25=12.5→13, should decrypt 10-15 candidates
        assertTrue(decryptionCount >= 10,
                "Should decrypt at least K candidates, got: " + decryptionCount);
        assertTrue(decryptionCount <= 15,
                "Should limit decryptions via stabilization, got: " + decryptionCount);
    }

    /**
     * Test: Insufficient candidates returns all available
     *
     * With only 6 candidates, should decrypt all 6
     * (final result will be min(6, K=10) = 6)
     */
    @Test
    void testStabilizationWithInsufficientCandidates() {

        byte[] iv = new byte[12];
        byte[] ct = new byte[32];

        // Only 6 candidates (less than target)
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
        when(token.getTopK()).thenReturn(10);
        when(token.getEncryptedQuery()).thenReturn(new byte[32]);
        when(token.getIv()).thenReturn(iv);
        when(token.getDimension()).thenReturn(2);
        when(token.getCodes()).thenReturn(new BitSet[]{new BitSet(), new BitSet()});

        when(crypto.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{0.5, 0.5});
        when(index.lookupCandidateIds(token))
                .thenReturn(
                        raw.stream().map(EncryptedPoint::getId).toList()
                );
        when(index.loadPointIfActive(anyString()))
                .thenAnswer(inv -> {
                    String id = inv.getArgument(0);
                    return raw.stream()
                            .filter(p -> p.getId().equals(id))
                            .findFirst()
                            .orElse(null);
                });

        for (EncryptedPoint p : raw) {
            when(crypto.decryptFromPoint(eq(p), any()))
                    .thenReturn(new double[]{1.0, 1.0});
        }

        List<QueryResult> results = queryService.search(token);

        // VERIFY: Result count = all available candidates
        assertEquals(6, results.size(),
                "Should return all 6 available candidates");

        // VERIFY: All 6 were decrypted
        ArgumentCaptor<EncryptedPoint> captor = ArgumentCaptor.forClass(EncryptedPoint.class);
        verify(crypto, atLeastOnce()).decryptFromPoint(captor.capture(), any());

        int decryptionCount = captor.getAllValues().size();
        assertEquals(6, decryptionCount,
                "Should decrypt all 6 available candidates");
    }

    /**
     * Test: Stabilization disabled processes all candidates
     *
     * With stabilization OFF, should decrypt all 20 candidates
     * (final result still limited to topK=10)
     */
    @Test
    void testStabilizationDisabledProcessesAllCandidates() {

        when(stab.isEnabled()).thenReturn(false);  // DISABLE stabilization

        byte[] iv = new byte[12];
        byte[] ct = new byte[32];

        // Create 20 raw candidates
        List<EncryptedPoint> raw = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
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
        when(token.getTopK()).thenReturn(10);
        when(token.getEncryptedQuery()).thenReturn(new byte[32]);
        when(token.getIv()).thenReturn(iv);
        when(token.getDimension()).thenReturn(2);
        when(token.getCodes()).thenReturn(new BitSet[]{new BitSet(), new BitSet()});

        when(crypto.decryptQuery(any(), any(), any()))
                .thenReturn(new double[]{0.5, 0.5});
        when(index.lookupCandidateIds(token))
                .thenReturn(
                        raw.stream().map(EncryptedPoint::getId).toList()
                );
        when(index.loadPointIfActive(anyString()))
                .thenAnswer(inv -> {
                    String id = inv.getArgument(0);
                    return raw.stream()
                            .filter(p -> p.getId().equals(id))
                            .findFirst()
                            .orElse(null);
                });

        for (EncryptedPoint p : raw) {
            when(crypto.decryptFromPoint(eq(p), any()))
                    .thenReturn(new double[]{1.0, 1.0});
        }

        List<QueryResult> results = queryService.search(token);

        // VERIFY: Final result is still topK (always limited)
        assertEquals(10, results.size(),
                "Final result should be topK=10 regardless of stabilization");

        // VERIFY: ALL 20 candidates were decrypted (no stabilization limit)
        ArgumentCaptor<EncryptedPoint> captor = ArgumentCaptor.forClass(EncryptedPoint.class);
        verify(crypto, atLeastOnce()).decryptFromPoint(captor.capture(), any());

        int decryptionCount = captor.getAllValues().size();
        assertEquals(20, decryptionCount,
                "Should decrypt all candidates when refinement cap allows");

    }

    @Test
    void testKRelativeFloorIsRespected() {

        when(stab.getMinCandidatesRatio()).thenReturn(2.0); // force floor

        byte[] iv = new byte[12];
        byte[] ct = new byte[32];

        List<EncryptedPoint> raw = new ArrayList<>();
        for (int i = 1; i <= 15; i++) {
            raw.add(new EncryptedPoint(
                    String.valueOf(i), 1, iv, ct, 1, 2, 0, List.of(), List.of()
            ));
        }

        QueryToken token = mock(QueryToken.class);
        when(token.getVersion()).thenReturn(1);
        when(token.getTopK()).thenReturn(5);

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

        ArgumentCaptor<EncryptedPoint> captor =
                ArgumentCaptor.forClass(EncryptedPoint.class);
        verify(crypto, atLeastOnce()).decryptFromPoint(captor.capture(), any());

        int decryptionCount = captor.getAllValues().size();

        // K=5, ratio=2.0 → floor = 10
        assertTrue(decryptionCount >= 10);
    }

}