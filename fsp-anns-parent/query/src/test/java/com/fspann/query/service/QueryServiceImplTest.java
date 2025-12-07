package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class QueryServiceImplTest {

    @Mock private IndexService index;
    @Mock private CryptoService crypto;
    @Mock private KeyLifeCycleService keys;
    @Mock private GroundtruthManager gt;

    @Mock private EncryptedPointBuffer buffer;

    private QueryTokenFactory tf;
    private QueryServiceImpl svc;

    @BeforeEach
    void init() {
        MockitoAnnotations.openMocks(this);

        when(index.getPointBuffer()).thenReturn(buffer);
        when(buffer.getLastBatchInsertTimeMs()).thenReturn(1L);
        when(buffer.getTotalFlushedPoints()).thenReturn(1);
        when(buffer.getFlushThreshold()).thenReturn(10);

        tf = new QueryTokenFactory(
                crypto, keys,
                null,     // no LSH
                0,0,      // numTables, probeRange
                2,3,      // ℓ = 2, m = 3
                13L       // seedBase
        );

        svc = new QueryServiceImpl(index, crypto, keys, tf);
    }


    // ----------------------------------------------------------------------
    // TEST 1 — sorting behavior
    // ----------------------------------------------------------------------
    @Test
    void testPartialCandidateSorting() {
        byte[] iv = new byte[12];
        byte[] enc = new byte[32];
        double[] q = {1,2};

        SecretKey k = new SecretKeySpec(new byte[32], "AES");
        KeyVersion kv = new KeyVersion(7, k);

        when(keys.getCurrentVersion()).thenReturn(kv);
        when(keys.getVersion(7)).thenReturn(kv);
        when(crypto.decryptQuery(enc, iv, k)).thenReturn(q);

        EncryptedPoint good = new EncryptedPoint("good", 0, iv, enc, 7, 2, List.of());
        EncryptedPoint bad  = new EncryptedPoint("bad",  0, iv, enc, 7, 2, List.of());

        when(index.lookup(any())).thenReturn(List.of(good, bad));
        when(crypto.decryptFromPoint(good, k)).thenReturn(new double[]{1,2});
        when(crypto.decryptFromPoint(bad, k)).thenReturn(new double[]{99,99});

        QueryToken tok = new QueryToken(
                List.of(),
                new BitSet[]{new BitSet()},
                iv, enc,
                2, 0,
                "dim_2_v7",
                2,
                7
        );

        List<QueryResult> out = svc.search(tok);

        assertEquals("good", out.get(0).getId());
        assertEquals(2, svc.getLastCandTotal());
        assertEquals(2, svc.getLastCandDecrypted());
        assertEquals(2, svc.getLastReturned());
    }


    // ----------------------------------------------------------------------
    // TEST 2 — mismatched version still attempts decrypt(), but filtered out
    // ----------------------------------------------------------------------
    @Test
    void testVersionFilterSkipsMismatched() {
        byte[] iv = new byte[12];
        byte[] enc = new byte[32];
        double[] q = {0,0};

        SecretKey k = new SecretKeySpec(new byte[32], "AES");
        KeyVersion kv = new KeyVersion(2, k);

        when(keys.getVersion(2)).thenReturn(kv);
        when(keys.getCurrentVersion()).thenReturn(kv);
        when(crypto.decryptQuery(enc, iv, k)).thenReturn(q);

        EncryptedPoint ok   = new EncryptedPoint("ok",   0, iv, enc, 2, 2, List.of(1));
        EncryptedPoint skip = new EncryptedPoint("skip", 0, iv, enc, 1, 2, List.of(1));

        when(index.lookup(any())).thenReturn(List.of(ok, skip));
        when(crypto.decryptFromPoint(ok, k)).thenReturn(new double[]{0,0});
        when(crypto.decryptFromPoint(skip, k)).thenReturn(null); // stale/mismatch

        QueryToken tok = new QueryToken(
                List.of(List.of(1)),
                null,
                iv, enc,
                5,
                1,
                "epoch_2_dim_2",
                2,
                2
        );

        List<QueryResult> out = svc.search(tok);

        assertEquals(1, out.size());
        assertEquals("ok", out.get(0).getId());

        // decrypt MUST have been attempted on stale version
        verify(crypto, atLeastOnce()).decryptFromPoint(eq(skip), any());
    }


    // ----------------------------------------------------------------------
    // TEST 3 — no candidates
    // ----------------------------------------------------------------------
    @Test
    void testNoCandidates() {
        byte[] iv = new byte[12];
        byte[] enc = new byte[32];
        double[] q = {0,0};

        SecretKey k = new SecretKeySpec(new byte[32], "AES");
        KeyVersion kv = new KeyVersion(1,k);

        when(keys.getVersion(1)).thenReturn(kv);
        when(keys.getCurrentVersion()).thenReturn(kv);
        when(crypto.decryptQuery(enc, iv, k)).thenReturn(q);

        when(index.lookup(any())).thenReturn(List.of());

        QueryToken tok = new QueryToken(
                List.of(List.of(1)),
                null,
                iv, enc,
                3,
                1,
                "epoch_1_dim_2",
                2,
                1
        );

        List<QueryResult> out = svc.search(tok);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandTotal());
        assertEquals(0, svc.getLastCandKeptVersion());
        assertEquals(0, svc.getLastCandDecrypted());
        assertEquals(0, svc.getLastReturned());
    }


    // ----------------------------------------------------------------------
    // TEST 4 — precision computation, GT empty
    // ----------------------------------------------------------------------
    @Test
    void testPrecisionWhenGTEmpty() {
        byte[] iv = new byte[12];
        byte[] enc = new byte[32];
        double[] q = {0.5,0.6};

        SecretKey k = new SecretKeySpec(new byte[32], "AES");
        KeyVersion kv = new KeyVersion(1,k);

        when(keys.getVersion(1)).thenReturn(kv);
        when(keys.getCurrentVersion()).thenReturn(kv);
        when(crypto.decryptQuery(enc, iv, k)).thenReturn(q);

        EncryptedPoint e1 = new EncryptedPoint("id1",0,iv,enc,1,2,List.of());
        EncryptedPoint e2 = new EncryptedPoint("id2",0,iv,enc,1,2,List.of());

        when(index.lookup(any())).thenReturn(List.of(e1,e2));
        when(crypto.decryptFromPoint(any(), eq(k))).thenReturn(new double[]{0.5,0.6});
        when(gt.getGroundtruth(anyInt(), anyInt())).thenReturn(new int[]{});

        QueryToken tok = new QueryToken(
                List.of(),
                new BitSet[]{new BitSet()},
                iv, enc,
                10,
                0,
                "dim_2_v1",
                2,
                1
        );

        List<QueryEvaluationResult> out = svc.searchWithTopKVariants(tok, 1, gt);

        assertTrue(Double.isNaN(out.get(0).getRatio()));
        assertEquals(0.0, out.get(0).getPrecision());
    }

}
