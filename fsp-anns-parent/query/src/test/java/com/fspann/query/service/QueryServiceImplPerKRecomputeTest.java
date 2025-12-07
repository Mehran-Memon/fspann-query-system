package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class QueryServiceImplPerKRecomputeTest {

    private IndexService index;
    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private GroundtruthManager gt;
    private QueryTokenFactory tf;
    private QueryServiceImpl svc;

    @BeforeEach
    void init() {
        index = mock(IndexService.class);
        crypto = mock(CryptoService.class);
        keys = mock(KeyLifeCycleService.class);
        gt = mock(GroundtruthManager.class);

        tf = new QueryTokenFactory(crypto, keys, null, 0,0,2,3,13L);

        svc = new QueryServiceImpl(index, crypto, keys, tf);

        when(index.getPointBuffer()).thenReturn(mock(EncryptedPointBuffer.class));

        SecretKey k = new SecretKeySpec(new byte[32],"AES");
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1,k));
        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1,k));
        when(crypto.decryptQuery(any(), any(), eq(k))).thenReturn(new double[]{1,2});
    }

    @Test
    void noPerKRecompute() {
        QueryToken tok = new QueryToken(List.of(),
                new BitSet[]{new BitSet()}, new byte[12], new byte[32], 100, 0, "dim_2_v1",2,1);

        svc.searchWithTopKVariants(tok,0,gt);

        // IndexService.lookup should have been called only with the max-K token
        verify(index, atLeastOnce()).lookup(argThat(t -> t.getTopK()==100));
    }

    @Test
    void searchDynamicTopK() {
        SecretKey k = new SecretKeySpec(new byte[32],"AES");
        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1,k));
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1,k));

        EncryptedPoint a = new EncryptedPoint("good",0,new byte[12],new byte[32],1,2,List.of());
        EncryptedPoint b = new EncryptedPoint("bad",0,new byte[12],new byte[32],1,2,List.of());

        when(index.lookup(any())).thenReturn(List.of(a,b));
        when(crypto.decryptFromPoint(eq(a),eq(k))).thenReturn(new double[]{1,2});
        when(crypto.decryptFromPoint(eq(b),eq(k))).thenReturn(new double[]{99,99});

        QueryToken tok = new QueryToken(List.of(),
                new BitSet[]{new BitSet()}, new byte[12],new byte[32],100,0,"dim_2_v1",2,1);

        List<QueryResult> out = svc.search(tok);

        assertEquals("good", out.get(0).getId());
        assertEquals(2, svc.getLastCandTotal());
        assertEquals(2, svc.getLastCandDecrypted());
        assertEquals(2, svc.getLastReturned());
    }

    @Test
    void ratioIsNaNWhenGTEmpty() {
        SecretKey k = new SecretKeySpec(new byte[32],"AES");
        KeyVersion kv = new KeyVersion(1,k);
        when(keys.getCurrentVersion()).thenReturn(kv);
        when(keys.getVersion(1)).thenReturn(kv);
        when(gt.getGroundtruth(anyInt(), anyInt())).thenReturn(new int[]{});

        EncryptedPoint e = new EncryptedPoint("0",0,new byte[12],new byte[32],1,2,List.of());
        when(index.lookup(any())).thenReturn(List.of(e));
        when(crypto.decryptFromPoint(eq(e), eq(k))).thenReturn(new double[]{1,2});

        QueryToken tok = new QueryToken(List.of(),
                new BitSet[]{new BitSet()},
                new byte[12],new byte[32],1,0,"dim_2_v1",2,1);

        List<QueryEvaluationResult> out = svc.searchWithTopKVariants(tok,0,gt);

        assertTrue(Double.isNaN(out.get(0).getRatio()));
        assertEquals(0.0, out.get(0).getPrecision());
    }
}
