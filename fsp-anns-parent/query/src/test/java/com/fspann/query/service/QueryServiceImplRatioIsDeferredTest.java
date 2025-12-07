package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class QueryServiceImplRatioIsDeferredTest {

    @Test
    void ratioIsNaNButPrecisionIsComputed() {

        IndexService index = mock(IndexService.class);
        CryptoService crypto = mock(CryptoService.class);
        KeyLifeCycleService keys = mock(KeyLifeCycleService.class);
        GroundtruthManager gt = mock(GroundtruthManager.class);

        QueryTokenFactory tf = new QueryTokenFactory(crypto, keys, null,0,0,2,3,13L);
        QueryServiceImpl svc = new QueryServiceImpl(index, crypto, keys, tf);

        when(index.getPointBuffer()).thenReturn(mock(EncryptedPointBuffer.class));

        byte[] iv = new byte[12];
        byte[] enc = new byte[32];
        double[] q = {1,2};

        SecretKey k = new SecretKeySpec(new byte[32],"AES");
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1,k));
        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1,k));
        when(crypto.decryptQuery(enc,iv,k)).thenReturn(q);

        EncryptedPoint a = new EncryptedPoint("0",0,iv,enc,1,2,List.of());
        EncryptedPoint b = new EncryptedPoint("1",0,iv,enc,1,2,List.of());
        when(index.lookup(any())).thenReturn(List.of(a,b));

        when(crypto.decryptFromPoint(any(), eq(k))).thenReturn(new double[]{0,0});

        when(gt.getGroundtruth(eq(0), anyInt())).thenReturn(new int[]{0});

        QueryToken tok = new QueryToken(List.of(),
                new BitSet[]{new BitSet()}, iv, enc,1,0,"dim_2_v1",2,1);

        List<QueryEvaluationResult> out = svc.searchWithTopKVariants(tok, 0, gt);

        QueryEvaluationResult k1 = out.stream()
                .filter(r -> r.getTopKRequested()==1)
                .findFirst()
                .orElseThrow();

        assertTrue(Double.isNaN(k1.getRatio()));
        assertEquals(1.0, k1.getPrecision());
    }
}
