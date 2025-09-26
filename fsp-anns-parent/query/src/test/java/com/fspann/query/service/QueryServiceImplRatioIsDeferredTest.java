package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QueryServiceImplRatioIsDeferredTest {

    @Test
    void ratioIsNaNButPrecisionIsComputed() throws Exception {
        IndexService index = mock(IndexService.class);
        CryptoService crypto = mock(CryptoService.class);
        KeyLifeCycleService keySvc = mock(KeyLifeCycleService.class);
        GroundtruthManager gt = mock(GroundtruthManager.class);

        QueryServiceImpl svc = new QueryServiceImpl(index, crypto, keySvc);

        // Buffer stats (if consulted)
        EncryptedPointBuffer buf = mock(EncryptedPointBuffer.class);
        when(buf.getLastBatchInsertTimeMs()).thenReturn(0L);
        when(buf.getTotalFlushedPoints()).thenReturn(0);
        when(buf.getFlushThreshold()).thenReturn(0);
        when(index.getPointBuffer()).thenReturn(buf);

        byte[] iv = new byte[12];
        byte[] enc = new byte[32];
        double[] q = new double[]{1.0, 2.0};
        String ctx = "epoch_1_dim_2";

        SecretKey k = new SecretKeySpec(new byte[32], "AES");
        when(keySvc.getVersion(1)).thenReturn(new KeyVersion(1, k));
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(crypto.decryptQuery(enc, iv, k)).thenReturn(q);

        // Two retrieved ids: 0 and 1
        when(index.lookup(any())).thenReturn(List.of(
                new EncryptedPoint("0", 0, iv, enc, 1, 2, List.of()),
                new EncryptedPoint("1", 0, iv, enc, 1, 2, List.of())
        ));
        when(crypto.decryptFromPoint(any(), eq(k))).thenReturn(new double[]{0.0, 0.0});

        // GT has id 0 as the only relevant at k=1
        when(gt.getGroundtruth(eq(0), anyInt())).thenReturn(new int[]{0});

        QueryToken token = new QueryToken(
                List.of(List.of(1)),
                iv, enc, q, 1, 1, ctx, 2, 1
        );

        List<QueryEvaluationResult> res = svc.searchWithTopKVariants(token, 0, gt);
        QueryEvaluationResult k1 = res.stream().filter(r -> r.getTopKRequested() == 1).findFirst().orElseThrow();

        // Ratio is computed higher up (ForwardSecureANNSystem) → NaN here
        assertTrue(Double.isNaN(k1.getRatio()));
        // Precision@1 stored in 'precision' is 1.0 (id 0 retrieved among the top1)
        assertEquals(1.0, k1.getPrecision(), 1e-9);
    }
}
