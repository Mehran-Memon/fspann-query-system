package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyVersion;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.IndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class QueryServiceImplTest {
    @Mock
    private IndexService indexSvc;

    @Mock
    private CryptoService crypto;

    @Mock
    private com.fspann.common.KeyLifeCycleService keySvc;

    private QueryServiceImpl service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        service = new QueryServiceImpl(indexSvc, crypto, keySvc);
    }

    @Test
    void searchDecryptsQueryAndSorts() throws Exception {
        byte[] iv = new byte[12];
        Arrays.fill(iv, (byte) 1);

        byte[] encQuery = new byte[32];
        Arrays.fill(encQuery, (byte) 2);

        double[] plaintextQuery = new double[]{1.0, 2.0};
        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 2, 1, "epoch_v7");

        SecretKey qKey = new SecretKeySpec(new byte[32], "AES");
        // Use same iv/encQuery for versioned KeyVersion
        KeyVersion queryVer = new KeyVersion(7, qKey, iv, encQuery);
        KeyVersion currentVer = new KeyVersion(8, qKey, iv, encQuery);

        doNothing().when(keySvc).rotateIfNeeded();
        when(keySvc.getVersion(7)).thenReturn(queryVer);
        when(keySvc.getCurrentVersion()).thenReturn(currentVer);

        double[] qv = new double[]{1, 0, 0};
        when(crypto.decryptQuery(eq(encQuery), eq(iv), eq(currentVer.getSecretKey())))
                .thenReturn(qv);

        EncryptedPoint p1 = new EncryptedPoint("id1", 0, iv, encQuery, queryVer.getVersion());
        EncryptedPoint p2 = new EncryptedPoint("id2", 0, iv, encQuery, queryVer.getVersion());
        when(indexSvc.lookup(token)).thenReturn(List.of(p1, p2));

        when(crypto.decryptFromPoint(p1, currentVer.getSecretKey())).thenReturn(new double[]{0, 0, 0});
        when(crypto.decryptFromPoint(p2, currentVer.getSecretKey())).thenReturn(new double[]{1, 0, 0});

        List<QueryResult> results = service.search(token);

        verify(keySvc).rotateIfNeeded();
        verify(crypto).decryptQuery(eq(encQuery), eq(iv), eq(currentVer.getSecretKey()));
        verify(indexSvc).lookup(token);
        verify(crypto).decryptFromPoint(p1, currentVer.getSecretKey());
        verify(crypto).decryptFromPoint(p2, currentVer.getSecretKey());

        assertEquals("id2", results.get(0).getId());
        assertEquals(0.0, results.get(0).getDistance(), 1e-9);
        assertEquals("id1", results.get(1).getId());
        assertEquals(1.0, results.get(1).getDistance(), 1e-9);
    }

    @Test
    void searchLimitsToTopK() throws Exception {
        byte[] iv = new byte[12];
        Arrays.fill(iv, (byte) 1);

        byte[] encQuery = new byte[32];
        Arrays.fill(encQuery, (byte) 2);

        double[] plaintextQuery = new double[]{1.0, 2.0, 3.0};
        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 2, 1, "epoch_v7");

        SecretKey qKey = new SecretKeySpec(new byte[32], "AES");
        KeyVersion queryVer = new KeyVersion(7, qKey, iv, encQuery);
        doNothing().when(keySvc).rotateIfNeeded();
        when(keySvc.getVersion(7)).thenReturn(queryVer);
        when(keySvc.getCurrentVersion()).thenReturn(queryVer);

        EncryptedPoint p1 = new EncryptedPoint("id1", 0, iv, encQuery, queryVer.getVersion());
        EncryptedPoint p2 = new EncryptedPoint("id2", 0, iv, encQuery, queryVer.getVersion());
        EncryptedPoint p3 = new EncryptedPoint("id3", 0, iv, encQuery, queryVer.getVersion());
        when(indexSvc.lookup(token)).thenReturn(List.of(p1, p2, p3));

        when(crypto.decryptQuery(eq(encQuery), eq(iv), eq(queryVer.getSecretKey())))
                .thenReturn(plaintextQuery);

        // Adjust vectors to ensure correct order: id1, id3, id2
        when(crypto.decryptFromPoint(p1, queryVer.getSecretKey())).thenReturn(new double[]{1.0, 2.0, 3.0}); // Distance = 0
        when(crypto.decryptFromPoint(p3, queryVer.getSecretKey())).thenReturn(new double[]{1.1, 2.1, 3.1}); // Distance ≈ 0.173
        when(crypto.decryptFromPoint(p2, queryVer.getSecretKey())).thenReturn(new double[]{0.0, 0.0, 0.0}); // Distance = sqrt(14) ≈ 3.741

        List<QueryResult> results = service.search(token);

        assertEquals(2, results.size(), "Should limit to topK=2");
        assertEquals("id1", results.get(0).getId(), "First result should be closest");
        assertEquals(0.0, results.get(0).getDistance(), 1e-9, "Distance for id1");
        assertEquals("id3", results.get(1).getId(), "Second result");
        assertEquals(0.17320508075688773, results.get(1).getDistance(), 1e-9, "Distance for id3");
    }}
