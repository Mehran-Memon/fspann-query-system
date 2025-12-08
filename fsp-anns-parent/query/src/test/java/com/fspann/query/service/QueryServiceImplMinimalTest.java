package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;

import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class QueryServiceImplMinimalTest {

    CryptoService crypto;
    KeyLifeCycleService keys;
    IndexService index;
    QueryServiceImpl svc;
    SecretKey k;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = mock(IndexService.class);

        k = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, k));

        svc = new QueryServiceImpl(index, crypto, keys, null);
    }

    @Test
    void nullDecryption_skipsPoint() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint bad = new EncryptedPoint("x", 0, iv, ct, 1, 2, List.of());
        when(index.lookup(any())).thenReturn(List.of(bad));

        when(crypto.decryptFromPoint(bad, k)).thenReturn(null);

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "c", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandDecrypted());
    }

    @Test
    void malformedVectors_doNotCrash() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint p = new EncryptedPoint("m", 0, iv, ct, 1, 2, List.of());
        when(index.lookup(any())).thenReturn(List.of(p));

        when(crypto.decryptFromPoint(p, k))
                .thenReturn(new double[]{Double.NaN, 0});

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "ctx", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandDecrypted());
    }
}
