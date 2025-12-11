package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import org.junit.jupiter.api.*;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class QueryServiceImplPureTest {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private IndexService index;
    private QueryServiceImpl svc;
    private SecretKey k;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys = mock(KeyLifeCycleService.class);
        index = mock(IndexService.class);

        // Create a mock for SystemConfig
        SystemConfig cfg = mock(SystemConfig.class);

        // global non-null AES key
        k = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, k));

        // Update the constructor to include the cfg parameter
        svc = new QueryServiceImpl(index, crypto, keys, null, cfg);
    }


    @Test
    void versionFilter_skipsMismatchedVersion() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint ok   = new EncryptedPoint("ok",   0, iv, ct, 1, 2, List.of());
        EncryptedPoint skip = new EncryptedPoint("skip", 0, iv, ct, 2, 2, List.of());

        when(index.lookup(any())).thenReturn(List.of(ok, skip));

        when(crypto.decryptFromPoint(ok, k))
                .thenReturn(new double[]{0, 0});

        when(crypto.decryptFromPoint(skip, k))
                .thenReturn(null);

        QueryToken tok = new QueryToken(
                List.of(), null,
                iv, ct,
                5, 1, "ctx", 2, 1
        );

        List<QueryResult> out = svc.search(tok);

        assertEquals(1, out.size());
        assertEquals("ok", out.get(0).getId());
    }

    @Test
    void sorting_ordersByDistance() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint p1 = new EncryptedPoint("a", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p2 = new EncryptedPoint("b", 0, iv, ct, 1, 2, List.of());

        when(index.lookup(any())).thenReturn(List.of(p1, p2));

        when(crypto.decryptFromPoint(p1, k))
                .thenReturn(new double[]{1, 1});      // dist^2 = 2

        when(crypto.decryptFromPoint(p2, k))
                .thenReturn(new double[]{0.1, 0.1});  // closer

        QueryToken tok = new QueryToken(List.of(), null, iv, ct, 5, 1, "ctx", 2, 1);

        List<QueryResult> out = svc.search(tok);

        assertEquals("b", out.get(0).getId());
        assertEquals("a", out.get(1).getId());
    }

    @Test
    void emptyIndex_returnsEmptyList() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        when(index.lookup(any())).thenReturn(List.of());

        QueryToken tok = new QueryToken(List.of(), null, iv, ct, 5, 1, "ctx", 2, 1);

        List<QueryResult> out = svc.search(tok);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandTotal());
        assertEquals(0, svc.getLastReturned());
    }
}
