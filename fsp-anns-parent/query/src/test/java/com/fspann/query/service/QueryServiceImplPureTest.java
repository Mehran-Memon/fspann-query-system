package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryServiceImplPureTest {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private IndexService index;
    private QueryTokenFactory tf;
    private QueryServiceImpl svc;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = mock(IndexService.class);

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, null));
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1, null));

        tf = new QueryTokenFactory(crypto, keys, null, 0, 0, 2, 3, 13L);
        svc = new QueryServiceImpl(index, crypto, keys, tf);
    }

    @Test
    void versionFilter_skipsMismatchedVersion() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];
        double[] q = {0, 0};

        when(crypto.decryptQuery(ct, iv, null)).thenReturn(q);

        EncryptedPoint ok   = new EncryptedPoint("ok",   0, iv, ct, 1, 2, List.of());
        EncryptedPoint skip = new EncryptedPoint("skip", 0, iv, ct, 2, 2, List.of());

        when(index.lookup(any())).thenReturn(List.of(ok, skip));
        when(crypto.decryptFromPoint(ok, null)).thenReturn(new double[]{0, 0});
        when(crypto.decryptFromPoint(skip, null)).thenReturn(null);

        var tok = new QueryToken(List.of(), null, iv, ct, 3, 1, "ctx", 2, 1);

        List<QueryResult> out = svc.search(tok);
        assertEquals(1, out.size());
        assertEquals("ok", out.get(0).getId());
    }

    @Test
    void sorting_ordersByL2Distance() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];
        double[] q = {0, 0};

        when(crypto.decryptQuery(ct, iv, null)).thenReturn(q);

        EncryptedPoint p1 = new EncryptedPoint("a", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p2 = new EncryptedPoint("b", 0, iv, ct, 1, 2, List.of());

        when(index.lookup(any())).thenReturn(List.of(p1, p2));

        when(crypto.decryptFromPoint(p1, null)).thenReturn(new double[]{1, 1});   // dist = 2
        when(crypto.decryptFromPoint(p2, null)).thenReturn(new double[]{0.1, 0.1}); // dist < 2

        var tok = new QueryToken(List.of(), null, iv, ct, 5, 1, "ctx", 2, 1);
        var out = svc.search(tok);

        assertEquals("b", out.get(0).getId());
        assertEquals("a", out.get(1).getId());
    }

    @Test
    void emptyIndex_returnsEmpty() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];
        double[] q = {0, 0};

        when(crypto.decryptQuery(ct, iv, null)).thenReturn(q);
        when(index.lookup(any())).thenReturn(List.of());

        var tok = new QueryToken(List.of(), null, iv, ct, 5, 1, "ctx", 2, 1);
        var out = svc.search(tok);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandTotal());
        assertEquals(0, svc.getLastReturned());
    }
}
