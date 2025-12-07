package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SecureLSHIndexServicePureTest {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private SecureLSHIndexService index;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = new SecureLSHIndexService(crypto, keys, null, null, null);
    }

    private QueryToken dummyToken() {
        return new QueryToken(
                List.of(),      // tables
                null,           // codes
                new byte[12],   // iv
                new byte[16],   // enc query
                5,              // topK
                1,              // version
                "ctx",
                2,              // dim
                1               // shard
        );
    }

    @Test
    void insertAndLookup_returnsInsertedPoints() {
        double[] v1 = {1,2};
        double[] v2 = {3,4};

        index.insert("a", v1);
        index.insert("b", v2);

        QueryToken tok = dummyToken();
        List<EncryptedPoint> pts = index.lookup(tok);

        assertEquals(2, pts.size());
        assertTrue(pts.stream().anyMatch(p -> p.getId().equals("a")));
        assertTrue(pts.stream().anyMatch(p -> p.getId().equals("b")));
    }

    @Test
    void emptyIndex_returnsEmptyList() {
        QueryToken tok = dummyToken();
        assertTrue(index.lookup(tok).isEmpty());
    }
}
