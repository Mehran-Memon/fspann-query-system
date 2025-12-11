package com.index;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.io.IOException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SecureLSHIndexServicePureTest {

    @Mock CryptoService crypto;
    @Mock KeyLifeCycleService keys;
    @Mock RocksDBMetadataManager meta;
    @Mock PartitionedIndexService engine;
    @Mock EncryptedPointBuffer buffer;

    SecureLSHIndexService svc;

    @BeforeEach
    void init() {
        MockitoAnnotations.openMocks(this);

        // Mock SystemConfig
        SystemConfig config = mock(SystemConfig.class);

        when(meta.getPointsBaseDir()).thenReturn("tmpPoints");

        svc = new SecureLSHIndexService(crypto, keys, meta, engine, buffer, config);
    }


    @Test
    void insert_encrypts_persists_forwards() throws IOException {
        double[] v = {1, 2};

        EncryptedPoint ep = mock(EncryptedPoint.class);
        when(ep.getId()).thenReturn("a");
        when(ep.getVersion()).thenReturn(1);
        when(ep.getVectorLength()).thenReturn(2);

        when(crypto.encrypt("a", v)).thenReturn(ep);

        svc.insert("a", v);

        verify(crypto).encrypt("a", v);
        verify(meta).saveEncryptedPoint(ep);
        verify(buffer).add(ep);
        verify(engine).insert(ep, v);
    }

    @Test
    void lookup_delegatesToEngine() {
        QueryToken tok = mock(QueryToken.class);
        List<EncryptedPoint> ps = Collections.emptyList();  // Use the same empty list type as expected
        when(engine.lookup(tok)).thenReturn(ps);

        List<EncryptedPoint> result = svc.lookup(tok);

        // Compare the size of the lists instead of the exact objects
        assertTrue(result.isEmpty(), "Expected empty list, but got: " + result);
        assertSame(ps, result, "The lists should be the same reference.");
    }
}
