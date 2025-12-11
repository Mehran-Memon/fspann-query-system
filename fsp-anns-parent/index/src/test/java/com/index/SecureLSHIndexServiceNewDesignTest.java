package com.index;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.*;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceNewDesignTest {

    private CryptoService crypto;
    private KeyLifeCycleService keySvc;
    private RocksDBMetadataManager metadata;
    private EncryptedPointBuffer buffer;
    private PartitionedIndexService engine;
    private SecureLSHIndexService svc;

    @BeforeEach
    void setup() {
        crypto = mock(CryptoService.class);
        keySvc = mock(KeyLifeCycleService.class);
        metadata = mock(RocksDBMetadataManager.class);
        buffer = mock(EncryptedPointBuffer.class);
        engine = mock(PartitionedIndexService.class);

        // Mock SystemConfig
        SystemConfig config = mock(SystemConfig.class);

        svc = new SecureLSHIndexService(crypto, keySvc, metadata, engine, buffer, config);
    }

    @Test
    void getVectorCount_defaultsToZero() {
        when(engine.getTotalVectorCount()).thenReturn(0);
        assertEquals(0, svc.getIndexedVectorCount());
    }

    @Test
    void delete_noop_isDelegated() {
        svc.delete("x1");
        verify(engine).delete("x1");
    }

    @Test
    void lookupReturnsEngineOutput() {
        QueryToken tok = mock(QueryToken.class);
        when(engine.lookup(tok)).thenReturn(Collections.emptyList());
        assertTrue(svc.lookup(tok).isEmpty());
    }
}
