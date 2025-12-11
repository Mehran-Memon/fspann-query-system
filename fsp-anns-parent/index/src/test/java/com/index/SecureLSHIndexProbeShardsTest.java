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

class SecureLSHIndexProbeShardsTest {

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

        // Create mock SystemConfig
        SystemConfig config = mock(SystemConfig.class);
        when(config.getLsh()).thenReturn(new SystemConfig.LshConfig());

        svc = new SecureLSHIndexService(crypto, keySvc, metadata, engine, buffer, config);
    }


    @AfterEach
    void cleanup() {
        System.clearProperty("probe.shards");
    }

    @Test
    void lookupIgnoresProbeShardsProperty() {
        QueryToken token = mock(QueryToken.class);

        when(engine.lookup(token)).thenReturn(Collections.singletonList(
                new EncryptedPoint("a", 0, new byte[12], new byte[16], 1, 4, Collections.emptyList())
        ));

        System.setProperty("probe.shards", "999999");

        var out = svc.lookup(token);

        assertEquals(1, out.size());
        verify(engine).lookup(token);
    }
}
