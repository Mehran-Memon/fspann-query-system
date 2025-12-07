package com.index;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceTest {

    private CryptoService crypto;
    private KeyLifeCycleService keySvc;
    private RocksDBMetadataManager metadata;
    private PartitionedIndexService engine;
    private EncryptedPointBuffer buffer;

    private SecureLSHIndexService svc;

    @BeforeEach
    void setup() {
        crypto = mock(CryptoService.class);
        keySvc = mock(KeyLifeCycleService.class);
        metadata = mock(RocksDBMetadataManager.class);
        engine = mock(PartitionedIndexService.class);
        buffer = mock(EncryptedPointBuffer.class);

        when(metadata.getPointsBaseDir())
                .thenReturn(System.getProperty("java.io.tmpdir") + "/points");

        svc = new SecureLSHIndexService(
                crypto,
                keySvc,
                metadata,
                engine,
                buffer
        );
    }

    @Test
    void insert_encrypts_persists_then_forwards_to_engine() throws Exception {
        String id = "v123";
        double[] vec = {1.0, 2.0};

        EncryptedPoint enc = new EncryptedPoint(
                id,
                0,
                new byte[12],
                new byte[32],
                1,
                vec.length,
                Collections.emptyList()
        );

        when(crypto.encrypt(id, vec)).thenReturn(enc);

        svc.insert(id, vec);

        verify(crypto).encrypt(id, vec);

        verify(metadata).batchUpdateVectorMetadata(
                argThat(map -> {
                    var m = map.get(id);
                    return m != null &&
                            m.get("version").equals("1") &&
                            m.get("dim").equals("2");
                })
        );

        verify(metadata).saveEncryptedPoint(enc);
        verify(buffer).add(enc);
        verify(keySvc).incrementOperation();

        verify(engine).insert(enc, vec);
    }

    @Test
    void insertEncryptedPoint_isForbidden() {
        EncryptedPoint ep = new EncryptedPoint(
                "x", 0, new byte[12], new byte[32], 1, 2, Collections.emptyList()
        );

        assertThrows(UnsupportedOperationException.class,
                () -> svc.insert(ep));
    }

    @Test
    void lookup_delegates_to_engine() {
        QueryToken tok = mock(QueryToken.class);
        svc.lookup(tok);
        verify(engine).lookup(tok);
    }

    @Test
    void delete_delegates_to_engine() {
        svc.delete("abc");
        verify(engine).delete("abc");
    }

    @Test
    void vectorCounts_delegate_to_engine() {
        when(engine.getVectorCountForDimension(128)).thenReturn(55);
        assertEquals(55, svc.getVectorCountForDimension(128));
    }
}
