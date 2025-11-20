package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.EncryptedPointBuffer;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceTest {

    private CryptoService crypto;
    private KeyLifeCycleService keyService;
    private RocksDBMetadataManager metadataManager;
    private SecureLSHIndexService.PaperSearchEngine paper;
    private EncryptedPointBuffer buffer;
    private SecureLSHIndexService indexService;

    @BeforeEach
    void setup() {
        crypto = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        metadataManager = mock(RocksDBMetadataManager.class);
        paper = mock(SecureLSHIndexService.PaperSearchEngine.class);
        buffer = mock(EncryptedPointBuffer.class);

        when(metadataManager.getPointsBaseDir())
                .thenReturn(System.getProperty("java.io.tmpdir") + "/points");

        indexService = new SecureLSHIndexService(
                crypto,
                keyService,
                metadataManager,
                paper,      // non-Partitioned mock → "no codes engine" branch
                buffer
        );
    }

    @Test
    void insert_VectorIsEncryptedAndMetadataPersisted() throws Exception {
        String id = "test";
        double[] vector = {1.0, 2.0};
        int dimension = vector.length;

        EncryptedPoint enc = new EncryptedPoint(
                id,
                0,
                new byte[]{1},
                new byte[]{2},
                1,
                dimension,
                Collections.emptyList()
        );
        when(crypto.encrypt(eq(id), eq(vector))).thenReturn(enc);

        UnsupportedOperationException ex = assertThrows(
                UnsupportedOperationException.class,
                () -> indexService.insert(id, vector),
                "Paper mode must reject plaintext insert(id, vector)"
        );
        assertTrue(ex.getMessage().contains("precomputed codes"));

        // We still expect pre-handoff work to have happened:
        verify(crypto).encrypt(eq(id), eq(vector));

        verify(metadataManager).batchUpdateVectorMetadata(argThat(map -> {
            Map<String, String> m = map.get(id);
            return m != null
                    && "1".equals(m.get("version"))
                    && String.valueOf(dimension).equals(m.get("dim"));
        }));

        verify(metadataManager).saveEncryptedPoint(eq(enc));
        verify(keyService).incrementOperation();
        verify(buffer).add(eq(enc));

        // And absolutely no handoff to paper engine
        verify(paper, never()).insert(any(EncryptedPoint.class), any(double[].class));
        verify(paper, never()).insert(any(EncryptedPoint.class));
    }

    @Test
    void insert_PreEncryptedPoint_WritesMetadata() throws Exception {
        var buckets = java.util.Arrays.asList(1, 1, 1, 1);
        EncryptedPoint pt = new EncryptedPoint(
                "vec123",
                buckets.get(0),
                new byte[]{0},
                new byte[]{1},
                99,
                2,
                buckets
        );

        indexService.insert(pt);

        verify(metadataManager).batchUpdateVectorMetadata(argThat(map -> {
            Map<String, String> m = map.get("vec123");
            return m != null
                    && "2".equals(m.get("dim"))
                    && "99".equals(m.get("version"));
        }));

        verify(metadataManager).saveEncryptedPoint(pt);
    }
}
