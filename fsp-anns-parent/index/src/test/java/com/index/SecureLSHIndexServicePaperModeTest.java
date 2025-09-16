package com.index;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.SecureLSHIndexService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SecureLSHIndexServicePaperModeTest {

    private CryptoService crypto;
    private KeyLifeCycleService keySvc;
    private RocksDBMetadataManager meta;
    private SecureLSHIndexService.PaperSearchEngine paper;
    private EncryptedPointBuffer buffer;

    private SecureLSHIndexService svc;

    @BeforeEach
    void setup() {
        crypto = mock(CryptoService.class);
        keySvc = mock(KeyLifeCycleService.class);
        meta   = mock(RocksDBMetadataManager.class);
        paper  = mock(SecureLSHIndexService.PaperSearchEngine.class);
        buffer = mock(EncryptedPointBuffer.class);

        // Provide a stable key version for metadata
        SecretKey sk = new SecretKeySpec(new byte[32], "AES");
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(1, sk));

        // Construct service explicitly in "paper" shape (paper engine present)
        svc = new SecureLSHIndexService(
                crypto, keySvc, meta,
                paper,
                /*legacyIndex*/ null,
                /*legacyLSH*/ null,
                buffer,
                /*defaultNumBuckets*/ 32,
                /*defaultNumTables*/ 4
        );
    }

    @Test
    void insert_encrypts_persists_buffers_and_delegates_toPaper() throws IOException {
        String id = "42";
        double[] vec = new double[]{0.1, 0.2};

        // The crypto layer produces an EncryptedPoint that carries version/dim/iv/ciphertext
        EncryptedPoint enc = new EncryptedPoint(id, /*shard*/ 0, new byte[12], new byte[32], 1, vec.length, /*perTable*/ null);
        when(crypto.encrypt(eq(id), eq(vec))).thenReturn(enc);

        // Call
        svc.insert(id, vec);

        // 1) encryption happened
        verify(crypto).encrypt(eq(id), eq(vec));

        // 2) metadata persisted with version & dim
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Map<String, String>>> metaCap = ArgumentCaptor.forClass(Map.class);
        verify(meta).batchUpdateVectorMetadata(metaCap.capture());
        Map<String, Map<String, String>> md = metaCap.getValue();
        assertTrue(md.containsKey(id));
        assertEquals("1", md.get(id).get("version"));
        assertEquals(String.valueOf(vec.length), md.get(id).get("dim"));

        // 3) point saved
        verify(meta).saveEncryptedPoint(eq(enc));

        // 4) key op counted
        verify(keySvc).incrementOperation();

        // 5) buffered write
        verify(buffer).add(eq(enc));

        // 6) paper-engine placement with plaintext vector
        verify(paper).insert(eq(enc), eq(vec));
    }

    @Test
    void lookup_delegates_to_paper() {
        QueryToken t = new QueryToken(
                java.util.List.of(java.util.List.of(1)),
                new byte[12],
                new byte[32],
                new double[]{0.0, 0.0},
                5, 1, "epoch_1_dim_2", 2, 1
        );
        List<EncryptedPoint> canned = java.util.List.of(
                new EncryptedPoint("a", 0, new byte[12], new byte[32], 1, 2, java.util.List.of()),
                new EncryptedPoint("b", 0, new byte[12], new byte[32], 1, 2, java.util.List.of())
        );
        when(paper.lookup(eq(t))).thenReturn(canned);

        List<EncryptedPoint> out = svc.lookup(t);
        assertEquals(canned, out);
        verify(paper).lookup(eq(t));
    }

    @Test
    void getVectorCountForDimension_delegates_to_paper() {
        when(paper.getVectorCountForDimension(128)).thenReturn(777);
        assertEquals(777, svc.getVectorCountForDimension(128));
        verify(paper).getVectorCountForDimension(128);
    }

    @Test
    void delete_delegates_to_paper() {
        svc.delete("deadbeef");
        verify(paper).delete("deadbeef");
    }

    @Test
    void updateCachedPoint_isRemembered() {
        EncryptedPoint ep = new EncryptedPoint("cached", 0, new byte[12], new byte[32], 1, 2, java.util.List.of());
        svc.updateCachedPoint(ep);
        EncryptedPoint again = svc.getEncryptedPoint("cached");
        assertNotNull(again);
        assertEquals("cached", again.getId());
    }
}
