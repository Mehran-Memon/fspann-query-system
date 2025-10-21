package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.common.EncryptedPointBuffer;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.List;
import java.util.Map;

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
    void insert_encrypts_persists_buffers_then_throws_without_codes_engine() throws IOException {
        String id = "42";
        double[] vec = new double[]{0.1, 0.2};

        EncryptedPoint enc = new EncryptedPoint(id, /*shard*/ 0, new byte[12], new byte[32], 1, vec.length, /*perTable*/ null);
        when(crypto.encrypt(eq(id), eq(vec))).thenReturn(enc);

        UnsupportedOperationException ex =
                assertThrows(UnsupportedOperationException.class, () -> svc.insert(id, vec));
        assertTrue(ex.getMessage().contains("requires precomputed codes"));

        // Work that happens before the guarded handoff:
        verify(crypto).encrypt(eq(id), eq(vec));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Map<String, String>>> metaCap = ArgumentCaptor.forClass(Map.class);
        verify(meta).batchUpdateVectorMetadata(metaCap.capture());
        Map<String, Map<String, String>> md = metaCap.getValue();
        assertTrue(md.containsKey(id));
        assertEquals("1", md.get(id).get("version"));
        assertEquals(String.valueOf(vec.length), md.get(id).get("dim"));
        assertTrue(md.get(id).keySet().stream().noneMatch(k -> k.startsWith("b")),
                "Paper mode must not persist legacy per-table bucket metadata");

        verify(meta).saveEncryptedPoint(eq(enc));
        verify(keySvc).incrementOperation();
        verify(buffer).add(eq(enc));

        // The guarded handoff prevents calling paper.insert(...)
        verify(paper, never()).insert(any(EncryptedPoint.class), any(double[].class));
        verify(paper, never()).insert(any(EncryptedPoint.class));
    }

    @Test
    void batchInsert_encrypts_persists_each_then_throws_on_first_handoff_without_codes_engine() throws IOException {
        // Prepare two ids/vectors
        List<String> ids = List.of("0", "1");
        List<double[]> vecs = List.of(new double[]{0.1, 0.2}, new double[]{0.3, 0.4});

        EncryptedPoint enc0 = new EncryptedPoint("0", 0, new byte[12], new byte[32], 1, 2, null);
        EncryptedPoint enc1 = new EncryptedPoint("1", 0, new byte[12], new byte[32], 1, 2, null);
        when(crypto.encrypt(eq("0"), eq(vecs.get(0)))).thenReturn(enc0);
        when(crypto.encrypt(eq("1"), eq(vecs.get(1)))).thenReturn(enc1);

        // The first handoff to a non-codes engine triggers the guard
        UnsupportedOperationException ex =
                assertThrows(UnsupportedOperationException.class, () -> svc.batchInsert(ids, vecs));
        assertTrue(ex.getMessage().contains("requires precomputed codes"));

        // We still expect pre-handoff work for the first element
        verify(crypto).encrypt("0", vecs.get(0));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Map<String, String>>> metaCap = ArgumentCaptor.forClass(Map.class);
        verify(meta).batchUpdateVectorMetadata(metaCap.capture());
        Map<String, Map<String, String>> md = metaCap.getValue();
        assertTrue(md.containsKey("0"));
        assertEquals("1", md.get("0").get("version"));
        assertEquals("2", md.get("0").get("dim"));

        verify(meta).saveEncryptedPoint(eq(enc0));
        verify(keySvc).incrementOperation();
        verify(buffer).add(eq(enc0));

        // No paper calls (guarded)
        verify(paper, never()).insert(any(EncryptedPoint.class), any(double[].class));
        verify(paper, never()).insert(any(EncryptedPoint.class));

        // Depending on your implementation, the second item may not be attempted after the first throws.
        // If your service ever changes to continue-on-error, add verifies for "1" similar to above.
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
