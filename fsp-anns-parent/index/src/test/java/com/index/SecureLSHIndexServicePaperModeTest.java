package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.EncryptedPointBuffer;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.BitSet;
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

        // Provide a stable key version for metadata (even if not used heavily here)
        SecretKey sk = new SecretKeySpec(new byte[32], "AES");
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(1, sk));

        when(meta.getPointsBaseDir())
                .thenReturn(System.getProperty("java.io.tmpdir") + "/points");

        svc = new SecureLSHIndexService(
                crypto, keySvc, meta,
                paper,
                buffer
        );
    }

    private static QueryToken simpleToken(int topK, int dim) {
        List<List<Integer>> tableBuckets = List.of(List.of(1));
        BitSet[] codes = new BitSet[0];

        return new QueryToken(
                tableBuckets,
                codes,
                new byte[12],
                new byte[32],
                topK,
                1,
                "epoch_1_dim_" + dim,
                dim,
                1
        );
    }

    @Test
    void insert_encrypts_persists_buffers_then_throws_without_codes_engine() throws IOException {
        String id = "42";
        double[] vec = new double[]{0.1, 0.2};

        EncryptedPoint enc = new EncryptedPoint(
                id,
                0,
                new byte[12],
                new byte[32],
                1,
                vec.length,
                null
        );
        when(crypto.encrypt(eq(id), eq(vec))).thenReturn(enc);

        UnsupportedOperationException ex =
                assertThrows(UnsupportedOperationException.class, () -> svc.insert(id, vec));
        assertTrue(ex.getMessage().contains("precomputed codes"));

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

        verify(paper, never()).insert(any(EncryptedPoint.class), any(double[].class));
        verify(paper, never()).insert(any(EncryptedPoint.class));
    }

    @Test
    void batchInsert_encrypts_persists_each_then_throws_on_first_handoff_without_codes_engine() throws IOException {
        List<String> ids = List.of("0", "1");
        List<double[]> vecs = List.of(new double[]{0.1, 0.2}, new double[]{0.3, 0.4});

        EncryptedPoint enc0 = new EncryptedPoint("0", 0, new byte[12], new byte[32], 1, 2, null);
        EncryptedPoint enc1 = new EncryptedPoint("1", 0, new byte[12], new byte[32], 1, 2, null);
        when(crypto.encrypt(eq("0"), eq(vecs.get(0)))).thenReturn(enc0);
        when(crypto.encrypt(eq("1"), eq(vecs.get(1)))).thenReturn(enc1);

        UnsupportedOperationException ex =
                assertThrows(UnsupportedOperationException.class, () -> svc.batchInsert(ids, vecs));
        assertTrue(ex.getMessage().contains("precomputed codes"));

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

        verify(paper, never()).insert(any(EncryptedPoint.class), any(double[].class));
        verify(paper, never()).insert(any(EncryptedPoint.class));
    }

    @Test
    void lookup_delegates_to_paper() {
        QueryToken t = simpleToken(5, 2);

        List<EncryptedPoint> canned = List.of(
                new EncryptedPoint("a", 0, new byte[12], new byte[32], 1, 2, List.of()),
                new EncryptedPoint("b", 0, new byte[12], new byte[32], 1, 2, List.of())
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
    void updateCachedPoint_isRemembered() throws Exception {
        EncryptedPoint ep = new EncryptedPoint(
                "cached",
                0,
                new byte[12],
                new byte[32],
                1,
                2,
                List.of()
        );

        when(meta.loadEncryptedPoint("cached")).thenReturn(ep);

        svc.updateCachedPoint(ep);
        EncryptedPoint again = svc.getEncryptedPoint("cached");
        assertNotNull(again);
        assertEquals("cached", again.getId());
    }
}
