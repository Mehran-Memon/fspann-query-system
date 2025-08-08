package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.EncryptedPointBuffer;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceTest {

    private CryptoService crypto;
    private KeyLifeCycleService keyService;
    private RocksDBMetadataManager metadataManager;

    @BeforeEach
    void setup() {
        crypto = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        metadataManager = mock(RocksDBMetadataManager.class);
        when(metadataManager.getPointsBaseDir()).thenReturn(System.getProperty("java.io.tmpdir") + "/points");
    }

    @Test
    void testInsert_PersistsBucketsInMetadata() throws Exception {
        String id = "test-1";
        double[] vector = {1.0, 2.0, 3.0, 4.0};
        int dim = vector.length;

        // Deterministic LSH + index with known numTables
        int numBuckets = 32;
        int numTables = 3;
        EvenLSH testLsh = new EvenLSH(dim, numBuckets, 16, 777L);
        SecureLSHIndex testIndex = new SecureLSHIndex(numTables, numBuckets, testLsh);
        EncryptedPointBuffer buffer = mock(EncryptedPointBuffer.class);

        // crypto.encrypt returns the raw encrypted blob; service will wrap it with per-table buckets
        EncryptedPoint enc = new EncryptedPoint(id, 0, new byte[]{1}, new byte[]{2}, 7, dim, Collections.emptyList());
        when(crypto.encrypt(eq(id), eq(vector))).thenReturn(enc);

        SecureLSHIndexService svc = new SecureLSHIndexService(crypto, keyService, metadataManager, testIndex, testLsh, buffer);

        svc.insert(id, vector);

        // compute expected per-table buckets
        List<Integer> expectedBuckets = new ArrayList<>();
        for (int t = 0; t < numTables; t++) expectedBuckets.add(testLsh.getBucketId(vector, t));

        verify(metadataManager).batchUpdateVectorMetadata(argThat(map -> {
            Map<String, String> meta = map.get(id);
            if (meta == null) return false;
            if (!String.valueOf(dim).equals(meta.get("dim"))) return false;
            if (!"7".equals(meta.get("version"))) return false;
            for (int t = 0; t < numTables; t++) {
                String key = "b" + t;
                if (!meta.containsKey(key)) return false;
                if (!String.valueOf(expectedBuckets.get(t)).equals(meta.get(key))) return false;
            }
            return true;
        }));

        verify(metadataManager).saveEncryptedPoint(any(EncryptedPoint.class));
        verify(buffer).add(any(EncryptedPoint.class));
    }

    @Test
    void testInsert_MinimalMetadataPresence() throws Exception {
        // Use default service path (creates its own ctx)
        SecureLSHIndexService svc = new SecureLSHIndexService(crypto, keyService, metadataManager);

        String id = "vec-128";
        double[] vec = new double[128];
        EncryptedPoint enc = new EncryptedPoint(id, 0, new byte[12], new byte[16], 3, 128, Collections.emptyList());
        when(crypto.encrypt(eq(id), eq(vec))).thenReturn(enc);

        svc.insert(id, vec);

        verify(metadataManager).batchUpdateVectorMetadata(argThat(map -> {
            Map<String, String> meta = map.get(id);
            if (meta == null) return false;
            // must at least contain dim, version, and at least one b*
            if (!"128".equals(meta.get("dim"))) return false;
            if (!"3".equals(meta.get("version"))) return false;
            return meta.keySet().stream().anyMatch(k -> k.startsWith("b"));
        }));

        verify(metadataManager).saveEncryptedPoint(any(EncryptedPoint.class));
    }
}
