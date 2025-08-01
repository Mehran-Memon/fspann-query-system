package com.index;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceTest {

    private CryptoService crypto;
    private KeyLifeCycleService keyService;
    private RocksDBMetadataManager metadataManager;
    private SecureLSHIndexService indexService;

    @BeforeEach
    void setup() {
        crypto = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        metadataManager = mock(RocksDBMetadataManager.class);
        indexService = new SecureLSHIndexService(crypto, keyService, metadataManager);
    }

    @Test
    void testInsert_VectorEncryptedCorrectly() throws Exception {
        String id = "test";
        double[] vector = {1.0, 2.0};
        int dimension = vector.length;

        // Inject consistent EvenLSH and Index for controlled shardId
        EvenLSH testLsh = new EvenLSH(dimension, 32, 1); // Only 1 projection to keep it deterministic
        int shardId = testLsh.getBucketId(vector);
        SecureLSHIndex dummyIndex = new SecureLSHIndex(1, 32, testLsh);

        EncryptedPoint encrypted = new EncryptedPoint(id, shardId, new byte[]{1}, new byte[]{2}, 1, dimension, Collections.singletonList(shardId));
        when(crypto.encrypt(eq(id), eq(vector))).thenReturn(encrypted);

        // Inject LSH and Index into the service
        indexService = new SecureLSHIndexService(crypto, keyService, metadataManager, dummyIndex, testLsh, new EncryptedPointBuffer("metadata/points", metadataManager));

        indexService.insert(id, vector);

        // Verify metadata matches injected shardId
        verify(metadataManager).batchUpdateVectorMetadata(argThat(map -> {
            Map<String, String> meta = map.get(id);
            return meta != null &&
                    meta.get("shardId").equals(String.valueOf(shardId)) &&
                    meta.get("dim").equals(String.valueOf(dimension)) &&
                    meta.get("version").equals("1");
        }));

        verify(metadataManager).saveEncryptedPoint(any(EncryptedPoint.class));
    }

    @Test
    void testInsertHandlesDifferentDimensions() throws Exception {
        String id = "335ce2df-1702-45fe-9823-ba9d697f612d";
        double[] vector = new double[128];
        int dimension = 128;

        EvenLSH lsh = new EvenLSH(dimension, 32);
        int shardId = lsh.getBucketId(vector);

        EncryptedPoint encrypted = new EncryptedPoint(id, shardId, new byte[12], new byte[16], 7, dimension, Collections.singletonList(shardId));
        when(crypto.encrypt(eq(id), eq(vector))).thenReturn(encrypted);

        indexService.insert(id, vector);

        verify(metadataManager).batchUpdateVectorMetadata(argThat(map -> {
            Map<String, String> meta = map.get(id);
            return meta != null &&
                    meta.get("shardId").equals(String.valueOf(shardId)) &&
                    meta.get("dim").equals(String.valueOf(dimension)) &&
                    meta.get("version").equals("7");
        }));

        verify(metadataManager).saveEncryptedPoint(encrypted);
    }

    @Test
    void testInsert_UpdatesMetadataAndMarksShardDirty() throws Exception {
        EncryptedPoint pt = new EncryptedPoint("vec123", 4, new byte[]{0}, new byte[]{1}, 99, 2, Collections.singletonList(4));

        indexService.insert(pt);

        verify(metadataManager).batchUpdateVectorMetadata(argThat(map -> {
            Map<String, String> meta = map.get("vec123");
            return meta != null &&
                    meta.get("shardId").equals("4") &&
                    meta.get("dim").equals("2") &&
                    meta.get("version").equals("99");
        }));

        verify(metadataManager).saveEncryptedPoint(pt);
    }
}
