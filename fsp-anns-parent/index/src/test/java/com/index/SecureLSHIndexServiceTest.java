package com.index;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.common.RocksDBMetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Mockito;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceTest {

    @Mock private SecureLSHIndex index;
    @Mock private AesGcmCryptoService crypto;
    @Mock private KeyLifeCycleService keyService;
    @Mock private EvenLSH lsh;
    @Mock private RocksDBMetadataManager metadataManager;
    @Mock private EncryptedPointBuffer buffer;

    @Captor private ArgumentCaptor<Map<String, Map<String, String>>> metadataCaptor;

    private SecureLSHIndexService service;
    private final byte[] testIv = new byte[12];
    private final byte[] testCiphertext = new byte[32];

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.reset(metadataManager, crypto, index, keyService, lsh, buffer);
        service = new SecureLSHIndexService(crypto, keyService, metadataManager, index, lsh, buffer);
    }

    @Test
    void testInsert_VectorEncryptedCorrectly() throws IOException {
        double[] vector = {1.0, 2.0};
        String id = "test";

        when(lsh.getBucketId(eq(vector))).thenReturn(1);
        EncryptedPoint template = new EncryptedPoint(id, 0, testIv, testCiphertext, 1, 2, null);
        when(crypto.encrypt(eq(id), eq(vector))).thenReturn(template);

        service.insert(id, vector);

        verify(crypto).encrypt(eq(id), eq(vector));
        verify(index).addPoint(argThat(pt ->
                pt.getId().equals("test") &&
                        pt.getShardId() == 1 &&
                        pt.getVersion() == 1 &&
                        pt.getVectorLength() == 2 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), testCiphertext) &&
                        pt.getBuckets().equals(Collections.singletonList(1))
        ));
        verify(index).markShardDirty(1);

        verify(metadataManager).batchUpdateVectorMetadata(metadataCaptor.capture());
        Map<String, Map<String, String>> capturedMeta = metadataCaptor.getValue();
        System.out.println("Captured metadata: " + capturedMeta);
        Map<String, Map<String, String>> copiedMeta = new HashMap<>(capturedMeta);
        System.out.println("Copied metadata: " + copiedMeta);
        assertEquals(Map.of(id, Map.of("shardId", "1", "version", "1")), copiedMeta);

        verify(metadataManager).saveEncryptedPoint(argThat(pt ->
                pt.getId().equals("test") &&
                        pt.getShardId() == 1 &&
                        pt.getVersion() == 1 &&
                        pt.getVectorLength() == 2 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), testCiphertext) &&
                        pt.getBuckets().equals(Collections.singletonList(1))
        ));
    }

    @Test
    void testInsert_UpdatesMetadataAndMarksShardDirty() throws IOException {
        double[] vector = {5.5, 7.7};
        String id = "vec123";
        byte[] cipher = new byte[16];
        Arrays.fill(cipher, (byte) 2);

        when(lsh.getBucketId(eq(vector))).thenReturn(4);
        EncryptedPoint template = new EncryptedPoint(id, 0, testIv, cipher, 99, 2, null);
        when(crypto.encrypt(eq(id), eq(vector))).thenReturn(template);

        System.out.println("Calling insert with id: " + id);
        service.insert(id, vector);
        System.out.println("Insert completed");

        verify(index).addPoint(argThat(pt ->
                pt.getId().equals("vec123") &&
                        pt.getShardId() == 4 &&
                        pt.getVersion() == 99 &&
                        pt.getVectorLength() == 2 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), cipher) &&
                        pt.getBuckets().equals(Collections.singletonList(4))
        ));
        verify(index).markShardDirty(4);

        verify(metadataManager).batchUpdateVectorMetadata(metadataCaptor.capture());
        Map<String, Map<String, String>> capturedMeta = metadataCaptor.getValue();
        System.out.println("Captured metadata: " + capturedMeta);
        Map<String, Map<String, String>> copiedMeta = new HashMap<>(capturedMeta);
        System.out.println("Copied metadata: " + copiedMeta);
        assertEquals(Map.of(id, Map.of("shardId", "4", "version", "99")), copiedMeta);

        verify(metadataManager).saveEncryptedPoint(argThat(pt ->
                pt.getId().equals("vec123") &&
                        pt.getShardId() == 4 &&
                        pt.getVersion() == 99 &&
                        pt.getVectorLength() == 2 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), cipher) &&
                        pt.getBuckets().equals(Collections.singletonList(4))
        ));
    }

    @Test
    void testInsertHandlesDifferentDimensions() throws IOException {
        double[] vector = new double[128];
        String id = UUID.randomUUID().toString();
        byte[] cipher = new byte[64];
        Arrays.fill(cipher, (byte) 2);

        when(lsh.getBucketId(eq(vector))).thenReturn(11);
        EncryptedPoint template = new EncryptedPoint(id, 0, testIv, cipher, 7, 128, null);
        when(crypto.encrypt(eq(id), eq(vector))).thenReturn(template);

        System.out.println("Calling insert with id: " + id);
        service.insert(id, vector);
        System.out.println("Insert completed");

        verify(index).addPoint(argThat(pt ->
                pt.getId().equals(id) &&
                        pt.getShardId() == 11 &&
                        pt.getVersion() == 7 &&
                        pt.getVectorLength() == 128 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), cipher) &&
                        pt.getBuckets().equals(Collections.singletonList(11))
        ));
        verify(index).markShardDirty(11);

        verify(metadataManager).batchUpdateVectorMetadata(metadataCaptor.capture());
        Map<String, Map<String, String>> capturedMeta = metadataCaptor.getValue();
        System.out.println("Captured metadata: " + capturedMeta);
        Map<String, Map<String, String>> copiedMeta = new HashMap<>(capturedMeta);
        System.out.println("Copied metadata: " + copiedMeta);
        assertEquals(Map.of(id, Map.of("shardId", "11", "version", "7")), copiedMeta);

        verify(metadataManager).saveEncryptedPoint(argThat(pt ->
                pt.getId().equals(id) &&
                        pt.getShardId() == 11 &&
                        pt.getVersion() == 7 &&
                        pt.getVectorLength() == 128 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), cipher) &&
                        pt.getBuckets().equals(Collections.singletonList(11))
        ));
    }
}
