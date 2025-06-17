package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyVersion;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.MetadataManager;
import com.fspann.index.service.SecureLSHIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceTest {

    @Spy  private SecureLSHIndex index;
    @Mock private AesGcmCryptoService crypto;
    @Mock private KeyLifeCycleService keyService;
    @Mock private EvenLSH lsh;
    @Mock private MetadataManager metadataManager;

    private SecureLSHIndexService service;
    private final byte[] testIv = new byte[12];
    private final byte[] testCiphertext = new byte[32];

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        service = new SecureLSHIndexService(crypto, keyService, metadataManager); // Ensure proper setup
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, new SecretKeySpec(new byte[16], "AES")));
        when(lsh.getBucketId(any(double[].class))).thenReturn(1);
    }

    @Test
    void testInsert_VectorEncryptedCorrectly() {
        double[] vector = {1.0, 2.0};
        SecretKeySpec key = new SecretKeySpec(new byte[16], "AES");
        KeyVersion version = new KeyVersion(1, key);
        String id = "test";

        // Mock behavior
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(lsh.getBucketId(vector)).thenReturn(1);
        EncryptedPoint template = new EncryptedPoint(id, 0, testIv, testCiphertext, 1, 2);
        when(crypto.encryptToPoint(eq(id), eq(vector), eq(key))).thenReturn(template);

        // Insert test
        service.insert(id, vector);

        // Verify interactions
        verify(index).addPoint(argThat(pt ->
                pt.getId().equals("test") &&
                        pt.getShardId() == 1 &&
                        pt.getVersion() == 1 &&
                        pt.getVectorLength() == 2 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), testCiphertext)
        ));
        verify(index).markShardDirty(1);
        verify(metadataManager).putVectorMetadata(id, "1", "1");
    }


    @Test
    void testInsert_UpdatesMetadataAndMarksShardDirty() {
        double[] vector = {5.5, 7.7};
        SecretKeySpec key = new SecretKeySpec(new byte[16], "AES");
        KeyVersion version = new KeyVersion(99, key);
        String id = "vec123";
        byte[] cipher = new byte[16];
        Arrays.fill(cipher, (byte) 2);

        when(keyService.getCurrentVersion()).thenReturn(version);
        when(lsh.getBucketId(vector)).thenReturn(4);
        EncryptedPoint template = new EncryptedPoint(id, 0, testIv, cipher, 99, 2);
        when(crypto.encryptToPoint(eq(id), eq(vector), eq(key))).thenReturn(template);

        System.out.println("Calling insert with id: " + id);
        service.insert(id, vector);
        System.out.println("Insert completed");

        verify(index).addPoint(argThat(pt ->
                pt.getId().equals("vec123") &&
                        pt.getShardId() == 4 &&
                        pt.getVersion() == 99 &&
                        pt.getVectorLength() == 2 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), cipher)
        ));
        verify(index).markShardDirty(4);
        verify(metadataManager).putVectorMetadata(id, "4", "99");
    }

    @Test
    void testInsertHandlesDifferentDimensions() {
        double[] vector = new double[128];
        String id = UUID.randomUUID().toString();
        SecretKeySpec key = new SecretKeySpec(new byte[16], "AES");
        KeyVersion version = new KeyVersion(7, key);
        byte[] cipher = new byte[64];
        Arrays.fill(cipher, (byte) 2);

        when(keyService.getCurrentVersion()).thenReturn(version);
        when(lsh.getBucketId(vector)).thenReturn(11);
        EncryptedPoint template = new EncryptedPoint(id, 0, testIv, cipher, 7, 128);
        when(crypto.encryptToPoint(eq(id), eq(vector), eq(key))).thenReturn(template);

        System.out.println("Calling insert with id: " + id);
        service.insert(id, vector);
        System.out.println("Insert completed");

        verify(index).addPoint(argThat(pt ->
                pt.getId().equals(id) &&
                        pt.getShardId() == 11 &&
                        pt.getVersion() == 7 &&
                        pt.getVectorLength() == 128 &&
                        Arrays.equals(pt.getIv(), testIv) &&
                        Arrays.equals(pt.getCiphertext(), cipher)
        ));
        verify(index).markShardDirty(11);
        verify(metadataManager).putVectorMetadata(id, "11", "7");
    }
}