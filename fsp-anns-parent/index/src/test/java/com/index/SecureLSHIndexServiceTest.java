package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyVersion;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.common.MetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SecureLSHIndexServiceTest {
    @Mock
    private SecureLSHIndex index;
    @Mock
    private AesGcmCryptoService crypto;
    @Mock
    private KeyLifeCycleService keyService;
    @Mock
    private EvenLSH lsh;
    @Mock
    private MetadataManager metadataManager;

    private SecureLSHIndexService service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        service = new SecureLSHIndexService(index, crypto, keyService, lsh, metadataManager, 2, 10);
        when(lsh.getBucketId(any(double[].class))).thenReturn(1);
    }

    @Test
    void testInsert() {
        double[] vector = {1.0, 2.0};
        byte[] dummyIv             = new byte[12];
        byte[] dummyEncryptedQuery = new byte[32];
        KeyVersion version = new KeyVersion(1,
                new SecretKeySpec(new byte[16], "AES"),
                dummyIv,
                dummyEncryptedQuery
                );
        when(keyService.getCurrentVersion()).thenReturn(version);
        EncryptedPoint expectedPt = new EncryptedPoint("test", 0, new byte[12], new byte[32], 1);
        when(crypto.encryptToPoint(eq("test"), eq(vector), eq(version.getSecretKey()))).thenReturn(expectedPt);
        service.insert("test", vector);
        verify(index).addPoint(argThat(pt -> pt.getId().equals("test") && pt.getShardId() == 0));
    }

    @Test
    void testInsertWithProjection() {
        double[] vector = {1.0, 2.0};
        byte[] dummyIv             = new byte[12];
        byte[] dummyEncryptedQuery = new byte[32];
        KeyVersion version = new KeyVersion(
        1,
        new SecretKeySpec(new byte[16], "AES"),
        dummyIv,
        dummyEncryptedQuery
        );
        when(keyService.getCurrentVersion()).thenReturn(version);
        EncryptedPoint expectedPoint = new EncryptedPoint("test", 1, new byte[12], new byte[32], 1); // Expected shardId
        when(crypto.encryptToPoint(eq("test"), eq(vector), eq(version.getSecretKey()))).thenReturn(expectedPoint);
        when(lsh.getBucketId(eq(vector))).thenReturn(1); // Match expected shardId
        service.insert("test", vector);
        verify(index).addPoint(argThat(pt -> pt.getId().equals("test") && pt.getShardId() == 1));
        verify(index).markShardDirty(1);
    }
}