package com.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.common.MetadataManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AesGcmCryptoServiceTest {
    @Mock
    private KeyLifeCycleService keyService;

    @Mock
    private MetadataManager metadataManager;

    private AesGcmCryptoService cryptoService;
    private SecretKey key1, key2;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        cryptoService = new AesGcmCryptoService(meterRegistry, keyService, metadataManager);

        key1 = new SecretKeySpec(new byte[16], "AES");
        key2 = new SecretKeySpec(new byte[16], "AES");

        // Dummy IV and encryptedQuery for KeyVersion
        byte[] dummyIv = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        byte[] dummyEncryptedQuery = new byte[]{42, 43, 44};

        when(metadataManager.getVectorMetadata(anyString())).thenReturn(Map.of("version", "1"));
        when(keyService.getVersion(1)).thenReturn(new KeyVersion(1, key1, dummyIv, dummyEncryptedQuery));
    }

    @Test
    void testReEncrypt() {
        double[] vector = {1.0, 2.0};
        EncryptedPoint pt = cryptoService.encryptToPoint("test", vector, key1);
        EncryptedPoint reEncrypted = cryptoService.reEncrypt(pt, key2);

        assertNotEquals(pt.getCiphertext(), reEncrypted.getCiphertext());
        assertNotNull(reEncrypted.getIv());
        assertNotEquals(pt.getIv(), reEncrypted.getIv());
        assertEquals(pt.getId(), reEncrypted.getId());
        assertEquals(pt.getShardId(), reEncrypted.getShardId());
    }

    @Test
    void testReEncryptWithInvalidKey() {
        when(metadataManager.getVectorMetadata(anyString())).thenReturn(Map.of("version", "999"));
        when(keyService.getVersion(999)).thenThrow(new IllegalArgumentException("Version not found"));

        EncryptedPoint pt = cryptoService.encryptToPoint("test", new double[]{1.0, 2.0}, key1);
        assertThrows(com.fspann.crypto.AesGcmCryptoService.CryptoException.class,
                () -> cryptoService.reEncrypt(pt, key2));
    }

    @Test
    void testEncrypt() {
        byte[] iv = new byte[12];
        SecretKey key = new SecretKeySpec(new byte[16], "AES");
        double[] vector = {1.0, 2.0};
        byte[] ciphertext = cryptoService.encrypt(vector, key, iv);
        assertNotNull(ciphertext);
        assertTrue(ciphertext.length > 0);
    }
}
