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
import java.security.SecureRandom;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AesGcmCryptoServiceTest {
    @Mock
    private KeyLifeCycleService keyService;

    @Mock
    private MetadataManager metadataManager;

    private AesGcmCryptoService cryptoService;
    private SecretKey key1, key2;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        cryptoService = new AesGcmCryptoService(meterRegistry, keyService, metadataManager);

        byte[] k1 = new byte[16];
        byte[] k2 = new byte[16];
        SecureRandom random = new SecureRandom();
        random.nextBytes(k1);

        do {
            random.nextBytes(k2);
        } while (java.util.Arrays.equals(k1, k2)); // ensure they’re different

        key1 = new SecretKeySpec(k1, "AES");
        key2 = new SecretKeySpec(k2, "AES");

        when(metadataManager.getVectorMetadata(anyString())).thenReturn(Map.of("version", "1"));
        when(keyService.getVersion(1)).thenReturn(new KeyVersion(1, key1));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key1));
    }



    @Test
    public void testReEncrypt() {
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
    public void testReEncryptWithInvalidKey() {
        when(metadataManager.getVectorMetadata(anyString())).thenReturn(Map.of("version", "999"));
        when(keyService.getVersion(999)).thenThrow(new IllegalArgumentException("Version not found"));

        EncryptedPoint pt = cryptoService.encryptToPoint("test", new double[]{1.0, 2.0}, key1);
        assertThrows(com.fspann.crypto.AesGcmCryptoService.CryptoException.class,
                () -> cryptoService.reEncrypt(pt, key2));
    }

    @Test
    public void testEncrypt() {
        byte[] iv = new byte[12];
        SecretKey key = new SecretKeySpec(new byte[16], "AES");
        double[] vector = {1.0, 2.0};
        byte[] ciphertext = cryptoService.encrypt(vector, key, iv);
        assertNotNull(ciphertext);
        assertTrue(ciphertext.length > 0);
    }

    @Test
    public void testDecryptFromPoint() {
        double[] original = {1.0, 2.0};
        EncryptedPoint pt = cryptoService.encryptToPoint("vec123", original, key1);
        double[] decrypted = cryptoService.decryptFromPoint(pt, key1);
        assertArrayEquals(original, decrypted, 1e-9);
    }

    @Test
    public void testEncryptGeneratesUniqueIVs() {
        double[] vector = {1.0, 2.0};
        EncryptedPoint pt1 = cryptoService.encryptToPoint("id1", vector, key1);
        EncryptedPoint pt2 = cryptoService.encryptToPoint("id2", vector, key1);
        assertNotEquals(new String(pt1.getIv()), new String(pt2.getIv()));
    }

    @Test
    public void testDecryptQuery() {
        double[] queryVec = {3.3, 4.4};
        byte[] iv = cryptoService.generateIV();
        byte[] encrypted = cryptoService.encrypt(queryVec, key1, iv);
        double[] result = cryptoService.decryptQuery(encrypted, iv, key1);
        assertArrayEquals(queryVec, result, 1e-9);
    }

    @Test
    public void testDecryptWithWrongKeyFails() {
        double[] vector = {5.0, 6.0};
        EncryptedPoint pt = cryptoService.encryptToPoint("failVec", vector, key1);

        // Decryption should fail with wrong key
        assertThrows(AesGcmCryptoService.CryptoException.class, () -> {
            cryptoService.decryptFromPoint(pt, key2);
        });
    }



}
