package com.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.common.RocksDBMetadataManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.KeyManager;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AesGcmCryptoServiceTest {
    @Mock
    private KeyLifeCycleService keyService;

    @Mock
    private RocksDBMetadataManager metadataManager;

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
        // First, simulate version 1 for encryption
        when(keyService.getVersion(1)).thenReturn(new KeyVersion(1, key1));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key1));

        double[] vector = {1.0, 2.0};
        EncryptedPoint pt = cryptoService.encryptToPoint("test", vector, key1);
        assertEquals(1, pt.getVersion()); // now this should pass

        // Then simulate version 2 for re-encryption
        when(keyService.getVersion(2)).thenReturn(new KeyVersion(2, key2));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(2, key2));

        EncryptedPoint reEncrypted = cryptoService.reEncrypt(pt, key2, cryptoService.generateIV());

        assertNotEquals(pt.getCiphertext(), reEncrypted.getCiphertext());
        assertNotNull(reEncrypted.getIv());
        assertNotEquals(pt.getIv(), reEncrypted.getIv());
        assertEquals(pt.getId(), reEncrypted.getId());
        assertEquals(pt.getShardId(), reEncrypted.getShardId());
        assertEquals(2, reEncrypted.getVersion()); // ✅ new version after re-encryption
    }

    @Test
    public void testReEncryptWithInvalidOldKeyVersion() {
        // Simulate encrypted point with bad version (999)
        EncryptedPoint pt = new EncryptedPoint("bad", 0, cryptoService.generateIV(), new byte[16], 999, 2);
        when(keyService.getVersion(999)).thenThrow(new IllegalArgumentException("Version not found"));

        assertThrows(AesGcmCryptoService.CryptoException.class,
                () -> cryptoService.reEncrypt(pt, key2, cryptoService.generateIV()));
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

    @Test
    public void testReEncryptWithCustomIV() {
        // Step 1: encrypt with version 1
        when(keyService.getVersion(1)).thenReturn(new KeyVersion(1, key1));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key1));

        double[] vector = {7.7, 8.8};
        EncryptedPoint original = cryptoService.encryptToPoint("pt-custom", vector, key1);

        // Step 2: simulate upgrade to version 2
        when(keyService.getVersion(2)).thenReturn(new KeyVersion(2, key2));
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(2, key2));

        byte[] newIv = cryptoService.generateIV();
        EncryptedPoint reenc = cryptoService.reEncrypt(original, key2, newIv);

        assertEquals(2, reenc.getVersion());
        assertEquals(original.getId(), reenc.getId());
        assertEquals(original.getShardId(), reenc.getShardId());

        assertNotNull(reenc.getIv());
        assertNotEquals(new String(original.getIv()), new String(reenc.getIv()));
        assertNotEquals(original.getCiphertext(), reenc.getCiphertext());

        double[] decrypted = cryptoService.decryptFromPoint(reenc, key2);
        assertArrayEquals(vector, decrypted, 1e-9);
    }

    @Test
    public void testEncryptWithInvalidId() {
        double[] vector = {1.0, 2.0};
        assertThrows(IllegalArgumentException.class, () -> cryptoService.encrypt("invalid@id", vector));
    }

    @Test
    public void testEncryptWithInvalidVector() {
        double[] vector = {Double.NaN, 2.0};
        assertThrows(IllegalArgumentException.class, () -> cryptoService.encrypt("valid-id", vector));
    }
}
