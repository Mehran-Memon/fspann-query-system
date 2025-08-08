package com.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.EncryptionUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AesGcmCryptoServiceTest {

    private KeyLifeCycleService keySvc;
    private RocksDBMetadataManager meta;
    private AesGcmCryptoService crypto;

    private SecretKey keyV1;
    private SecretKey keyV2;

    @BeforeEach
    void setUp() throws Exception {
        keySvc = mock(KeyLifeCycleService.class);
        meta   = mock(RocksDBMetadataManager.class);

        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256, SecureRandom.getInstanceStrong());
        keyV1 = kg.generateKey();
        keyV2 = kg.generateKey();

        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(1, keyV1));

        crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, meta);
    }

    @Test
    void encrypt_then_decrypt_roundTrip() {
        double[] vec = new double[]{0.1, -2.5, 3.14, 9.0};

        // encrypt(id, vector) uses current version & writes metadata
        EncryptedPoint pt = crypto.encrypt("v123", vec);
        assertEquals(1, pt.getVersion());
        verify(meta).updateVectorMetadata(eq("v123"), eq(Map.of("version", "1")));

        double[] out = crypto.decryptFromPoint(pt, keyV1);
        assertArrayEquals(vec, out, 1e-12);
    }

    @Test
    void encryptToPoint_noMetadataSideEffect() {
        double[] q = new double[]{1.0, 2.0};
        EncryptedPoint tokenPt = crypto.encryptToPoint("index", q, keyV1);

        assertEquals(1, tokenPt.getVersion());
        // critical: no metadata writes for query tokens
        verify(meta, never()).updateVectorMetadata(eq("index"), any());
    }

    @Test
    void reEncrypt_usesOldAndNewKeys_andUpdatesVersion() throws Exception {
        // original with v1
        double[] vec = new double[]{7.0, 8.0};
        byte[] iv1 = EncryptionUtils.generateIV();
        byte[] ct1 = EncryptionUtils.encryptVector(vec, iv1, keyV1);

        EncryptedPoint original = new EncryptedPoint("z1", 0, iv1, ct1, 1, vec.length, null);

        // now key service current is v2, but also able to fetch v1
        when(keySvc.getVersion(1)).thenReturn(new KeyVersion(1, keyV1));
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(2, keyV2));

        byte[] iv2 = EncryptionUtils.generateIV();
        EncryptedPoint upd = crypto.reEncrypt(original, keyV2, iv2);
        assertEquals(2, upd.getVersion());

        double[] round = crypto.decryptFromPoint(upd, keyV2);
        assertArrayEquals(vec, round, 1e-12);

        verify(meta).updateVectorMetadata(eq("z1"), eq(Map.of("version", "2")));
    }

    @Test
    void encrypt_rejectsNaN() {
        assertThrows(IllegalArgumentException.class, () -> crypto.encrypt("bad", new double[]{Double.NaN}));
    }

    @Test
    void decrypt_withWrongKeyThrows() {
        double[] vec = new double[]{1,2,3};
        EncryptedPoint pt = crypto.encrypt("abc", vec);

        // wrong key (new current)
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(99, keyV2));

        assertThrows(AesGcmCryptoService.CryptoException.class, () ->
                crypto.decryptFromPoint(pt, keyV2));
    }
}
