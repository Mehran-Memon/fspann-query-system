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

        // Stored encryption binds AAD (id, version, dim) and writes metadata (version + dim)
        EncryptedPoint pt = crypto.encrypt("v123", vec);
        assertEquals(1, pt.getVersion());
        verify(meta).updateVectorMetadata(eq("v123"),
                eq(Map.of("version", "1", "dim", String.valueOf(vec.length))));

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
    void decryptQuery_roundTripWithEncryptionUtils() throws Exception {
        // This test pins the contract used by QueryServiceImpl:
        // decryptQuery(ciphertext, iv, key) must successfully decrypt a blob
        // that was produced by EncryptionUtils.encryptVector with the same key/iv.
        double[] vec = { 1.5, -2.0, 3.25 };

        byte[] iv = EncryptionUtils.generateIV();
        byte[] ct = EncryptionUtils.encryptVector(vec, iv, keyV1);

        double[] out = crypto.decryptQuery(ct, iv, keyV1);
        assertArrayEquals(vec, out, 1e-12);

        // Wrong key must fail with the service-level CryptoException
        assertThrows(AesGcmCryptoService.CryptoException.class, () ->
                crypto.decryptQuery(ct, iv, keyV2));
    }

    @Test
    void reEncrypt_usesOldAndNewKeys_andUpdatesVersion() {
        double[] vec = new double[]{7.0, 8.0};

        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(1, keyV1));
        EncryptedPoint original = crypto.encrypt("z1", vec);

        when(keySvc.getVersion(1)).thenReturn(new KeyVersion(1, keyV1));
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(2, keyV2));

        byte[] iv2 = EncryptionUtils.generateIV();
        EncryptedPoint upd = crypto.reEncrypt(original, keyV2, iv2);
        assertEquals(2, upd.getVersion());

        double[] round = crypto.decryptFromPoint(upd, keyV2);
        assertArrayEquals(vec, round, 1e-12);

        verify(meta).updateVectorMetadata(eq("z1"),
                eq(Map.of("version", "2", "dim", String.valueOf(vec.length))));
    }

    @Test
    void encrypt_rejectsNaN() {
        assertThrows(IllegalArgumentException.class,
                () -> crypto.encrypt("bad", new double[]{Double.NaN}));
    }

    @Test
    void decrypt_withWrongKeyThrows() {
        double[] vec = new double[]{1,2,3};
        EncryptedPoint pt = crypto.encrypt("abc", vec);

        // wrong key (simulate different current key)
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(99, keyV2));

        assertThrows(AesGcmCryptoService.CryptoException.class, () ->
                crypto.decryptFromPoint(pt, keyV2));
    }

    @Test
    void decrypt_failsIfIdChanges_dueToAAD() {
        double[] vec = { 1.5, 2.5 };
        EncryptedPoint pt = crypto.encrypt("good-id", vec);

        // Tamper the ID → AAD mismatch
        EncryptedPoint tampered = new EncryptedPoint(
                "bad-id",
                pt.getShardId(),
                pt.getIv(),
                pt.getCiphertext(),
                pt.getVersion(),
                pt.getVectorLength(),
                pt.getBuckets()
        );

        assertThrows(AesGcmCryptoService.CryptoException.class, () ->
                crypto.decryptFromPoint(tampered, keyV1));
    }

    @Test
    void decrypt_failsOnCiphertextTamper() {
        double[] vec = { 0.25, -0.75, 9.5 };
        EncryptedPoint pt = crypto.encrypt("id-tamper", vec);

        byte[] badCt = pt.getCiphertext().clone();
        badCt[badCt.length - 1] ^= 0x01;  // flip one bit

        EncryptedPoint tampered =
                new EncryptedPoint(pt.getId(), pt.getShardId(), pt.getIv(), badCt,
                        pt.getVersion(), pt.getVectorLength(), pt.getBuckets());

        assertThrows(AesGcmCryptoService.CryptoException.class, () ->
                crypto.decryptFromPoint(tampered, keyV1));
    }

    @Test
    void ivIs96BitsAndVariesAcrossEncryptions() {
        double[] vec = { 3, 1, 4 };
        EncryptedPoint a = crypto.encrypt("iv-test-A", vec);
        EncryptedPoint b = crypto.encrypt("iv-test-B", vec);

        assertEquals(12, a.getIv().length, "IV must be 96 bits (12 bytes)");
        assertEquals(12, b.getIv().length, "IV must be 96 bits (12 bytes)");

        assertNotEquals(
                java.util.Arrays.toString(a.getIv()),
                java.util.Arrays.toString(b.getIv()),
                "IVs should differ between encryptions"
        );
    }

    @Test
    void encryptToPoint_bindsAAD_andNoMetadataWrites() {
        double[] q = { 6.0, 7.0, 8.0 };
        EncryptedPoint tokenPt = crypto.encryptToPoint("query-token", q, keyV1);

        verify(meta, never()).updateVectorMetadata(any(), any());

        // Change the ID → AAD mismatch → failure
        EncryptedPoint tampered =
                new EncryptedPoint("other-id", tokenPt.getShardId(), tokenPt.getIv(), tokenPt.getCiphertext(),
                        tokenPt.getVersion(), tokenPt.getVectorLength(), tokenPt.getBuckets());

        assertThrows(AesGcmCryptoService.CryptoException.class, () ->
                crypto.decryptFromPoint(tampered, keyV1));
    }

    @Test
    void reEncrypt_changesIvAndRejectsOldKey() {
        double[] vec = { 2.0, 4.0 };

        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(1, keyV1));
        EncryptedPoint original = crypto.encrypt("reenc-id", vec);

        when(keySvc.getVersion(1)).thenReturn(new KeyVersion(1, keyV1));
        when(keySvc.getCurrentVersion()).thenReturn(new KeyVersion(2, keyV2));

        byte[] iv2 = EncryptionUtils.generateIV();
        EncryptedPoint upd = crypto.reEncrypt(original, keyV2, iv2);

        assertEquals(2, upd.getVersion());
        assertArrayEquals(vec, crypto.decryptFromPoint(upd, keyV2), 1e-12);
        assertNotEquals(
                java.util.Arrays.toString(original.getIv()),
                java.util.Arrays.toString(upd.getIv()),
                "Re-encrypt should use a different IV"
        );

        // Old key must not decrypt the new blob
        assertThrows(AesGcmCryptoService.CryptoException.class, () ->
                crypto.decryptFromPoint(upd, keyV1));
    }

    @Test
    void encrypt_rejectsNullOrEmptyId() {
        assertThrows(NullPointerException.class, () -> crypto.encrypt(null, new double[]{1.0}));
        assertThrows(IllegalArgumentException.class, () -> crypto.encrypt("", new double[]{1.0}));
    }

    @Test
    void encrypt_rejectsInf() {
        assertThrows(IllegalArgumentException.class, () -> crypto.encrypt("inf", new double[]{Double.POSITIVE_INFINITY}));
    }

    @Test
    void encrypt_zeroLengthVectorIsRejectedOrContractPinned() {
        // Current contract: reject empty vectors
        assertThrows(IllegalArgumentException.class, () -> crypto.encrypt("empty", new double[]{}));
    }
}
