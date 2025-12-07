package com.key;

import com.fspann.common.KeyVersion;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;

import org.junit.jupiter.api.*;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KeyRotationServiceImplTest {

    private KeyManager km;
    private KeyRotationServiceImpl svc;
    private RocksDBMetadataManager meta;
    private CryptoService crypto;

    @BeforeEach
    void setup() {
        SecretKeySpec k1 = new SecretKeySpec(new byte[32], "AES");
        SecretKeySpec k2 = new SecretKeySpec(new byte[32], "AES");

        km = mock(KeyManager.class);

        when(km.getCurrentVersion()).thenReturn(new KeyVersion(2, k2));
        when(km.getPreviousVersion()).thenReturn(new KeyVersion(1, k1));
        when(km.rotateKey()).thenReturn(
                new KeyVersion(3, new SecretKeySpec(new byte[32], "AES"))
        );

        meta = mock(RocksDBMetadataManager.class);
        crypto = mock(CryptoService.class);

        svc = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(5, 60_000),
                "meta-path",
                meta,
                crypto
        );
    }

    @Test
    void rotateKeyDelegatesExactlyOnce() {
        svc.rotateKey();
        verify(km, times(1)).rotateKey();
    }

    @Test
    void reEncryptAll_doesNothingWhenIndexServiceUnset() {
        assertDoesNotThrow(() -> svc.reEncryptAll());
        verify(meta, never()).getAllEncryptedPoints();
        verify(crypto, never()).encrypt(any(), any());
    }

    @Test
    void repeatedReencryptAllStillNoop() {
        svc.reEncryptAll();
        svc.reEncryptAll();
        svc.reEncryptAll();

        verify(meta, never()).getAllEncryptedPoints();
    }

    @Test
    void currentVersionIsReadFromKeyManager() {
        assertEquals(2, svc.getCurrentVersion().getVersion());
    }
}
