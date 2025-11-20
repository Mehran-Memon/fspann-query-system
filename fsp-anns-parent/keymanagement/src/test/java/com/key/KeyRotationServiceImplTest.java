package com.key;

import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.common.KeyVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

/**
 * Unit tests aligned with the CURRENT KeyRotationServiceImpl behavior.
 *
 * In this minimal wiring:
 *  - indexService is NOT set → reEncryptAll() logs "services not initialized" and returns.
 *  - We only assert that it is safe (no exceptions) and does not touch metadata.
 *  - rotateKey() must delegate to KeyManager.rotateKey().
 */
class KeyRotationServiceImplTest {

    private KeyManager km;
    private KeyRotationServiceImpl svc;
    private RocksDBMetadataManager meta;
    private CryptoService crypto;

    private final SecretKey v1 = new SecretKeySpec(new byte[32], "AES");
    private final SecretKey v2 = new SecretKeySpec(new byte[32], "AES");

    @BeforeEach
    void init() {
        km = mock(KeyManager.class);
        when(km.getCurrentVersion()).thenReturn(new KeyVersion(2, v2));
        when(km.getPreviousVersion()).thenReturn(new KeyVersion(1, v1));
        when(km.getSessionKey(1)).thenReturn(v1);
        when(km.getSessionKey(2)).thenReturn(v2);
        when(km.rotateKey()).thenReturn(
                new KeyVersion(3, new SecretKeySpec(new byte[32], "AES"))
        );

        meta   = mock(RocksDBMetadataManager.class);
        crypto = mock(CryptoService.class);

        // Note: indexService is NOT passed here; this matches your production ctor usage
        // in places where forward-secure re-encryption may be disabled or deferred.
        svc = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(5, 60_000),
                "rot-meta",
                meta,
                crypto
        );
    }

    @Test
    void reEncryptAll_isNoopWhenServicesNotInitialized_butDoesNotThrow() {
        // With indexService unset, implementation should simply log and return.
        assertDoesNotThrow(() -> svc.reEncryptAll());

        // Because services are "not initialized", metadata must not be touched.
        verify(meta, never()).getAllEncryptedPoints();
    }

    @Test
    void rotateKey_delegatesToKeyManager() {
        svc.rotateKey();
        verify(km, times(1)).rotateKey();
    }
}
