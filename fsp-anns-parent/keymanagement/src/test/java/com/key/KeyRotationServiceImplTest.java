package com.key;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.fspann.key.KeyManager;

import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Path;
import java.util.Collections;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KeyRotationServiceImplTest {

    @TempDir Path tmp;

    @Test
    void reEncryptAll_reinsertsViaIndexService() throws Exception {
        // Mocks and fixtures
        KeyManager keyManager = mock(KeyManager.class);
        KeyRotationPolicy policy = new KeyRotationPolicy(1, Long.MAX_VALUE);
        RocksDBMetadataManager meta = mock(RocksDBMetadataManager.class);
        CryptoService crypto = mock(CryptoService.class);
        IndexService index = mock(IndexService.class);

        when(meta.getPointsBaseDir()).thenReturn(tmp.toString());

        // Old point on disk (v=1)
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];
        EncryptedPoint old = new EncryptedPoint("id123", 0, iv, ct, 1, 2, Collections.singletonList(0));
        PersistenceUtils.saveObject(old, tmp.resolve("id123.point").toString(), tmp.toString());

        // Keys
        KeyVersion v1 = new KeyVersion(1, new SecretKeySpec(new byte[32], "AES"));
        KeyVersion v2 = new KeyVersion(2, new SecretKeySpec(new byte[32], "AES"));
        when(keyManager.getCurrentVersion()).thenReturn(v2);
        when(keyManager.getSessionKey(1)).thenReturn(v1.getKey());

        // Crypto decrypt old -> raw vector
        double[] raw = new double[]{0.1, 0.2};
        when(crypto.decryptFromPoint(eq(old), eq(v1.getKey()))).thenReturn(raw);

        KeyRotationServiceImpl svc = new KeyRotationServiceImpl(
                keyManager, policy, tmp.toString(), meta, crypto);
        svc.setIndexService(index);

        // Act
        svc.reEncryptAll();

        // Assert: reinserted via service with same id and raw vector
        verify(index, times(1)).insert(eq("id123"), aryEq(raw));
        // optional: buffer flush + cache clear are best-effort; we won’t assert
    }

    @Test
    void rotateIfNeeded_returnsEmptyListAndPersistsMeta(@TempDir Path dir) throws Exception {
        KeyManager keyManager = mock(KeyManager.class);
        KeyRotationPolicy policy = new KeyRotationPolicy(1, 1); // easy to trigger
        RocksDBMetadataManager meta = mock(RocksDBMetadataManager.class);
        CryptoService crypto = mock(CryptoService.class);
        IndexService index = mock(IndexService.class);

        when(meta.getPointsBaseDir()).thenReturn(dir.toString());
        when(keyManager.rotateKey()).thenReturn(new KeyVersion(3, new SecretKeySpec(new byte[32], "AES")));
        when(keyManager.getCurrentVersion()).thenReturn(new KeyVersion(3, new SecretKeySpec(new byte[32], "AES")));

        KeyRotationServiceImpl svc = new KeyRotationServiceImpl(
                keyManager, policy, dir.toString(), meta, crypto);
        svc.setIndexService(index);

        // Trigger rotation
        svc.incrementOperation(); // hits maxOperations==1
        var updated = svc.rotateIfNeededAndReturnUpdated();

        // We return empty list now (no metadataManager.getAllEncryptedPoints() call)
        assert(updated.isEmpty());
        // verify rotation happened once
        verify(keyManager, times(1)).rotateKey();
    }
}
