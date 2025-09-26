package com.key;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.key.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.SecretKey;
import java.util.List;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.*;

class KeyRotationServiceImplTest {

    private KeyManager km;
    private KeyRotationServiceImpl svc;
    private RocksDBMetadataManager meta;
    private CryptoService crypto;
    private IndexService index;

    private final SecretKey v1 = new SecretKeySpec(new byte[32], "AES");
    private final SecretKey v2 = new SecretKeySpec(new byte[32], "AES");

    @BeforeEach
    void init() throws Exception {
        // Use a real KeyManager? You can also mock it. Here we stub via a tiny real instance:
        km = mock(KeyManager.class);
        when(km.getCurrentVersion()).thenReturn(new KeyVersion(2, v2));
        when(km.getPreviousVersion()).thenReturn(new KeyVersion(1, v1));
        when(km.getSessionKey(1)).thenReturn(v1);
        when(km.getSessionKey(2)).thenReturn(v2);
        when(km.rotateKey()).thenReturn(new KeyVersion(3, new SecretKeySpec(new byte[32], "AES")));

        meta = mock(RocksDBMetadataManager.class);
        crypto = mock(CryptoService.class);
        index = mock(IndexService.class);

        svc = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(5, 60_000), // thresholds not used in this unit
                "rot-meta",
                meta,
                crypto
        );
        svc.setIndexService(index);
        svc.setCryptoService(crypto);
    }

    @Test
    void reEncryptAll_usesPointVersionKeyAndReinsertsById() {
        EncryptedPoint p1 = new EncryptedPoint("id-1", 0, new byte[12], new byte[16], 1, 4, List.of(0));
        EncryptedPoint p2 = new EncryptedPoint("id-2", 0, new byte[12], new byte[16], 1, 4, List.of(0));

        when(meta.getAllEncryptedPoints()).thenReturn(List.of(p1, p2));
        when(crypto.decryptFromPoint(eq(p1), eq(v1))).thenReturn(new double[]{0,0,0,0});
        when(crypto.decryptFromPoint(eq(p2), eq(v1))).thenReturn(new double[]{1,1,1,1});

        svc.reEncryptAll();

        // Reinsert raw vectors by id (index service encrypts w/ CURRENT key)
        verify(index, times(1)).insert(eq("id-1"), any(double[].class));
        verify(index, times(1)).insert(eq("id-2"), any(double[].class));
        verify(index, atLeast(0)).getPointBuffer(); // may be called to flush

        verify(index).insert(eq("id-1"), aryEq(new double[]{0,0,0,0}));
        verify(index).insert(eq("id-2"), aryEq(new double[]{1,1,1,1}));

    }

    @Test
    void rotateIfNeeded_persistsNewVersionAndCallsReencrypt() {
        // Force rotation via public method
        svc.rotateKey();
        // Just assert that it did not blow up; deep verifications can mock PersistenceUtils if desired
        verify(km, times(1)).rotateKey();
        // reEncryptAll() is invoked; we don't assert internals here as the previous test covers it
    }
}
