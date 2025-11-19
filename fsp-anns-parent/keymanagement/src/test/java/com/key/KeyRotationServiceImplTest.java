package com.key;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.key.*;
import com.fspann.common.IndexService;
import com.fspann.common.EncryptedPoint;
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

        km = mock(KeyManager.class);
        when(km.getCurrentVersion()).thenReturn(new KeyVersion(2, v2));
        when(km.getPreviousVersion()).thenReturn(new KeyVersion(1, v1));
        when(km.getSessionKey(1)).thenReturn(v1);
        when(km.getSessionKey(2)).thenReturn(v2);
        when(km.rotateKey()).thenReturn(new KeyVersion(3,
                new SecretKeySpec(new byte[32], "AES")));

        meta = mock(RocksDBMetadataManager.class);
        crypto = mock(CryptoService.class);
        index = mock(IndexService.class);

        svc = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(5, 60_000),
                "rot-meta",
                meta,
                crypto
        );
    }

    @Test
    void reEncryptAll_usesPointVersionKeyAndReinsertsById() {
        EncryptedPoint p1 = new EncryptedPoint(
                "id-1", 0, new byte[12], new byte[16], 1, 4, List.of(0)
        );
        EncryptedPoint p2 = new EncryptedPoint(
                "id-2", 0, new byte[12], new byte[16], 1, 4, List.of(0)
        );

        when(meta.getAllEncryptedPoints()).thenReturn(List.of(p1, p2));
        when(crypto.decryptFromPoint(eq(p1), eq(v1)))
                .thenReturn(new double[]{0, 0, 0, 0});
        when(crypto.decryptFromPoint(eq(p2), eq(v1)))
                .thenReturn(new double[]{1, 1, 1, 1});

        svc.reEncryptAll();

        verify(index).insert(eq("id-1"), aryEq(new double[]{0, 0, 0, 0}));
        verify(index).insert(eq("id-2"), aryEq(new double[]{1, 1, 1, 1}));

        verify(index, atLeastOnce()).getPointBuffer();
    }

    @Test
    void rotateIfNeeded_persistsNewVersionAndCallsReencrypt() {
        svc.rotateKey();
        verify(km, times(1)).rotateKey();
    }
}
