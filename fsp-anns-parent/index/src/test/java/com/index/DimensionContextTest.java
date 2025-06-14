package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.SecretKey;

import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DimensionContextTest {
    @Mock CryptoService     crypto;
    @Mock KeyLifeCycleService keyService;
    @Mock SecureLSHIndex    index;

    private DimensionContext context;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        context = new DimensionContext(
                index,
                crypto,
                keyService,
                new EvenLSH(2, 10)
        );

        // 1) Stub both old and new versions (iv + encryptedQuery can be anything non‐null)
        byte[] oldIv  = new byte[12];
        byte[] oldQ   = new byte[32];
        byte[] newIv  = new byte[12];
        byte[] newQ   = new byte[32];
        SecretKey dummyKey = new SecretKeySpec(new byte[16], "AES");

        KeyVersion vOld = new KeyVersion(0, dummyKey, oldIv, oldQ);
        KeyVersion vNew = new KeyVersion(1, dummyKey, newIv, newQ);
        when(keyService.getPreviousVersion()).thenReturn(vOld);
        when(keyService.getCurrentVersion()).thenReturn(vNew);

        // 2) Tell the index there is exactly one dirty shard: shard 5 (for example)
        when(index.getDirtyShards()).thenReturn(Set.of(5));

        // 3) That shard contains exactly one point with id "test"
        EncryptedPoint pt = new EncryptedPoint("test", 5, oldIv, oldQ, vOld.getVersion());
        when(index.queryEncrypted(any(QueryToken.class))).thenReturn(List.of(pt));

        // 4) Re-encrypt returns a new EncryptedPoint (must be non-null)
        EncryptedPoint rePt = new EncryptedPoint("test", 5, newIv, newQ, vNew.getVersion());
        when(crypto.reEncrypt(eq(pt), eq(vNew.getSecretKey()))).thenReturn(rePt);
    }

    @Test
    void testReEncryptAll() {
        // this will now execute the loop once, remove "test" and then add the rePt
        context.reEncryptAll();

        verify(index, times(1)).removePoint("test");
        verify(index, times(1)).addPoint(any(EncryptedPoint.class));
    }
}
