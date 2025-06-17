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
import org.mockito.Spy;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DimensionContextTest {

    @Mock private CryptoService crypto;
    @Mock private KeyLifeCycleService keyService;
    @Spy  private SecureLSHIndex index;
    @Mock private EvenLSH lsh;

    private DimensionContext context;
    private final byte[] oldIv = new byte[12];
    private final byte[] oldQ = new byte[32];
    private final byte[] newIv = new byte[12];
    private final byte[] newQ = new byte[32];
    private final SecretKey dummyKey = new SecretKeySpec(new byte[16], "AES");

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        KeyVersion vOld = new KeyVersion(0, dummyKey);
        KeyVersion vNew = new KeyVersion(1, dummyKey);
        when(keyService.getPreviousVersion()).thenReturn(vOld);
        when(keyService.getCurrentVersion()).thenReturn(vNew);

        // Make sure index is mocked to return dirty shards and query results
        when(index.getDirtyShards()).thenReturn(Set.of(5));
        when(index.queryEncrypted(any(QueryToken.class))).thenReturn(List.of(new EncryptedPoint("test", 5, oldIv, oldQ, vOld.getVersion(), 2)));

        // Mocking crypto service methods
        when(crypto.reEncrypt(any(), eq(dummyKey))).thenReturn(new EncryptedPoint("test", 5, newIv, newQ, vNew.getVersion(), 2));

        // Create the context with mocked dependencies
        context = new DimensionContext(index, crypto, keyService, lsh);
    }


    @Test
    void testReEncryptAll() {
        EncryptedPoint pt = new EncryptedPoint("test", 5, oldIv, oldQ, 0, 2);
        EncryptedPoint rePt = new EncryptedPoint("test", 5, newIv, newQ, 1, 2);

        // Make sure queryEncrypted returns the mock point
        when(index.queryEncrypted(any(QueryToken.class))).thenReturn(List.of(pt));
        when(crypto.reEncrypt(eq(pt), eq(dummyKey))).thenReturn(rePt);

        // Ensure that the method calls removePoint, addPoint, and clearDirtyShard
        context.reEncryptAll();

        verify(index, times(1)).removePoint("test");
        verify(index, times(1)).addPoint(argThat(p ->
                p.getId().equals("test") &&
                        p.getShardId() == 5 &&
                        p.getVersion() == 1 &&
                        p.getVectorLength() == 2 &&
                        Arrays.equals(p.getIv(), newIv) &&
                        Arrays.equals(p.getCiphertext(), newQ)
        ));
        verify(index, times(1)).clearDirtyShard(5);
    }


    @Test
    void testReEncryptAll_NoDirtyShards() {
        when(index.getDirtyShards()).thenReturn(Set.of());
        context.reEncryptAll();

        verify(index, never()).removePoint(any());
        verify(index, never()).addPoint(any());
        verify(index, never()).clearDirtyShard(anyInt());
    }

    @Test
    void testReEncryptAll_EmptyResult() {
        when(index.queryEncrypted(any(QueryToken.class))).thenReturn(List.of());
        context.reEncryptAll();

        verify(index, never()).removePoint(any());
        verify(index, never()).addPoint(any());
        verify(index, never()).clearDirtyShard(anyInt());
    }

    @Test
    void testReEncryptAll_MultiplePoints() {
        EncryptedPoint pt1 = new EncryptedPoint("pt1", 5, oldIv, oldQ, 0, 2);
        EncryptedPoint pt2 = new EncryptedPoint("pt2", 5, oldIv, oldQ, 0, 2);
        EncryptedPoint rePt1 = new EncryptedPoint("pt1", 5, newIv, newQ, 1, 2);
        EncryptedPoint rePt2 = new EncryptedPoint("pt2", 5, newIv, newQ, 1, 2);

        when(index.queryEncrypted(any(QueryToken.class))).thenReturn(List.of(pt1, pt2));
        when(crypto.reEncrypt(eq(pt1), eq(dummyKey))).thenReturn(rePt1);
        when(crypto.reEncrypt(eq(pt2), eq(dummyKey))).thenReturn(rePt2);

        context.reEncryptAll();

        verify(index, times(1)).removePoint("pt1");
        verify(index, times(1)).removePoint("pt2");
        verify(index, times(1)).addPoint(argThat(p ->
                p.getId().equals("pt1") &&
                        p.getShardId() == 5 &&
                        p.getVersion() == 1 &&
                        p.getVectorLength() == 2 &&
                        Arrays.equals(p.getIv(), newIv) &&
                        Arrays.equals(p.getCiphertext(), newQ)
        ));
        verify(index, times(1)).addPoint(argThat(p ->
                p.getId().equals("pt2") &&
                        p.getShardId() == 5 &&
                        p.getVersion() == 1 &&
                        p.getVectorLength() == 2 &&
                        Arrays.equals(p.getIv(), newIv) &&
                        Arrays.equals(p.getCiphertext(), newQ)
        ));
        verify(index, times(1)).clearDirtyShard(5);
    }
}