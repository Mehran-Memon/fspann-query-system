package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.EvenLSH;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class QueryTokenFactoryTest {

    @Mock private CryptoService crypto;
    @Mock private KeyLifeCycleService keys;
    @Mock private EvenLSH lsh;

    private QueryTokenFactory factory;

    @BeforeEach
    void init() {
        MockitoAnnotations.openMocks(this);

        SecretKey mockKey = new SecretKeySpec(new byte[16], "AES");
        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(7, mockKey));

        when(lsh.getDimensions()).thenReturn(2);

        when(crypto.encryptToPoint(eq("query"), any(double[].class), any()))
                .thenReturn(new EncryptedPoint("query",0,new byte[12],new byte[32],7,2,List.of(0)));

        factory = new QueryTokenFactory(
                crypto,
                keys,
                lsh,
                3,      // numTables
                0,      // probeRange
                2,      // divisions
                3,      // m
                13L     // seedBase
        );
    }

    // --------------------------------------------------------------------

    @Test
    void testCreateToken() {
        double[] vec = {1.0, 2.0};

        // mock LSH to return 3 tables
        when(lsh.getBuckets(eq(vec), anyInt(), eq(0))).thenReturn(List.of(1,2,3));
        when(lsh.getBuckets(eq(vec), anyInt(), eq(1))).thenReturn(List.of(4,5,6));
        when(lsh.getBuckets(eq(vec), anyInt(), eq(2))).thenReturn(List.of(7,8,9));

        QueryToken t = factory.create(vec, 5);

        assertEquals(3, t.getNumTables());
        assertEquals(3, t.getTableBuckets().size());
        assertEquals("dim_2_v7", t.getEncryptionContext());
        assertEquals(2, t.getDimension());

        assertNotNull(t.getCodes());
        assertEquals(2, t.getCodes().length);
    }

    // --------------------------------------------------------------------

    @Test
    void testCreateTokenWithDifferentNumTables() {
        factory = new QueryTokenFactory(
                crypto, keys, lsh,
                5, 0,
                2, 3, 13L
        );

        when(lsh.getDimensions()).thenReturn(2);

        for (int i = 0; i < 5; i++) {
            when(lsh.getBuckets(any(), anyInt(), eq(i)))
                    .thenReturn(List.of(i+1));
        }

        QueryToken t = factory.create(new double[]{1,2}, 5);
        assertEquals(5, t.getNumTables());
        assertEquals(5, t.getTableBuckets().size());
    }

    // --------------------------------------------------------------------

    @Test
    void testNullVectorThrows() {
        NullPointerException ex =
                assertThrows(NullPointerException.class, () -> factory.create(null, 5));
        assertEquals("query vector", ex.getMessage());
    }

    // --------------------------------------------------------------------

    @Test
    void testEmptyVectorThrowsIllegalState() {
        // empty vector → dimension mismatch check triggers IllegalStateException
        when(lsh.getDimensions()).thenReturn(2);

        assertThrows(IllegalStateException.class, () ->
                factory.create(new double[0], 5));
    }

    // --------------------------------------------------------------------

    @Test
    void testDimensionMismatchThrows() {
        when(lsh.getDimensions()).thenReturn(3); // mismatch
        double[] q = {1.0, 2.0};

        assertThrows(IllegalStateException.class, () ->
                factory.create(q, 5));
    }

    // --------------------------------------------------------------------

    @Test
    void testDeriveMaintainsAllFields() {
        double[] vec = {1,2};

        when(lsh.getBuckets(vec, 32, 0)).thenReturn(List.of(1));
        when(lsh.getBuckets(vec, 32, 1)).thenReturn(List.of(2));
        when(lsh.getBuckets(vec, 32, 2)).thenReturn(List.of(3));

        QueryToken base = factory.create(vec, 5);
        QueryToken w = factory.derive(base, 20);

        assertEquals(5, base.getTopK());
        assertEquals(20, w.getTopK());
        assertEquals(base.getTableBuckets(), w.getTableBuckets());
        assertEquals(base.getDimension(), w.getDimension());
        assertEquals(base.getEncryptionContext(), w.getEncryptionContext());
    }
}
