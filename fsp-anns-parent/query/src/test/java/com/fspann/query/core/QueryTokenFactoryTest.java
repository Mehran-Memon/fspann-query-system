package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QueryTokenFactoryTest {
    @Mock private CryptoService cryptoService;
    @Mock private KeyLifeCycleService keyService;
    @Mock private EvenLSH lsh;

    private QueryTokenFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        SecretKey mockKey = new SecretKeySpec(new byte[16], "AES");
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(7, mockKey));
        when(lsh.getDimensions()).thenReturn(2);

        // For numTables = 3, return 3 lists of contiguous buckets
        when(lsh.getBucketsForAllTables(any(double[].class), eq(5), eq(3)))
                .thenReturn(List.of(
                        Arrays.asList(1, 2, 3),
                        Arrays.asList(4, 5, 6),
                        Arrays.asList(7, 8, 9)
                ));

        lenient().when(lsh.getBuckets(any(double[].class), anyInt(), anyInt()))
                .thenReturn(List.of(1, 2, 3));
        factory = new QueryTokenFactory(cryptoService, keyService, lsh, 2, 3);
    }

    @Test
    void testCreateToken() {
        double[] vector = {1.0, 2.0};
        byte[] iv = new byte[12];
        byte[] ciphertext = new byte[32];

        when(cryptoService.encryptToPoint(eq("query"), eq(vector), any(SecretKey.class)))
                .thenReturn(new EncryptedPoint("query", 0, iv, ciphertext, 7, vector.length, List.of(0)));

        QueryToken token = factory.create(vector, 5);

        assertNotNull(token);
        assertEquals(3, token.getNumTables());
        assertEquals(3, token.getTableBuckets().size());
        assertTrue(token.getTableBuckets().stream().allMatch(l -> !l.isEmpty()));
        assertArrayEquals(vector, token.getPlaintextQuery());
        assertEquals(5, token.getTopK());
        assertEquals(String.format("epoch_%d_dim_%d", 7, vector.length), token.getEncryptionContext());
        assertEquals(vector.length, token.getDimension());
        assertNotNull(token.getIv());
        assertNotNull(token.getEncryptedQuery());
    }

    @Test
    void testCreateTokenWithDifferentNumTables() {
        // For numTables = 5, change factory and mock
        factory = new QueryTokenFactory(cryptoService, keyService, lsh, 2, 5);
        when(lsh.getBucketsForAllTables(any(double[].class), eq(5), eq(5)))
                .thenReturn(List.of(
                        List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)
                ));
        when(cryptoService.encryptToPoint(eq("query"), any(double[].class), any()))
                .thenReturn(new EncryptedPoint("query", 0, new byte[12], new byte[32], 7, 2, List.of(0)));

        QueryToken token = factory.create(new double[]{1.0, 2.0}, 5);
        assertEquals(5, token.getNumTables());
        assertEquals(5, token.getTableBuckets().size());
    }

    @Test
    void testNullVectorThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> factory.create(null, 5));
    }

    @Test
    void testEmptyVectorThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> factory.create(new double[0], 5));
    }

    @Test
    void testTopKZeroThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> factory.create(new double[]{1.0, 2.0}, 0));
    }

    @Test
    void testNegativeTopKThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> factory.create(new double[]{1.0, 2.0}, -1));
    }

    @Test
    void testDimensionMismatchThrows() {
        when(lsh.getDimensions()).thenReturn(3); // factory expects 3D, we pass 2D
        assertThrows(IllegalArgumentException.class, () -> factory.create(new double[]{1.0, 2.0}, 5));
    }

    @Test
    void testInvalidConstructorThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueryTokenFactory(cryptoService, keyService, lsh, 0));
        assertThrows(NullPointerException.class, () ->
                new QueryTokenFactory(null, keyService, lsh, 3));
        assertThrows(NullPointerException.class, () ->
                new QueryTokenFactory(cryptoService, null, lsh, 3));
        assertThrows(NullPointerException.class, () ->
                new QueryTokenFactory(cryptoService, keyService, null, 3));
    }
}
