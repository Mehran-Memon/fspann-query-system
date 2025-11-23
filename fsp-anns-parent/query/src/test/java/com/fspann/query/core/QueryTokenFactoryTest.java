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
import java.util.Arrays;
import java.util.BitSet;
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
        when(lsh.getBucketsForAllTables(any(double[].class), anyInt(), eq(3)))
                .thenReturn(List.of(
                        Arrays.asList(1, 2, 3),
                        Arrays.asList(4, 5, 6),
                        Arrays.asList(7, 8, 9)
                ));

        // Legacy/multiprobe path safeguard (used when getBucketsForAllTables returns null/mismatch)
        lenient().when(lsh.getBuckets(any(double[].class), anyInt(), anyInt()))
                .thenReturn(List.of(1, 2, 3));

        // New ctor: (crypto, keySvc, lsh, numTables, divisions(ℓ), m, seedBase)
        factory = new QueryTokenFactory(
                cryptoService,
                keyService,
                lsh,
                /*numTables*/ 3,
                /*divisions*/ 2,
                /*m*/ 3,
                /*seedBase*/ 13L
        );
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

        // Paper path: codes should be present (length = divisions)
        assertNotNull(token.getCodes());
        assertEquals(2, token.getCodes().length);

        // Top-level fields
        assertEquals(5, token.getTopK());
        assertEquals(String.format("epoch_%d_dim_%d", 7, vector.length), token.getEncryptionContext());
        assertEquals(vector.length, token.getDimension());
        assertArrayEquals(iv, token.getIv());
        assertArrayEquals(ciphertext, token.getEncryptedQuery());
        assertEquals(7, token.getVersion());
    }

    @Test
    void testCreateTokenWithDifferentNumTables() {
        // For numTables = 5, change factory and mock
        factory = new QueryTokenFactory(
                cryptoService,
                keyService,
                lsh,
                /*numTables*/ 5,
                /*divisions*/ 2,
                /*m*/ 3,
                /*seedBase*/ 13L
        );

        when(lsh.getBucketsForAllTables(any(double[].class), anyInt(), eq(5)))
                .thenReturn(List.of(
                        List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)
                ));
        when(cryptoService.encryptToPoint(eq("query"), any(double[].class), any()))
                .thenReturn(new EncryptedPoint("query", 0, new byte[12], new byte[32], 7, 2, List.of(0)));

        QueryToken token = factory.create(new double[]{1.0, 2.0}, 5);
        assertEquals(5, token.getNumTables());
        assertEquals(5, token.getTableBuckets().size());

        // Codes still present with same divisions
        assertNotNull(token.getCodes());
        assertEquals(2, token.getCodes().length);
    }

    @Test
    void testCreateTokenFallsBackToLegacyBucketsWhenAllTablesReturnNull() {
        // Force getBucketsForAllTables to return null → fallback path
        when(lsh.getBucketsForAllTables(any(double[].class), anyInt(), eq(3)))
                .thenReturn(null);

        when(cryptoService.encryptToPoint(eq("query"), any(double[].class), any()))
                .thenReturn(new EncryptedPoint("query", 0, new byte[12], new byte[32], 7, 2, List.of(0)));

        double[] v = {0.5, -1.0};
        QueryToken token = factory.create(v, 7);

        assertEquals(3, token.getNumTables());
        assertEquals(3, token.getTableBuckets().size());
        assertTrue(token.getTableBuckets().stream().allMatch(l -> !l.isEmpty()));

        // Should have called legacy getBuckets once per table
        verify(lsh, atLeast(3)).getBuckets(any(double[].class), eq(7), anyInt());
    }

    @Test
    void testDeriveReusesCiphertextBucketsAndCodesByValue() {
        double[] vector = {1.0, 2.0};
        byte[] iv = new byte[12];
        byte[] ciphertext = new byte[32];

        when(cryptoService.encryptToPoint(eq("query"), eq(vector), any()))
                .thenReturn(new EncryptedPoint("query", 0, iv, ciphertext, 7, vector.length, List.of(0)));

        QueryToken base = factory.create(vector, 5);
        QueryToken widened = factory.derive(base, 20);

        assertEquals(5, base.getTopK());
        assertEquals(20, widened.getTopK());

        // Same per-table expansions logically
        assertEquals(base.getTableBuckets(), widened.getTableBuckets());

        // Codes copied by value (BitSets equal but not necessarily same reference)
        BitSet[] baseCodes = base.getCodes();
        BitSet[] widenedCodes = widened.getCodes();
        assertNotNull(baseCodes);
        assertNotNull(widenedCodes);
        assertEquals(baseCodes.length, widenedCodes.length);
        for (int i = 0; i < baseCodes.length; i++) {
            BitSet bc = baseCodes[i];
            BitSet wc = widenedCodes[i];
            if (bc == null || wc == null) {
                assertEquals(bc, wc);
            } else {
                assertNotSame(bc, wc);
                assertEquals(bc, wc);
            }
        }

        // IV and ciphertext equal by content
        assertArrayEquals(base.getIv(), widened.getIv());
        assertArrayEquals(base.getEncryptedQuery(), widened.getEncryptedQuery());
        assertEquals(base.getEncryptionContext(), widened.getEncryptionContext());
        assertEquals(base.getDimension(), widened.getDimension());
        assertEquals(base.getVersion(), widened.getVersion());
    }

    @Test
    void testNullVectorThrowsException() {
        var npe = assertThrows(NullPointerException.class, () -> factory.create(null, 5));
        assertEquals("vector", npe.getMessage());
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
    void testDimensionMismatchUsesDummyBucketsButKeepsCodes() {
        // Now we EXPECT no exception; we just log a warning and produce dummy buckets.
        when(lsh.getDimensions()).thenReturn(3); // factory expects 3D, we pass 2D

        when(cryptoService.encryptToPoint(eq("query"), any(double[].class), any()))
                .thenReturn(new EncryptedPoint("query", 0, new byte[12], new byte[32], 7, 2, List.of(0)));

        double[] query = {1.0, 2.0};
        QueryToken token = factory.create(query, 5);

        // Still reports 3 tables, but all with empty bucket lists (dummy structure)
        assertEquals(3, token.getNumTables());
        assertEquals(3, token.getTableBuckets().size());
        assertTrue(token.getTableBuckets().stream().allMatch(List::isEmpty));

        // Paper codes must still be attached
        assertNotNull(token.getCodes());
        assertEquals(2, token.getCodes().length);
    }

    @Test
    void testInvalidConstructorThrowsException() {
        // numTables < 0 is invalid
        assertThrows(IllegalArgumentException.class, () ->
                new QueryTokenFactory(cryptoService, keyService, lsh, -1, 2, 3, 13L));

        // divisions <= 0 is invalid
        assertThrows(IllegalArgumentException.class, () ->
                new QueryTokenFactory(cryptoService, keyService, lsh, 3, 0, 3, 13L));

        // m <= 0 is invalid
        assertThrows(IllegalArgumentException.class, () ->
                new QueryTokenFactory(cryptoService, keyService, lsh, 3, 2, 0, 13L));

        // null deps still invalid
        assertThrows(NullPointerException.class, () ->
                new QueryTokenFactory(null, keyService, lsh, 3, 2, 3, 13L));
        assertThrows(NullPointerException.class, () ->
                new QueryTokenFactory(cryptoService, null, lsh, 3, 2, 3, 13L));

        // lsh may now be null in pure paper mode → should NOT throw
        assertDoesNotThrow(() ->
                new QueryTokenFactory(cryptoService, keyService, null, 3, 2, 3, 13L));
    }
}
