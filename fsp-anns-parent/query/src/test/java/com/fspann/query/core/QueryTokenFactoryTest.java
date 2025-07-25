package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.index.core.EvenLSH;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
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
        lenient().when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(7, mockKey));
        lenient().when(lsh.getBuckets(any(double[].class))).thenReturn(List.of(1, 2, 3));
        factory = new QueryTokenFactory(cryptoService, keyService, lsh, 2, 3);
    }

    @Test
    void testCreateToken() {
        double[] vector = {1.0, 2.0};
        byte[] iv = new byte[12];
        byte[] ciphertext = new byte[32];
        when(cryptoService.encryptToPoint(eq("index"), eq(vector), any(SecretKey.class)))
                .thenReturn(new EncryptedPoint("query", 0, iv, ciphertext, 7, vector.length, Collections.singletonList(0)));

        QueryToken token = factory.create(vector, 5);

        assertNotNull(token);
        assertEquals(3, token.getCandidateBuckets().size());
        assertArrayEquals(vector, token.getPlaintextQuery());
        assertEquals(5, token.getTopK());
        assertEquals(3, token.getNumTables());
        assertEquals(String.format("epoch_%d_dim_%d", 7, vector.length), token.getEncryptionContext());
        assertEquals(vector.length, token.getDimension());
        assertNotNull(token.getIv());
        assertNotNull(token.getEncryptedQuery());
    }

    @Test
    void testCreateTokenWithDifferentNumTables() {
        factory = new QueryTokenFactory(cryptoService, keyService, lsh, 2, 5);
        double[] vector = {1.0, 2.0};
        byte[] iv = new byte[12];
        byte[] ciphertext = new byte[32];
        when(cryptoService.encryptToPoint(eq("index"), eq(vector), any(SecretKey.class)))
                .thenReturn(new EncryptedPoint("query", 0, iv, ciphertext, 7, vector.length, Collections.singletonList(0)));

        QueryToken token = factory.create(vector, 5);
        assertEquals(5, token.getNumTables());
    }

    @Test
    void testNullVectorThrowsException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                factory.create(null, 5));
        assertEquals("Input vector must be non-null and non-empty", ex.getMessage());
    }

    @Test
    void testEmptyVectorThrowsException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                factory.create(new double[0], 5));
        assertEquals("Input vector must be non-null and non-empty", ex.getMessage());
    }

    @Test
    void testTopKZeroThrowsException() {
        double[] vector = {1.0, 2.0};
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                factory.create(vector, 0));
        assertEquals("topK must be greater than zero", ex.getMessage());
    }

    @Test
    void testNegativeTopKThrowsException() {
        double[] vector = {1.0, 2.0};
        assertThrows(IllegalArgumentException.class, () -> factory.create(vector, -1));
    }

    @Test
    void testInvalidConstructorThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueryTokenFactory(cryptoService, keyService, lsh, -1, 3));
        assertThrows(IllegalArgumentException.class, () ->
                new QueryTokenFactory(cryptoService, keyService, lsh, 2, 0));
        assertThrows(NullPointerException.class, () ->
                new QueryTokenFactory(null, keyService, lsh, 2, 3));
    }
}
