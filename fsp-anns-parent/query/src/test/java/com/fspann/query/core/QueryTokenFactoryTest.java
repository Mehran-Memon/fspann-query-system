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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryTokenFactoryTest {
    @Mock
    private CryptoService cryptoService;

    @Mock
    private KeyLifeCycleService keyService;

    @Mock
    private EvenLSH lsh;

    private QueryTokenFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new QueryTokenFactory(cryptoService, keyService, lsh, 2, 3);

        // Provide IV and encryptedQuery placeholders for KeyVersion
        byte[] iv = new byte[12];
        byte[] encryptedQuery = new byte[32];

        when(keyService.getCurrentVersion()).thenReturn(
                new KeyVersion(
                        7,
                        new SecretKeySpec(new byte[16], "AES"),
                        iv,
                        encryptedQuery
                )
        );
        when(lsh.getBuckets(any(double[].class))).thenReturn(List.of(1, 2, 3));
        when(cryptoService.encryptToPoint(eq("query"), any(double[].class), any(SecretKey.class)))
                .thenReturn(new EncryptedPoint("query", 0, new byte[12], new byte[32], 7));
    }

    @Test
    void testCreateToken() {
        double[] vector = {1.0, 2.0};
        QueryToken token = factory.create(vector, 5);
        assertNotNull(token.getBuckets());
        assertEquals(3, token.getBuckets().size());
        assertNotNull(token.getIv());
        assertNotNull(token.getEncryptedQuery());
        assertArrayEquals(vector, token.getPlaintextQuery());
        assertEquals(5, token.getTopK());
        assertEquals("epoch_v7", token.getEncryptionContext());
    }
}
