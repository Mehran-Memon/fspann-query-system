package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyVersion;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.IndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class QueryServiceImplTest {
    @Mock
    private IndexService indexService;

    @Mock
    private CryptoService cryptoService;

    @Mock
    private com.fspann.common.KeyLifeCycleService keyService;

    private QueryServiceImpl service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        service = new QueryServiceImpl(indexService, cryptoService, keyService);
    }

    @Test
    void searchDecryptsQueryAndSorts() throws Exception {
        byte[] iv = new byte[12];
        Arrays.fill(iv, (byte) 1);
        byte[] encQuery = new byte[32];
        Arrays.fill(encQuery, (byte) 2);

        double[] plaintextQuery = new double[]{1.0, 2.0};
        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 2, 1, "epoch_v7");

        SecretKey queryKey = new SecretKeySpec(new byte[32], "AES");
        KeyVersion queryVersion = new KeyVersion(7, queryKey);
        KeyVersion currentVersion = new KeyVersion(8, queryKey);

        doNothing().when(keyService).rotateIfNeeded();
        when(keyService.getVersion(7)).thenReturn(queryVersion);
        when(keyService.getCurrentVersion()).thenReturn(currentVersion);

        double[] queryVector = new double[]{1.0, 2.0};
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(queryKey))).thenReturn(queryVector);

        EncryptedPoint p1 = new EncryptedPoint("id1", 0, iv, encQuery, queryVersion.getVersion(), queryVector.length);
        EncryptedPoint p2 = new EncryptedPoint("id2", 0, iv, encQuery, queryVersion.getVersion(), queryVector.length);
        when(indexService.lookup(token)).thenReturn(List.of(p1, p2));

        when(cryptoService.decryptFromPoint(p1, queryKey)).thenReturn(new double[]{0.0, 0.0});
        when(cryptoService.decryptFromPoint(p2, queryKey)).thenReturn(new double[]{1.0, 2.0});

        List<QueryResult> results = service.search(token);

        verify(keyService).rotateIfNeeded();
        verify(cryptoService).decryptQuery(eq(encQuery), eq(iv), eq(queryKey));
        verify(indexService).lookup(token);
        verify(cryptoService).decryptFromPoint(p1, queryKey);
        verify(cryptoService).decryptFromPoint(p2, queryKey);

        assertEquals("id2", results.get(0).getId());
        assertEquals(0.0, results.get(0).getDistance(), 1e-9);
        assertEquals("id1", results.get(1).getId());
        assertEquals(2.23606797749979, results.get(1).getDistance(), 1e-9); // Fix: Correct expected distance
    }


    @Test
    void searchLimitsToTopK() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] plaintextQuery = new double[]{1.0, 2.0, 3.0};
        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 2, 1, "epoch_v7");

        SecretKey queryKey = new SecretKeySpec(new byte[32], "AES");
        KeyVersion queryVersion = new KeyVersion(7, queryKey);
        doNothing().when(keyService).rotateIfNeeded();
        when(keyService.getVersion(7)).thenReturn(queryVersion);
        when(keyService.getCurrentVersion()).thenReturn(queryVersion);

        EncryptedPoint p1 = new EncryptedPoint("id1", 0, iv, encQuery, queryVersion.getVersion(), 3); // Match vector length
        EncryptedPoint p2 = new EncryptedPoint("id2", 0, iv, encQuery, queryVersion.getVersion(), 3);
        EncryptedPoint p3 = new EncryptedPoint("id3", 0, iv, encQuery, queryVersion.getVersion(), 3);
        when(indexService.lookup(token)).thenReturn(List.of(p1, p2, p3));

        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(queryKey))).thenReturn(plaintextQuery);
        when(cryptoService.decryptFromPoint(p1, queryKey)).thenReturn(new double[]{1.0, 2.0, 3.0});
        when(cryptoService.decryptFromPoint(p3, queryKey)).thenReturn(new double[]{1.1, 2.1, 3.1});
        when(cryptoService.decryptFromPoint(p2, queryKey)).thenReturn(new double[]{0.0, 0.0, 0.0});

        List<QueryResult> results = service.search(token);

        assertEquals(2, results.size());
        assertEquals("id1", results.get(0).getId());
        assertEquals(0.0, results.get(0).getDistance(), 1e-9);
        assertEquals("id3", results.get(1).getId());
        assertEquals(0.17320508075688773, results.get(1).getDistance(), 1e-9);
    }

    @Test
    void testInvalidEncryptionContextFormat() {
        QueryToken token = new QueryToken(List.of(1), new byte[12], new byte[32], new double[]{1, 2}, 1, 1, "bad_context");

        assertThrows(NumberFormatException.class, () -> service.search(token));
    }

    @Test
    void testDecryptionFailureThrowsRuntime() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] vector = new double[]{1, 2};

        QueryToken token = new QueryToken(List.of(1), iv, encQuery, vector, 1, 1, "epoch_v7");
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(7, key);

        when(keyService.getVersion(7)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenThrow(new RuntimeException("Decryption failed"));

        assertThrows(RuntimeException.class, () -> service.search(token));
    }

    @Test
    void testEmptyCandidateListReturnsEmptyResults() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.5, 0.6};

        QueryToken token = new QueryToken(List.of(5), iv, encQuery, query, 10, 1, "epoch_v1");
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(query);
        when(indexService.lookup(token)).thenReturn(List.of());

        List<QueryResult> results = service.search(token);
        assertNotNull(results);
        assertTrue(results.isEmpty(), "Expected empty result list when no candidates returned");
    }

    @Test
    void testMismatchedVectorDimensionsThrows() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] plaintextQuery = new double[]{1.0, 2.0};

        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 1, 1, "epoch_v7");
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(7, key);

        when(keyService.getVersion(7)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(plaintextQuery);

        EncryptedPoint p = new EncryptedPoint("idX", 0, iv, encQuery, 7, 2);
        when(indexService.lookup(token)).thenReturn(List.of(p));
        when(cryptoService.decryptFromPoint(p, key)).thenReturn(new double[]{1.0, 2.0, 3.0});

        assertThrows(IllegalArgumentException.class, () -> service.search(token));
    }
}
