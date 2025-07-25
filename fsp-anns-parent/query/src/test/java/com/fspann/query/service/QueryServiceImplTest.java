package com.fspann.query.service;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.common.IndexService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QueryServiceImplTest {
    @Mock private IndexService indexService;
    @Mock private CryptoService cryptoService;
    @Mock private KeyLifeCycleService keyService;
    @Mock private GroundtruthManager groundtruthManager;

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
        String encryptionContext = String.format("epoch_%d_dim_%d", 7, plaintextQuery.length);
        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 2, 1, encryptionContext, 2, 0, 7);

        SecretKey queryKey = new SecretKeySpec(new byte[32], "AES");
        KeyVersion queryVersion = new KeyVersion(7, queryKey);
        KeyVersion currentVersion = new KeyVersion(8, queryKey);

        doNothing().when(keyService).rotateIfNeeded();
        when(keyService.getVersion(7)).thenReturn(queryVersion);
        when(keyService.getCurrentVersion()).thenReturn(currentVersion);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(queryKey))).thenReturn(plaintextQuery);

        EncryptedPoint p1 = new EncryptedPoint("id1", 0, iv, encQuery, 7, 2, Collections.singletonList(1));
        EncryptedPoint p2 = new EncryptedPoint("id2", 0, iv, encQuery, 7, 2, Collections.singletonList(1));
        when(indexService.lookup(token)).thenReturn(Arrays.asList(p1, p2));

        when(cryptoService.decryptFromPoint(p1, queryKey)).thenReturn(new double[]{0.0, 0.0});
        when(cryptoService.decryptFromPoint(p2, queryKey)).thenReturn(new double[]{1.0, 2.0});

        List<QueryResult> results = service.search(token);

        verify(keyService).rotateIfNeeded();
        verify(cryptoService).decryptQuery(eq(encQuery), eq(iv), eq(queryKey));
        verify(indexService).lookup(token);
        verify(cryptoService).decryptFromPoint(p1, queryKey);
        verify(cryptoService).decryptFromPoint(p2, queryKey);

        assertEquals(2, results.size());
        assertEquals("id2", results.get(0).getId());
        assertEquals(0.0, results.get(0).getDistance(), 1e-9);
        assertEquals("id1", results.get(1).getId());
        assertEquals(computeDistance(plaintextQuery, new double[]{0.0, 0.0}), results.get(1).getDistance(), 1e-9);
    }

    @Test
    void searchLimitsToTopK() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] plaintextQuery = new double[]{1.0, 2.0, 3.0};
        String encryptionContext = String.format("epoch_%d_dim_%d", 7, plaintextQuery.length);
        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 2, 1, encryptionContext, 3, 0, 7);

        SecretKey queryKey = new SecretKeySpec(new byte[32], "AES");
        KeyVersion queryVersion = new KeyVersion(7, queryKey);

        doNothing().when(keyService).rotateIfNeeded();
        when(keyService.getVersion(7)).thenReturn(queryVersion);
        when(keyService.getCurrentVersion()).thenReturn(queryVersion);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(queryKey))).thenReturn(plaintextQuery);

        EncryptedPoint p1 = new EncryptedPoint("id1", 0, iv, encQuery, 7, 3, Collections.singletonList(1));
        EncryptedPoint p2 = new EncryptedPoint("id2", 0, iv, encQuery, 7, 3, Collections.singletonList(1));
        EncryptedPoint p3 = new EncryptedPoint("id3", 0, iv, encQuery, 7, 3, Collections.singletonList(1));
        when(indexService.lookup(token)).thenReturn(Arrays.asList(p1, p2, p3));

        when(cryptoService.decryptFromPoint(p1, queryKey)).thenReturn(new double[]{1.0, 2.0, 3.0});
        when(cryptoService.decryptFromPoint(p2, queryKey)).thenReturn(new double[]{0.0, 0.0, 0.0});
        when(cryptoService.decryptFromPoint(p3, queryKey)).thenReturn(new double[]{1.1, 2.1, 3.1});

        List<QueryResult> results = service.search(token);

        assertEquals(2, results.size());
        assertEquals("id1", results.get(0).getId());
        assertEquals(0.0, results.get(0).getDistance(), 1e-9);
        assertEquals("id3", results.get(1).getId());
        assertEquals(computeDistance(plaintextQuery, new double[]{1.1, 2.1, 3.1}), results.get(1).getDistance(), 1e-9);
    }

    @Test
    void testInvalidEncryptionContextFormat() {
        QueryToken token = new QueryToken(List.of(1), new byte[12], new byte[32], new double[]{1, 2}, 1, 1, "bad_context", 2, 0, 1);
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key));
        when(cryptoService.decryptQuery(any(), any(), eq(key))).thenReturn(new double[]{1, 2});
        when(indexService.lookup(token)).thenReturn(List.of());
        List<QueryResult> results = service.search(token);
        assertTrue(results.isEmpty(), "Should return empty results for invalid context with no candidates");
    }

    @Test
    void testDecryptionFailureThrowsRuntime() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] vector = new double[]{1, 2};
        String encryptionContext = String.format("epoch_%d_dim_%d", 7, vector.length);
        QueryToken token = new QueryToken(List.of(1), iv, encQuery, vector, 1, 1, encryptionContext, 2, 0, 7);

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
        String encryptionContext = String.format("epoch_%d_dim_%d", 1, query.length);
        QueryToken token = new QueryToken(List.of(5), iv, encQuery, query, 10, 1, encryptionContext, 2, 0, 1);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(query);
        when(indexService.lookup(token)).thenReturn(List.of());

        List<QueryResult> results = service.search(token);
        assertNotNull(results);
        assertTrue(results.isEmpty(), "Expected empty result list when no candidates returned");
        assertTrue(service.getLastQueryDurationNs() > 0, "Query duration should be recorded");
    }

    @Test
    void testMismatchedVectorDimensionsThrows() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] plaintextQuery = new double[]{1.0, 2.0};
        String encryptionContext = String.format("epoch_%d_dim_%d", 7, plaintextQuery.length);
        QueryToken token = new QueryToken(List.of(1, 2), iv, encQuery, plaintextQuery, 1, 1, encryptionContext, 2, 0, 7);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(7, key);

        when(keyService.getVersion(7)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(plaintextQuery);

        EncryptedPoint p = new EncryptedPoint("idX", 0, iv, encQuery, 7, 3, Collections.singletonList(1));
        when(indexService.lookup(token)).thenReturn(Arrays.asList(p));
        when(cryptoService.decryptFromPoint(p, key)).thenReturn(new double[]{1.0, 2.0, 3.0});

        assertThrows(IllegalArgumentException.class, () -> service.search(token));
    }

    @Test
    void testInvalidTokenFields_NullBuckets() {
        assertThrows(IllegalArgumentException.class, () -> {
            new QueryToken(null, new byte[12], new byte[32], new double[]{1, 2}, 1, 1, "epoch_1_dim_2", 2, 0, 1);
        }, "candidateBuckets cannot be null or empty");
    }


    @Test
    void testInvalidTokenFields_InvalidNumTables() {
        assertThrows(IllegalArgumentException.class, () -> {
            new QueryToken(List.of(1), new byte[12], new byte[32], new double[]{1, 2}, 1, 0, "epoch_1_dim_2", 2, 0, 1);
        }, "numTables must be positive");
    }


    @Test
    void testIndexServiceFailure() {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.5, 0.6};
        String encryptionContext = String.format("epoch_%d_dim_%d", 1, query.length);
        QueryToken token = new QueryToken(List.of(5), iv, encQuery, query, 10, 1, encryptionContext, 2, 0, 1);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(query);
        when(indexService.lookup(token)).thenThrow(new RuntimeException("Index lookup failed"));

        assertThrows(RuntimeException.class, () -> service.search(token));
    }

    @Test
    void testSearchWithTopKVariants() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.5, 0.6};
        String encryptionContext = String.format("epoch_%d_dim_%d", 1, query.length);
        QueryToken token = new QueryToken(List.of(5), iv, encQuery, query, 10, 1, encryptionContext, 2, 0, 1);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(query);
        // Use numeric IDs for points to match groundtruth!
        when(indexService.lookup(any(QueryToken.class))).thenReturn(Arrays.asList(
                new EncryptedPoint("1", 0, iv, encQuery, 1, 2, Collections.singletonList(5)),
                new EncryptedPoint("2", 0, iv, encQuery, 1, 2, Collections.singletonList(5))
        ));
        when(cryptoService.decryptFromPoint(any(), eq(key))).thenReturn(new double[]{0.5, 0.6});
        when(groundtruthManager.getGroundtruth(eq(1), anyInt())).thenReturn(new int[]{1});

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 1, groundtruthManager);
        assertEquals(6, results.size());
        assertEquals(1, results.get(0).getTopKRequested());
        assertEquals(1, results.get(0).getRetrieved());
        assertEquals(1.0, results.get(0).getRatio(), 1e-9);
        assertEquals(1.0, results.get(0).getRecall(), 1e-9);
        assertTrue(results.get(0).getTimeMs() >= 0);

        List<Integer> expectedTopKs = List.of(1, 20, 40, 60, 80, 100);
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedTopKs.get(i), results.get(i).getTopKRequested());
        }
    }

    @Test
    void testKeyServiceVersionFailure() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.5, 0.6};
        String encryptionContext = String.format("epoch_%d_dim_%d", 999, query.length);
        QueryToken token = new QueryToken(List.of(5), iv, encQuery, query, 10, 1, encryptionContext, 2, 0, 999);

        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, new SecretKeySpec(new byte[32], "AES")));
        when(keyService.getVersion(999)).thenThrow(new IllegalArgumentException("Unknown key version"));

        assertThrows(IllegalArgumentException.class, () -> service.search(token));
    }

    @Test
    void testSearchPerformance() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.5, 0.6};
        String encryptionContext = String.format("epoch_%d_dim_%d", 1, query.length);
        QueryToken token = new QueryToken(List.of(5), iv, encQuery, query, 10, 1, encryptionContext, 2, 0, 1);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(query);
        when(indexService.lookup(token)).thenReturn(List.of());

        long start = System.nanoTime();
        service.search(token);
        long durationMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(durationMs < 500, "Query should complete within 500ms, took " + durationMs + "ms");
    }

    @Test
    void testSearchWithTopKVariantsEmptyGroundtruth() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{0.5, 0.6};
        String encryptionContext = String.format("epoch_%d_dim_%d", 1, query.length);
        QueryToken token = new QueryToken(List.of(5), iv, encQuery, query, 10, 1, encryptionContext, 2, 0, 1);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(query);
        when(indexService.lookup(any(QueryToken.class))).thenReturn(Arrays.asList(
                new EncryptedPoint("id1", 0, iv, encQuery, 1, 2, Collections.singletonList(5)),
                new EncryptedPoint("id2", 0, iv, encQuery, 1, 2, Collections.singletonList(5))
        ));
        when(cryptoService.decryptFromPoint(any(), eq(key))).thenReturn(new double[]{0.5, 0.6});
        when(groundtruthManager.getGroundtruth(eq(1), anyInt())).thenReturn(new int[]{});

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 1, groundtruthManager);
        assertEquals(6, results.size());
        assertEquals(0.0, results.get(0).getRecall(), 1e-9);
    }

    private double computeDistance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }
}
