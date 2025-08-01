package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

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

    @AfterEach
    void tearDown() {
        clearInvocations(indexService, cryptoService, keyService, groundtruthManager);
    }

    @Test
    void testTokenWithEmptyContextDefaults() {
        QueryToken token = new QueryToken(List.of(1), new byte[12], new byte[32], new double[]{1.0, 2.0}, 1, 1, "", 2, 0, 0);
        assertTrue(token.getEncryptionContext().startsWith("epoch_0_dim_"));
    }

    @Test
    void testPartialCandidateSorting() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{1.0, 2.0};
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(7, key);
        String ctx = String.format("epoch_%d_dim_%d", 7, query.length);

        QueryToken token = new QueryToken(List.of(1), iv, encQuery, query, 2, 1, ctx, 2, 0, 7);

        when(keyService.getVersion(7)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(encQuery, iv, key)).thenReturn(query);

        EncryptedPoint good = new EncryptedPoint("good", 0, iv, encQuery, 7, 2, List.of(1));
        EncryptedPoint bad = new EncryptedPoint("bad", 0, iv, encQuery, 7, 2, List.of(1));

        when(indexService.lookup(token)).thenReturn(List.of(good, bad));
        when(cryptoService.decryptFromPoint(good, key)).thenReturn(new double[]{1.0, 2.0});
        when(cryptoService.decryptFromPoint(bad, key)).thenReturn(new double[]{100.0, 100.0});

        List<QueryResult> results = service.search(token);
        assertEquals("good", results.get(0).getId());
    }

    @Test
    void testKeyVersionFallbackNotUsed() {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] query = new double[]{1.0, 2.0};
        QueryToken token = new QueryToken(List.of(1), iv, encQuery, query, 1, 1, "epoch_999_dim_2", 2, 0, 999);

        SecretKey fallbackKey = new SecretKeySpec(new byte[32], "AES");
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, fallbackKey));
        when(keyService.getVersion(999)).thenThrow(new IllegalArgumentException("no such version"));

        assertThrows(IllegalArgumentException.class, () -> service.search(token));
    }

    @Test
    void testZeroRatioWithEmptyGroundtruth() throws Exception {
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
        when(groundtruthManager.hasVector(anyString())).thenReturn(false);

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 1, groundtruthManager);
        assertEquals(1.0, results.get(0).getRatio(), 1e-9, "Expected fallback ratio of 1.0 when groundtruth is empty");
        assertEquals(0.0, results.get(0).getRecall(), 1e-9, "Recall should still be 0.0 when groundtruth is empty");
    }

    @Test
    void testRatioCalculationWithGroundtruth() throws Exception {
        byte[] iv = new byte[12];
        byte[] encQuery = new byte[32];
        double[] queryVec = new double[]{1.0, 1.0};
        String ctx = "epoch_1_dim_2";

        QueryToken token = new QueryToken(List.of(5), iv, encQuery, queryVec, 1, 1, ctx, 2, 0, 1);

        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        KeyVersion version = new KeyVersion(1, key);

        when(keyService.getVersion(1)).thenReturn(version);
        when(keyService.getCurrentVersion()).thenReturn(version);
        when(cryptoService.decryptQuery(eq(encQuery), eq(iv), eq(key))).thenReturn(queryVec);

        EncryptedPoint top1 = new EncryptedPoint("42", 0, iv, encQuery, 1, 2, Collections.singletonList(5));
        when(indexService.lookup(any())).thenReturn(List.of(top1));
        when(cryptoService.decryptFromPoint(eq(top1), eq(key))).thenReturn(new double[]{0.0, 0.0});

        when(groundtruthManager.getGroundtruth(eq(0), anyInt())).thenReturn(new int[]{42});
        when(groundtruthManager.hasVector("42")).thenReturn(true);
        when(groundtruthManager.getVectorById("42")).thenReturn(new double[]{0.0, 0.0});  // Perfect match

        List<QueryEvaluationResult> results = service.searchWithTopKVariants(token, 0, groundtruthManager);

        QueryEvaluationResult top1Result = results.stream()
                .filter(r -> r.getTopKRequested() == 1)
                .findFirst().orElseThrow();

        assertEquals(1.0, top1Result.getRatio(), 1e-6);
        assertEquals(1.0, top1Result.getRecall(), 1e-6);
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
