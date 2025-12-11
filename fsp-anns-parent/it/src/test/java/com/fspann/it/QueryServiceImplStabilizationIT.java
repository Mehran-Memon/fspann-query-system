package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.query.service.QueryServiceImpl;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.*;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class QueryServiceImplStabilizationIT {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private IndexService index;
    private QueryServiceImpl queryService;
    private SecretKey k;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = mock(IndexService.class);

        k = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, k));

        SystemConfig config = mock(SystemConfig.class);
        SystemConfig.StabilizationConfig stabilizationConfig = mock(SystemConfig.StabilizationConfig.class);
        when(config.getStabilization()).thenReturn(stabilizationConfig);
        when(stabilizationConfig.isEnabled()).thenReturn(true);
        when(stabilizationConfig.getAlpha()).thenReturn(0.2); // 20% of raw candidates
        when(stabilizationConfig.getMinCandidates()).thenReturn(5);  // Min 5 candidates

        QueryTokenFactory tokenFactory = mock(QueryTokenFactory.class);

        queryService = new QueryServiceImpl(index, crypto, keys, tokenFactory, config);
    }

    @Test
    void testStabilizationAppliesAlphaAndMinCandidates() {
        // Setup: 6 candidates, alpha=0.2 (20% = ~1 candidate), but minCandidates=5
        // Expected: should keep 5 (minCandidates is binding, not alpha)

        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        EncryptedPoint p1 = new EncryptedPoint("1", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p2 = new EncryptedPoint("2", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p3 = new EncryptedPoint("3", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p4 = new EncryptedPoint("4", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p5 = new EncryptedPoint("5", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p6 = new EncryptedPoint("6", 0, iv, ct, 1, 2, List.of());

        List<EncryptedPoint> rawCandidates = Arrays.asList(p1, p2, p3, p4, p5, p6);

        // Create a real QueryToken mock with proper configuration
        QueryToken queryToken = mock(QueryToken.class);
        when(queryToken.getVersion()).thenReturn(1);
        when(queryToken.getTopK()).thenReturn(100);  // Large K so stabilization is the limit

        // CRITICAL: Mock the query vector decryption
        byte[] encQuery = new byte[32];
        when(queryToken.getEncryptedQuery()).thenReturn(encQuery);
        when(queryToken.getIv()).thenReturn(iv);

        // Query vector: dim=2, all values valid
        double[] queryVector = new double[]{0.5, 0.5};
        when(crypto.decryptQuery(encQuery, iv, k)).thenReturn(queryVector);

        // Mock index lookup to return the raw candidates
        when(index.lookup(queryToken)).thenReturn(rawCandidates);

        // Mock decryption of each candidate (creates some distance variation)
        when(crypto.decryptFromPoint(p1, k)).thenReturn(new double[]{1.0, 1.0});
        when(crypto.decryptFromPoint(p2, k)).thenReturn(new double[]{2.0, 2.0});
        when(crypto.decryptFromPoint(p3, k)).thenReturn(new double[]{3.0, 3.0});
        when(crypto.decryptFromPoint(p4, k)).thenReturn(new double[]{4.0, 4.0});
        when(crypto.decryptFromPoint(p5, k)).thenReturn(new double[]{5.0, 5.0});
        when(crypto.decryptFromPoint(p6, k)).thenReturn(new double[]{6.0, 6.0});

        // Execute search
        List<QueryResult> results = queryService.search(queryToken);

        assertNotNull(results, "Search results should not be null");
        assertEquals(5, results.size(),
                "Stabilization with alpha=0.2 (1 candidate) and minCandidates=5 should keep 5");
    }

    @Test
    void testStabilizationNoAlphaKeepsAllCandidates() {
        // Setup: disable stabilization, should return all candidates

        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        EncryptedPoint p1 = new EncryptedPoint("1", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p2 = new EncryptedPoint("2", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p3 = new EncryptedPoint("3", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p4 = new EncryptedPoint("4", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p5 = new EncryptedPoint("5", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p6 = new EncryptedPoint("6", 0, iv, ct, 1, 2, List.of());

        List<EncryptedPoint> rawCandidates = Arrays.asList(p1, p2, p3, p4, p5, p6);

        // Create a real QueryToken mock
        QueryToken queryToken = mock(QueryToken.class);
        when(queryToken.getVersion()).thenReturn(1);
        when(queryToken.getTopK()).thenReturn(100);  // Large K

        // CRITICAL: Mock the query vector decryption
        byte[] encQuery = new byte[32];
        when(queryToken.getEncryptedQuery()).thenReturn(encQuery);
        when(queryToken.getIv()).thenReturn(iv);

        // Query vector: dim=2, all values valid
        double[] queryVector = new double[]{0.5, 0.5};
        when(crypto.decryptQuery(encQuery, iv, k)).thenReturn(queryVector);

        // Mock index lookup to return the raw candidates
        when(index.lookup(queryToken)).thenReturn(rawCandidates);

        // Mock decryption of each candidate
        when(crypto.decryptFromPoint(p1, k)).thenReturn(new double[]{1.0, 1.0});
        when(crypto.decryptFromPoint(p2, k)).thenReturn(new double[]{2.0, 2.0});
        when(crypto.decryptFromPoint(p3, k)).thenReturn(new double[]{3.0, 3.0});
        when(crypto.decryptFromPoint(p4, k)).thenReturn(new double[]{4.0, 4.0});
        when(crypto.decryptFromPoint(p5, k)).thenReturn(new double[]{5.0, 5.0});
        when(crypto.decryptFromPoint(p6, k)).thenReturn(new double[]{6.0, 6.0});

        // OVERRIDE: Disable stabilization for this test
        SystemConfig config = mock(SystemConfig.class);
        SystemConfig.StabilizationConfig stabilizationConfig = mock(SystemConfig.StabilizationConfig.class);
        when(config.getStabilization()).thenReturn(stabilizationConfig);
        when(stabilizationConfig.isEnabled()).thenReturn(false);  // <<< DISABLED

        QueryTokenFactory tokenFactory = mock(QueryTokenFactory.class);
        queryService = new QueryServiceImpl(index, crypto, keys, tokenFactory, config);

        // Execute search
        List<QueryResult> results = queryService.search(queryToken);

        assertNotNull(results, "Search results should not be null");
        assertEquals(6, results.size(),
                "With stabilization disabled, should keep all 6 candidates");
    }
}