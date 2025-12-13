package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
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
    private PartitionedIndexService index;
    private QueryServiceImpl queryService;
    private SecretKey k;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        index  = mock(PartitionedIndexService.class);

        k = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, k));

        SystemConfig config = mock(SystemConfig.class);
        SystemConfig.StabilizationConfig stabilizationConfig = mock(SystemConfig.StabilizationConfig.class);
        when(config.getStabilization()).thenReturn(stabilizationConfig);
        when(stabilizationConfig.isEnabled()).thenReturn(true);
        when(stabilizationConfig.getAlpha()).thenReturn(0.2);
        when(stabilizationConfig.getMinCandidates()).thenReturn(5);

        QueryTokenFactory tokenFactory = mock(QueryTokenFactory.class);

        queryService = new QueryServiceImpl(index, crypto, keys, tokenFactory, config);
    }

    @Test
    void testStabilizationAppliesAlphaAndMinCandidates() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        EncryptedPoint p1 = new EncryptedPoint("1", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p2 = new EncryptedPoint("2", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p3 = new EncryptedPoint("3", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p4 = new EncryptedPoint("4", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p5 = new EncryptedPoint("5", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p6 = new EncryptedPoint("6", 0, iv, ct, 1, 2, List.of());

        List<EncryptedPoint> rawCandidates = Arrays.asList(p1, p2, p3, p4, p5, p6);

        QueryToken queryToken = mock(QueryToken.class);
        when(queryToken.getVersion()).thenReturn(1);
        when(queryToken.getTopK()).thenReturn(100);

        byte[] encQuery = new byte[32];
        when(queryToken.getEncryptedQuery()).thenReturn(encQuery);
        when(queryToken.getIv()).thenReturn(iv);

        double[] queryVector = new double[]{0.5, 0.5};
        when(crypto.decryptQuery(encQuery, iv, k)).thenReturn(queryVector);

        when(index.lookup(queryToken)).thenReturn(rawCandidates);

        when(crypto.decryptFromPoint(p1, k)).thenReturn(new double[]{1.0, 1.0});
        when(crypto.decryptFromPoint(p2, k)).thenReturn(new double[]{2.0, 2.0});
        when(crypto.decryptFromPoint(p3, k)).thenReturn(new double[]{3.0, 3.0});
        when(crypto.decryptFromPoint(p4, k)).thenReturn(new double[]{4.0, 4.0});
        when(crypto.decryptFromPoint(p5, k)).thenReturn(new double[]{5.0, 5.0});
        when(crypto.decryptFromPoint(p6, k)).thenReturn(new double[]{6.0, 6.0});

        List<QueryResult> results = queryService.search(queryToken);

        assertNotNull(results);
        assertEquals(5, results.size());
    }

    @Test
    void testStabilizationNoAlphaKeepsAllCandidates() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        EncryptedPoint p1 = new EncryptedPoint("1", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p2 = new EncryptedPoint("2", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p3 = new EncryptedPoint("3", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p4 = new EncryptedPoint("4", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p5 = new EncryptedPoint("5", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p6 = new EncryptedPoint("6", 0, iv, ct, 1, 2, List.of());

        List<EncryptedPoint> rawCandidates = Arrays.asList(p1, p2, p3, p4, p5, p6);

        QueryToken queryToken = mock(QueryToken.class);
        when(queryToken.getVersion()).thenReturn(1);
        when(queryToken.getTopK()).thenReturn(100);

        byte[] encQuery = new byte[32];
        when(queryToken.getEncryptedQuery()).thenReturn(encQuery);
        when(queryToken.getIv()).thenReturn(iv);

        double[] queryVector = new double[]{0.5, 0.5};
        when(crypto.decryptQuery(encQuery, iv, k)).thenReturn(queryVector);

        when(index.lookup(queryToken)).thenReturn(rawCandidates);

        when(crypto.decryptFromPoint(p1, k)).thenReturn(new double[]{1.0, 1.0});
        when(crypto.decryptFromPoint(p2, k)).thenReturn(new double[]{2.0, 2.0});
        when(crypto.decryptFromPoint(p3, k)).thenReturn(new double[]{3.0, 3.0});
        when(crypto.decryptFromPoint(p4, k)).thenReturn(new double[]{4.0, 4.0});
        when(crypto.decryptFromPoint(p5, k)).thenReturn(new double[]{5.0, 5.0});
        when(crypto.decryptFromPoint(p6, k)).thenReturn(new double[]{6.0, 6.0});

        SystemConfig config = mock(SystemConfig.class);
        SystemConfig.StabilizationConfig stabilizationConfig = mock(SystemConfig.StabilizationConfig.class);
        when(config.getStabilization()).thenReturn(stabilizationConfig);
        when(stabilizationConfig.isEnabled()).thenReturn(false);

        QueryTokenFactory tokenFactory = mock(QueryTokenFactory.class);
        queryService = new QueryServiceImpl(index, crypto, keys, tokenFactory, config);

        List<QueryResult> results = queryService.search(queryToken);

        assertNotNull(results);
        assertEquals(6, results.size());
    }
}